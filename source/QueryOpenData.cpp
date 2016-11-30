#include "QueryOpenData.h"
#include "Utils.h"
#include <spine/TimeSeriesOutput.h>
#include <spine/Exception.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <macgyver/String.h>
#include <boost/algorithm/string/join.hpp>

#include <boost/foreach.hpp>
#include <boost/format.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

// #define MYDEBUG 1

QueryOpenData::QueryOpenData()
    : itsValueFormatter(new SmartMet::Spine::ValueFormatter(SmartMet::Spine::ValueFormatterParam()))
{
}
QueryOpenData::~QueryOpenData()
{
}

class my_visitor : public boost::static_visitor<double>
{
 public:
  my_visitor() {}
  double operator()(SmartMet::Spine::TimeSeries::None none) { return kFloatMissing; }
  double operator()(double luku) { return luku; }
  double operator()(const std::string& s) { return kFloatMissing; }
  double operator()(int i) { return static_cast<double>(i); }
  double operator()(boost::local_time::local_date_time i) { return kFloatMissing; }
  double operator()(SmartMet::Spine::TimeSeries::LonLat i) { return kFloatMissing; }
};

string QueryOpenData::makeParameterBlock(const SmartMet::Spine::Parameter& p, Oracle& oracle)
{
  try
  {
    string name = p.name();
    Fmi::ascii_tolower(name);

    bool isQC = false;

    std::string dataTable = oracle.getDatabaseTableName();

    std::string parameterColumn;
    std::string sensorColumn;
    std::string dataColumn;
    if (dataTable == "weather_data_qc")
    {
      parameterColumn = "parameter";
      sensorColumn = "sensor_no";
      dataColumn = "value";
    }
    else
    {
      parameterColumn = "measurand_id";
      sensorColumn = "measurand_no";
      dataColumn = "data_value";
    }

    // Do we want data_quality or data_value
    isQC = removePrefix(name, "qc_");

    // Kludge for WindCompass values
    if (name.find("windcompass") != std::string::npos)
    {
      string parameterBlock = "";
      parameterBlock += "MAX(CASE WHEN data." + parameterColumn + "=";
      parameterBlock +=
          "'" + Fmi::ascii_toupper_copy(oracle.parameterMap["winddirection"][oracle.stationType]) +
          "'";
      parameterBlock += " THEN data." + dataColumn + " END)";
      parameterBlock +=
          " KEEP(DENSE_RANK FIRST ORDER BY " + sensorColumn + " ASC) AS " + name + ",";
      return parameterBlock;
    }

    if (name.find("feelslike") != std::string::npos)
    {
      string parameterBlock = "";
      parameterBlock += "MAX(CASE WHEN data.measurand_id=";
      parameterBlock += oracle.parameterMap["temperature"][oracle.stationType];
      parameterBlock += " THEN data.data_value END)";
      parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY measurand_no ASC) AS meta_temperature,";

      parameterBlock += "MAX(CASE WHEN data.measurand_id=";
      parameterBlock += oracle.parameterMap["windspeedms"][oracle.stationType];
      parameterBlock += " THEN data.data_value END)";
      parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY measurand_no ASC) AS meta_windspeed,";

      parameterBlock += "MAX(CASE WHEN data.measurand_id=";
      parameterBlock += oracle.parameterMap["humidity"][oracle.stationType];
      parameterBlock += " THEN data.data_value END)";
      parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY measurand_no ASC) AS meta_humidity,";
      return parameterBlock;
    }

    std::string& measurandId = oracle.parameterMap[name][oracle.stationType];
    if (!measurandId.empty())
    {
      if (dataTable == "weather_data_qc")
      {
        Fmi::ascii_toupper(measurandId);
        measurandId = "'" + measurandId + "'";
      }

      std::string mainMeasurandId = oracle.parameterMap[name]["main_measurand_id"];

      string parameterBlock = "";

      if (dataTable == "weather_data_qc")
        parameterBlock += "MAX(CASE WHEN UPPER(data." + parameterColumn + ")=";
      else
        parameterBlock += "MAX(CASE WHEN data." + parameterColumn + "=";

      if (not mainMeasurandId.empty())
      {
        // Choose sub data (e.g. deviation of the main parameter)
        parameterBlock += mainMeasurandId;
        parameterBlock += " AND sm.submeasurand_id=";
        parameterBlock += measurandId;
        parameterBlock += " THEN";
        parameterBlock += " (CASE sm.COLUMN_NAME";
        parameterBlock += " WHEN 'VALUE_1' then data.VALUE_1";
        parameterBlock += " WHEN 'VALUE_2' then data.VALUE_2";
        parameterBlock += " WHEN 'VALUE_3' then data.VALUE_3";
        parameterBlock += " END)";
        parameterBlock += " END)";
        parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY measurand_no ASC) AS ";
        parameterBlock += name;
      }
      else if (isQC)
      {
        parameterBlock += measurandId;
        parameterBlock += " THEN data.data_quality END)";
        parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY measurand_no ASC) AS ";
        parameterBlock += Fmi::ascii_tolower_copy(p.name());
      }
      else
      {
        parameterBlock += measurandId;
        parameterBlock += " THEN data." + dataColumn + " END)";
        parameterBlock += " KEEP(DENSE_RANK FIRST ORDER BY " + sensorColumn + " ASC) AS ";
        parameterBlock += name;
      }

      parameterBlock += ",";
      return parameterBlock;
    }
    return "";
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 *  Create the main sql query in the case that no timestep is given, or it is 0.
 *  This means that all observations between a time interval should be returned
 *  and no missing values are subsituted
 */
string QueryOpenData::makeSQLWithNoTimestep(string& qstations, Settings& settings, Oracle& oracle)
{
  try
  {
    string dataTable = oracle.getDatabaseTableName();

    string measurandTable = "data_measurand";
    string locationTable = "locations";

    std::string idColumn;
    std::string timeColumn;
    std::string parameterColumn;
    if (dataTable == "weather_data_qc")
    {
      idColumn = "fmisid";
      timeColumn = "obstime";
      parameterColumn = "parameter";
    }
    else
    {
      idColumn = "station_id";
      timeColumn = "data_time";
      parameterColumn = "measurand_id";
    }

    std::list<std::string> producer_id_str_list;
    for (auto& prodId : settings.producer_ids)
      producer_id_str_list.push_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");

    string query = "";

    query +=
        "SELECT * FROM ("
        "SELECT data." +
        idColumn +
        " AS fmisid,"
        "data." +
        timeColumn + " AS obstime,";
    query +=
        "loc.location_id, "
        "loc.location_end, ";
    query += "FIRST_VALUE(loc.location_id)OVER(PARTITION BY data." + idColumn +
             " ORDER BY loc.location_end "
             "DESC) last_location, ";
    if (settings.latest)
    {
      query += "LAST_VALUE(data." + timeColumn + ") OVER(PARTITION BY data." + idColumn +
               ") max_obstime,";
    }

    query +=
        "loc.latitude AS lat,"
        "loc.longitude AS lon,"
        "loc.elevation AS elevation, ";
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (not_special(p))
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("windcompass") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
    }

    query = trimCommasFromEnd(query);

    query += " FROM ";
    query += dataTable;
    query += " data ";
    query += "JOIN locations loc ON (data." + idColumn + " = loc.fmisid) ";
    if (dataTable != "weather_data_qc")
    {
      query +=
          "LEFT OUTER JOIN REG_API_A.MEASURAND_SUBMEASURAND_V1 sm ON (sm.measurand_id = "
          "data.measurand_id and sm.producer_id = data.producer_id) ";
    }
    query += "WHERE data." + idColumn + " IN (" + qstations + ") ";
    query += "AND data." + timeColumn + " >= to_date('" + oracle.makeStringTime(oracle.startTime) +
             "', 'YYYYMMDDHH24MI') ";
    query += "AND data." + timeColumn + " <= to_date('" + oracle.makeStringTime(oracle.endTime) +
             "', 'YYYYMMDDHH24MI') ";
    if (not producerIds.empty())
      query += "AND data.producer_id IN (" + producerIds + ") ";
    if (dataTable == "weather_data_qc")
      query += "AND UPPER(data." + parameterColumn + ") IN (";
    else
      query += "AND data." + parameterColumn + " IN (";

    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (name.find("windcompass") != std::string::npos)
      {
        query += "'" +
                 Fmi::ascii_toupper_copy(oracle.parameterMap["winddirection"][oracle.stationType]) +
                 "',";
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += oracle.parameterMap["temperature"][oracle.stationType] + ",";
        query += oracle.parameterMap["windspeedms"][oracle.stationType] + ",";
        query += oracle.parameterMap["humidity"][oracle.stationType] + ",";
      }

      if (not_special(p))
      {
        if (!oracle.parameterMap[name][oracle.stationType].empty())
        {
          query += oracle.parameterMap[name][oracle.stationType] + ",";
        }
      }
    }
    query = trimCommasFromEnd(query);
    query += ") ";

    query += "GROUP BY data." + idColumn +
             ","
             "data." +
             timeColumn +
             ","
             "loc.location_id,"
             "loc.location_end,"
             "loc.latitude,"
             "loc.longitude,"
             "loc.elevation"
             ") ";
    if (settings.latest)
    {
      query += "WHERE obstime = max_obstime ";
      query += "AND location_id = last_location ";
    }
    else
    {
      query += "WHERE location_id = last_location ";
    }
    query += "AND obstime IS NOT NULL ";
    query += "ORDER BY fmisid ASC, obstime ASC";

#ifdef MYDEBUG
    std::cout << query << std::endl;
#endif

    return query;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 *  Create the main sql query in the case that timestep is given.
 */

string QueryOpenData::makeSQLWithTimestep(string& qstations, Settings& settings, Oracle& oracle)
{
  try
  {
    string dataTable = oracle.getDatabaseTableName();

    string measurandTable = "data_measurand";
    string locationTable = "locations";

    std::list<std::string> producer_id_str_list;
    for (const auto& prodId : settings.producer_ids)
      producer_id_str_list.push_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");
    string query = "";

    query +=
        "SELECT * FROM ("
        "SELECT fmisid AS fmisid,"
        "data_time AS obstime,";
    query +=
        "loc.location_id, "
        "loc.location_end, ";
    query +=
        "FIRST_VALUE(loc.location_id)OVER(PARTITION BY loc.fmisid ORDER BY loc.location_end DESC) "
        "last_location, ";

    if (settings.latest)
    {
      query += "LAST_VALUE(a.intervals) OVER(PARTITION BY loc.fmisid) max_obstime,";
    }

    query +=
        "loc.latitude AS lat,"
        "loc.longitude AS lon,"
        "loc.elevation AS elevation, ";
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (not_special(p))
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("windcompass") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
    }

    query = trimCommasFromEnd(query);

    query += " FROM locations loc";

    query += " LEFT OUTER JOIN ";
    query += dataTable;
    query += " data ";
    query += "ON (loc.fmisid = data.station_id ";
    query += "AND data.data_time >= to_date('" + oracle.makeStringTime(oracle.startTime) +
             "', 'YYYYMMDDHH24MI') ";
    query += "AND data.data_time <= to_date('" + oracle.makeStringTime(oracle.endTime) +
             "', 'YYYYMMDDHH24MI') ";
    query +=
        "AND MOD(60 * TO_NUMBER(TO_CHAR(data.data_time, 'HH24')) + "
        "TO_NUMBER(TO_CHAR(data.data_time, "
        "'MI')), " +
        Fmi::to_string(settings.timestep) + ") = 0 ";
    if (not producerIds.empty())
      query += "AND data.producer_id IN (" + producerIds + ") ";
    query += "AND data.measurand_id IN (";
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (name.find("windcompass") != std::string::npos)
      {
        query += oracle.parameterMap["winddirection"][oracle.stationType] + ",";
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += oracle.parameterMap["temperature"][oracle.stationType] + ",";
        query += oracle.parameterMap["windspeedms"][oracle.stationType] + ",";
        query += oracle.parameterMap["humidity"][oracle.stationType] + ",";
      }
      if (not_special(p))
      {
        if (!oracle.parameterMap[name][oracle.stationType].empty())
        {
          query += oracle.parameterMap[name][oracle.stationType] + ",";
        }
      }
    }
    query = trimCommasFromEnd(query);
    query += ") ";
    query += ") ";

    query +=
        "LEFT OUTER JOIN REG_API_A.MEASURAND_SUBMEASURAND_V1 sm ON (sm.measurand_id = "
        "data.measurand_id AND sm.producer_id = data.producer_id) ";

    query += "WHERE loc.fmisid IN(" + qstations + ") ";

    query +=
        "GROUP BY loc.fmisid,"
        "data.data_time,"
        "loc.location_id,"
        "loc.location_end,"
        "loc.latitude,"
        "loc.longitude,"
        "loc.elevation"
        ") ";
    if (settings.latest)
    {
      query += "WHERE obstime = max_obstime ";
      query += "AND location_id = last_location ";
    }
    else
    {
      query += "WHERE location_id = last_location ";
    }
    query += "AND obstime IS NOT NULL ";
    query += "ORDER BY fmisid ASC, obstime ASC";

#ifdef MYDEBUG
    std::cout << query << std::endl;
#endif

    return query;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 *  Create the main sql query in the case that a timeseries is given.
 *  We'll just fetch all values in the interval and prune later.
 */

string QueryOpenData::makeSQLWithTimeSeries(
    string& qstations,
    Settings& settings,
    Oracle& oracle,
    const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
    const Fmi::TimeZones& timezones)
{
  try
  {
    string dataTable = oracle.getDatabaseTableName();

    string measurandTable = "data_measurand";
    string locationTable = "locations";

    std::string idColumn;
    std::string timeColumn;
    std::string parameterColumn;
    if (dataTable == "weather_data_qc")
    {
      idColumn = "fmisid";
      timeColumn = "obstime";
      parameterColumn = "parameter";
    }
    else
    {
      idColumn = "station_id";
      timeColumn = "data_time";
      parameterColumn = "measurand_id";
    }

    std::list<std::string> producer_id_str_list;
    for (auto& prodId : settings.producer_ids)
      producer_id_str_list.push_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");

    string query = "";

    query +=
        "SELECT * FROM ("
        "SELECT data." +
        idColumn +
        " AS fmisid,"
        "data." +
        timeColumn + " AS obstime,";
    query +=
        "loc.location_id, "
        "loc.location_end, ";
    query += "FIRST_VALUE(loc.location_id)OVER(PARTITION BY data." + idColumn +
             " ORDER BY loc.location_end "
             "DESC) last_location, ";
    if (settings.latest)
    {
      query +=
          "LAST_VALUE(data." + idColumn + ") OVER(PARTITION BY data." + idColumn + ") max_obstime,";
    }

    query +=
        "loc.latitude AS lat,"
        "loc.longitude AS lon,"
        "loc.elevation AS elevation, ";
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (not_special(p))
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("windcompass") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += makeParameterBlock(p, oracle);
      }
    }

    query = trimCommasFromEnd(query);

    /*
     * There are multiple options on what the maximum time period we need is:
     *
     * if tz <> localtime, startime and endtime specify the maximal extend.
     * if tz == localtime
     *    if there are stations with one tz only, the case is simple again
     *    if there are multiple stations
     *       we could calculate the maximal extent
     *       or just add +-N hours to the period at both ends
     *
     * We opt for the last case and leave further optimization to the TODO-list
     */

    boost::posix_time::ptime periodstarttime;
    boost::posix_time::ptime periodendtime;

    if (settings.timezone != "localtime")
    {
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      if (tlist.empty())
        throw SmartMet::Spine::Exception(BCP, "Querying observations for an empty time series");

      periodstarttime = tlist.front().utc_time();
      periodendtime = tlist.back().utc_time();
    }
    else
    {
      periodstarttime = timeSeriesOptions.startTime - hours(15);
      periodendtime = timeSeriesOptions.endTime + hours(15);
    }

    const auto& startsql = oracle.makeStringTime(oracle.makeOTLTime(periodstarttime));
    const auto& endsql = oracle.makeStringTime(oracle.makeOTLTime(periodendtime));

    query += " FROM ";
    query += dataTable;
    query += " data ";
    query += "JOIN locations loc ON (data." + idColumn + " = loc.fmisid) ";
    if (dataTable != "weather_data_qc")
    {
      query +=
          "LEFT OUTER JOIN REG_API_A.MEASURAND_SUBMEASURAND_V1 sm ON (sm.measurand_id = "
          "data.measurand_id and sm.producer_id = data.producer_id) ";
    }
    query += "WHERE data." + idColumn + " IN (" + qstations + ") ";
    query += "AND data." + timeColumn + " >= to_date('" + startsql + "', 'YYYYMMDDHH24MI') ";
    query += "AND data." + timeColumn + " <= to_date('" + endsql + "', 'YYYYMMDDHH24MI') ";
    if (not producerIds.empty())
      query += "AND data.producer_id IN (" + producerIds + ") ";

    if (dataTable == "weather_data_qc")
      query += "AND UPPER(data." + parameterColumn + ") IN (";
    else
      query += "AND data." + parameterColumn + " IN (";

    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      string name = p.name();
      Fmi::ascii_tolower(name);
      removePrefix(name, "qc_");
      if (name.find("windcompass") != std::string::npos)
      {
        query += "'" +
                 Fmi::ascii_toupper_copy(oracle.parameterMap["winddirection"][oracle.stationType]) +
                 "',";
      }
      else if (name.find("feelslike") != std::string::npos)
      {
        query += oracle.parameterMap["temperature"][oracle.stationType] + ",";
        query += oracle.parameterMap["windspeedms"][oracle.stationType] + ",";
        query += oracle.parameterMap["humidity"][oracle.stationType] + ",";
      }

      if (not_special(p))
      {
        if (!oracle.parameterMap[name][oracle.stationType].empty())
        {
          query += oracle.parameterMap[name][oracle.stationType] + ",";
        }
      }
    }
    query = trimCommasFromEnd(query);
    query += ") ";

    query += "GROUP BY data." + idColumn +
             ","
             "data." +
             timeColumn +
             ","
             "loc.location_id,"
             "loc.location_end,"
             "loc.latitude,"
             "loc.longitude,"
             "loc.elevation"
             ") ";
    if (settings.latest)
    {
      query += "WHERE obstime = max_obstime ";
      query += "AND location_id = last_location ";
    }
    else
    {
      query += "WHERE location_id = last_location ";
    }
    query += "AND obstime IS NOT NULL ";
    query += "ORDER BY fmisid ASC, obstime ASC";

#ifdef MYDEBUG
    std::cout << query << std::endl;
#endif

    return query;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryOpenData::ping(Oracle& oracle)
{
  try
  {
    string query = "SELECT 1 FROM dual";

    otl_stream stream;

    try
    {
      stream.open(1, query.c_str(), oracle.getConnection());
    }
    catch (otl_exception&)
    {
      oracle.beginSession();
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryOpenData::selectOne(Oracle& oracle)
{
  try
  {
    string query = "SELECT 1 FROM dual";

    otl_stream stream;

    double one;

    try
    {
      stream.open(1, query.c_str(), oracle.getConnection());
      stream.set_commit(0);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;
      iterator.attach(stream);

      while (iterator.next_row())
      {
        iterator.get(1, one);
      }
      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (oracle.isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        oracle.reConnect();
        return selectOne(oracle);
      }
      else
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string getFMISID(const ts::Value& tv)
{
  try
  {
    stringstream ss;
    ss << tv;
    return ss.str();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string QueryOpenData::tableName(const string& stationtype)
{
  try
  {
    if (stationtype == "opendata_daily")
    {
      return "climatological_data";
    }
    else if (stationtype == "opendata_minute" || stationtype == "research")
    {
      return "minute_data";
    }
    else if (stationtype == "opendata" || stationtype == "opendata_buoy" ||
             stationtype == "opendata_mareograph" || stationtype == "syke")
    {
      return "observation_data";
    }
    else
    {
      throw SmartMet::Spine::Exception(BCP, "[ObsEngine] Error: bad stationtype: " + stationtype);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> QueryOpenData::executeQuery(
    Oracle& oracle,
    SmartMet::Spine::Stations& stations,
    Settings& settings,
    const Fmi::TimeZones& timezones)
{
  try
  {
    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;

    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    for (const SmartMet::Spine::Station& s : stations)
    {
      itsStations.insert(std::make_pair(Fmi::to_string(s.station_id), s));
    }

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    map<string, int> specialsOrder;

    int j = 0;
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      if (!not_special(p))
      {
        string name = p.name();
        Fmi::ascii_toupper(name);
        specialsOrder.insert(std::make_pair(name, j));
      }
      j++;
    }

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    map<string, int> queryparamindex;
    string queryparams = "";
    string firstquery = "";

    oracle.makeParamIndexesForWeatherDataQC(
        settings.parameters, paramindex, specialsindex, queryparams, queryparamindex);

    if (itsTimeSeriesColumns)
    {
      for (unsigned int i = 0; i < specialsOrder.size() + paramindex.size(); i++)
      {
        itsTimeSeriesColumns->push_back(ts::TimeSeries());
      }
    }

    string query;
    if (settings.timestep > 1)
    {
      query = makeSQLWithTimestep(qstations, settings, oracle);
    }
    else
    {
      query = makeSQLWithNoTimestep(qstations, settings, oracle);
    }

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, query.c_str(), oracle.getConnection());
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      otl_datetime timestamp;

      int row = 0;

      otl_var_desc* desc;
      int desc_len;
      desc = stream.describe_out_vars(desc_len);

      otl_column_desc* columnDesc = stream.describe_select(desc_len);

      double value = 0;

      SmartMet::Spine::TimeSeriesGeneratorOptions opt;
      opt.startTime = oracle.makePosixTime(oracle.startTime, timezones);
      opt.endTime = oracle.makePosixTime(oracle.endTime, timezones);
      if (settings.timestep == 0)
      {
        opt.timeStep = 1;
      }
      else
      {
        opt.timeStep = settings.timestep;
      }
      opt.startTimeUTC = false;
      opt.endTimeUTC = false;
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          opt, timezones.time_zone_from_string(settings.timezone));
      auto timeIterator = std::begin(tlist);
      std::string currentFMISID = "";
      boost::local_time::local_date_time currentTime(boost::local_time::not_a_date_time);
      std::map<std::string, ts::Value> lastValues;

      // key => value
      map<string, string> values;
      // time series structure
      map<string, ts::Value> ts_values;

      while (iterator.next_row())
      {
        // Get the data!
        for (int i = 1; i <= desc_len; i++)
        {
          // Check NULL values first
          if (iterator.is_null(i))
          {
#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << settings.missingtext << std::endl;
#endif
            if (itsTimeSeriesColumns)
              ts_values[desc[i - 1].name] = ts::None();
            else
              values[desc[i - 1].name] = settings.missingtext;

            continue;
          }

          // Re-use the previously declared stream and reset flags
          ss.str("");
          ss.clear();

          if (columnDesc[i - 1].otl_var_dbtype != 8)  // otl_var_dbtype = 8 => date
          {
            iterator.get(i, value);

#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << value << std::endl;
#endif
            if (itsTimeSeriesColumns)
            {
              if (boost::lexical_cast<std::string>(desc[i - 1].name) == "FMISID")
              {
                ss << fixed << setprecision(0) << value;
                ts_values[desc[i - 1].name] = ss.str();
              }
              else
              {
                ts_values[desc[i - 1].name] = value;
              }
            }
            else
            {
              if (boost::lexical_cast<std::string>(desc[i - 1].name) == "FMISID" ||
                  boost::lexical_cast<std::string>(desc[i - 1].name).compare(0, 3, "QC_") == 0)
              {
                ss << fixed << setprecision(0) << value;
              }
              else if (boost::lexical_cast<std::string>(desc[i - 1].name) == "LAT" ||
                       boost::lexical_cast<std::string>(desc[i - 1].name) == "LON")
              {
                ss << fixed << setprecision(5) << value;
              }
              else
              {
                ss << fixed << setprecision(1) << value;
              }
              values[desc[i - 1].name] = ss.str();
            }
          }
          // Time related values must be last so that FMISID has been got
          else if (boost::lexical_cast<std::string>(desc[i - 1].name) == "OBSTIME")
          {
            iterator.get(i, timestamp);

#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << timestamp << std::endl;
#endif
            std::string fmisid(getFMISID(ts_values["FMISID"]));
            ;

            // DB must provide FMISID if localtime is requested
            if ((fmisid.empty() || fmisid == "nan") && settings.timezone == "localtime")
              throw SmartMet::Spine::Exception(
                  BCP, "FMISID is required for all stations if localtime is requested");

            std::string zone(settings.timezone == "localtime" ? itsStations.at(fmisid).timezone
                                                              : settings.timezone);
            auto localtz = timezones.time_zone_from_string(zone);

            boost::posix_time::ptime utctime = oracle.makePosixTime(timestamp, timezones, "UTC");

            if (itsTimeSeriesColumns)
            {
              currentTime = local_date_time(utctime, localtz);
              ts_values["OBSTIME"] = currentTime;

              ts_values["UTCTIME"] = oracle.timeFormatter->format(utctime);
              ts_values["LOCALTIME"] = oracle.timeFormatter->format(
                  oracle.makePosixTime(timestamp, timezones, itsStations.at(fmisid).timezone));
              ts_values["EPOCHTIME"] = oracle.makeEpochTime(utctime);
              ts_values["ORIGINTIME"] =
                  oracle.timeFormatter->format(boost::posix_time::second_clock::universal_time());
              if (settings.timezone == "localtime")
              {
                ts_values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, itsStations.at(fmisid).timezone));
              }
              else
              {
                ts_values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, settings.timezone));
              }
            }
            else
            {
              // Perhaps better to check if these are needed, date conversions are potentially slow

              values["UTCTIME"] = oracle.timeFormatter->format(utctime);

              values["LOCALTIME"] = oracle.timeFormatter->format(oracle.makePosixTime(
                  timestamp, timezones, itsStations.at(values["FMISID"]).timezone));

              values["EPOCHTIME"] = oracle.makeEpochTime(utctime);

              values["ORIGINTIME"] =
                  oracle.timeFormatter->format(boost::posix_time::second_clock::universal_time());

              if (settings.timezone == "localtime")
              {
                values["TIME"] = oracle.timeFormatter->format(oracle.makePosixTime(
                    timestamp, timezones, itsStations.at(values["FMISID"]).timezone));
              }
              else
              {
                values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, settings.timezone));
              }
            }
          }
        }

        if (itsTimeSeriesColumns)
        {
          if (settings.timestep > 1)
          {
            if (currentFMISID != getFMISID(ts_values["FMISID"]))
            {
              timeIterator = std::begin(tlist);
            }
            while (*timeIterator < currentTime)
            {
              ts_values["OBSTIME"] = *timeIterator;
              fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings, true);
              timeIterator++;
            }
            ts_values["OBSTIME"] = currentTime;
            fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings);
            currentFMISID = getFMISID(ts_values["FMISID"]);
            timeIterator++;
          }
          else
          {
            fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings);
          }
        }
        else
        {
          fillSpecials(*result, values, specialsOrder, settings, row);
          fillData(*result, values, paramindex, row);
        }
      }

      iterator.detach();
      stream.close();
    }

    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (oracle.isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        oracle.reConnect();
        return executeQuery(oracle, stations, settings, timezones);
      }

      if (p.code == 32036)  // not connected!
      {
        oracle.attach();
        oracle.beginSession();
        executeQuery(oracle, stations, settings, timezones);
      }
      else
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
      }
    }

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryOpenData::fillSpecials(SmartMet::Spine::Table& result,
                                 map<string, string>& values,
                                 map<string, int>& specialsOrder,
                                 Settings& settings,
                                 int row)
{
  try
  {
    typedef map<string, int> type;

    string fmisid = values["FMISID"];

    for (const type::value_type& p : specialsOrder)
    {
      // Values from database
      if (p.first == "FMISID" || p.first == "ELEVATION")
      {
        result.set(p.second, row, values[p.first]);
      }

      else if (p.first == "LAT" || p.first == "LATITUDE" || p.first == "STATIONLATITUDE")
      {
        result.set(p.second, row, values["LAT"]);
      }
      else if (p.first == "LON" || p.first == "LONGITUDE" || p.first == "STATIONLONGITUDE")
      {
        result.set(p.second, row, values["LON"]);
      }

      // Time values
      else if (p.first == "TIME")
      {
        result.set(p.second, row, values["TIME"]);
      }

      else if (p.first == "UTCTIME")
      {
        result.set(p.second, row, values["UTCTIME"]);
      }

      else if (p.first == "EPOCHTIME")
      {
        result.set(p.second, row, values["EPOCHTIME"]);
      }

      else if (p.first == "LOCALTIME")
      {
        result.set(p.second, row, values["LOCALTIME"]);
      }

      else if (p.first == "ORIGINTIME")
      {
        result.set(p.second, row, values["ORIGINTIME"]);
      }

      else if (p.first == "TZ")
      {
        if (settings.timezone == "localtime")
        {
          result.set(p.second, row, itsStations.at(fmisid).timezone);
        }
        else
        {
          result.set(p.second, row, settings.timezone);
        }
      }

      // Values from a Station

      else if (p.first == "NAME")
      {
        if (itsStations.at(fmisid).requestedName.length() > 0)
        {
          result.set(p.second, row, itsStations.at(fmisid).requestedName);
        }
        else
        {
          result.set(p.second, row, itsStations.at(fmisid).station_formal_name);
        }
      }

      else if (p.first == "DISTANCE")
      {
        if (!settings.boundingBoxIsGiven)
          result.set(p.second, row, itsStations.at(fmisid).distance);
        else
          result.set(p.second, row, settings.missingtext);
      }

      else if (p.first == "REGION")
      {
        result.set(p.second, row, itsStations.at(fmisid).region);
      }

      else if (p.first == "DIRECTION")
      {
        string dir = itsValueFormatter->format(itsStations.at(fmisid).stationDirection, 1);
        if (!settings.boundingBoxIsGiven)
          result.set(p.second, row, dir);
        else
          result.set(p.second, row, settings.missingtext);
      }

      else if (p.first == "STATIONNAME")
      {
        result.set(p.second, row, itsStations.at(fmisid).station_formal_name);
      }

      else if (p.first == "GEOID")
      {
        result.set(p.second, row, Fmi::to_string(itsStations.at(fmisid).geoid));
      }

      else if (p.first == "WMO")
      {
        if (itsStations.at(fmisid).wmo > 0)
        {
          result.set(p.second, row, Fmi::to_string(itsStations.at(fmisid).wmo));
        }
        else
        {
          result.set(p.second, row, settings.missingtext);
        }
      }

      // WindCompass values are a special case
      else if (p.first == "WINDCOMPASS8")
      {
        string windCompass = windCompass8(Fmi::stod(values["WINDCOMPASS8"]));
        result.set(p.second, row, windCompass);
      }
      else if (p.first == "WINDCOMPASS16")
      {
        string windCompass = windCompass16(Fmi::stod(values["WINDCOMPASS16"]));
        result.set(p.second, row, windCompass);
      }
      else if (p.first == "WINDCOMPASS32")
      {
        string windCompass = windCompass32(Fmi::stod(values["WINDCOMPASS32"]));
        result.set(p.second, row, windCompass);
      }
      else if (p.first == "FEELSLIKE")
      {
        std::string wind = values["META_WINDSPEED"];
        std::string rh = values["META_HUMIDITY"];
        std::string temp = values["META_TEMPERATURE"];

        if (wind == settings.missingtext || rh == settings.missingtext ||
            temp == settings.missingtext)
        {
          result.set(p.second, row, settings.missingtext);
        }
        else
        {
          double feelsLike = FmiFeelsLikeTemperature(
              Fmi::stod(wind), Fmi::stod(rh), Fmi::stod(temp), kFloatMissing);
          if (feelsLike != kFloatMissing)
          {
            result.set(p.second, row, Fmi::to_string(feelsLike));
          }
          else
          {
            result.set(p.second, row, settings.missingtext);
          }
        }
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryOpenData::fillData(SmartMet::Spine::Table& result,
                             map<string, string>& values,
                             map<string, int>& paramindex,
                             int& row)
{
  try
  {
    typedef map<string, int> type;
    for (const type::value_type& p : paramindex)
    {
      result.set(p.second, row, values[p.first]);
    }

    row++;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryOpenData::fillTimeSeriesData(map<string, ts::Value>& values,
                                       map<string, int>& specialsOrder,
                                       map<string, int>& paramindex,
                                       Settings& settings,
                                       bool fillMissingValues)
{
  try
  {
    typedef map<string, int> type;

    std::string fmisid(getFMISID(values["FMISID"]));
    ;

    local_date_time obstime(not_a_date_time);
    if (values.find("OBSTIME") != values.end())
      obstime = *(boost::get<boost::local_time::local_date_time>(&values["OBSTIME"]));

    for (const type::value_type& p : specialsOrder)
    {
      // Values from database
      if (p.first == "FMISID" || p.first == "ELEVATION")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values[p.first]));
      }

      else if (p.first == "LAT" || p.first == "LATITUDE" || p.first == "STATIONLATITUDE" ||
               p.first == "STATIONLAT")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["LAT"]));
      }
      else if (p.first == "LON" || p.first == "LONGITUDE" || p.first == "STATIONLONGITUDE" ||
               p.first == "STATIONLON")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["LON"]));
      }

      // Time values
      else if (p.first == "TIME")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["TIME"]));
      }

      else if (p.first == "UTCTIME")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["UTCTIME"]));
      }

      else if (p.first == "EPOCHTIME")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["EPOCTIME"]));
      }

      else if (p.first == "LOCALTIME")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["LOCALTIME"]));
      }

      else if (p.first == "ORIGINTIME")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, values["ORIGINTIME"]));
      }

      else if (p.first == "TZ")
      {
        if (settings.timezone == "localtime")
        {
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).timezone));
        }
        else
        {
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, settings.timezone));
        }
      }

      // Values from a Station

      else if (p.first == "NAME")
      {
        if (itsStations.at(fmisid).requestedName.length() > 0)
        {
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).requestedName));
        }
        else
        {
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).station_formal_name));
        }
      }

      else if (p.first == "DISTANCE")
      {
        if (!settings.boundingBoxIsGiven)
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).distance));
        else
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, ts::None()));
      }

      else if (p.first == "REGION")
      {
        itsTimeSeriesColumns->at(p.second)
            .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).region));
      }

      else if (p.first == "DIRECTION")
      {
        string dir = itsValueFormatter->format(itsStations.at(fmisid).stationDirection, 1);
        if (!settings.boundingBoxIsGiven)
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, dir));
        else
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, ts::None()));
      }

      else if (p.first == "STATIONNAME")
      {
        itsTimeSeriesColumns->at(p.second)
            .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).station_formal_name));
      }

      else if (p.first == "GEOID")
      {
        itsTimeSeriesColumns->at(p.second)
            .push_back(ts::TimedValue(obstime, Fmi::to_string(itsStations.at(fmisid).geoid)));
      }

      else if (p.first == "MODEL")
      {
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, settings.stationtype));
      }

      else if (p.first == "WMO")
      {
        if (itsStations.at(fmisid).wmo > 0)
        {
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).wmo));
        }
        else
        {
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, ts::None()));
        }
      }

      else if (p.first == "LPNN")
      {
        if (itsStations.at(fmisid).lpnn > 0)
        {
          itsTimeSeriesColumns->at(p.second)
              .push_back(ts::TimedValue(obstime, itsStations.at(fmisid).lpnn));
        }
        else
        {
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, ts::None()));
        }
      }
      // WindCompass values are a special case
      else if (p.first == "WINDCOMPASS8")
      {
        my_visitor doubleVisitor;
        string windCompass =
            windCompass8(boost::apply_visitor(doubleVisitor, values["WINDCOMPASS8"]));
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, windCompass));
      }
      else if (p.first == "WINDCOMPASS16")
      {
        my_visitor doubleVisitor;
        string windCompass =
            windCompass16(boost::apply_visitor(doubleVisitor, values["WINDCOMPASS16"]));
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, windCompass));
      }
      else if (p.first == "WINDCOMPASS32")
      {
        my_visitor doubleVisitor;
        string windCompass =
            windCompass32(boost::apply_visitor(doubleVisitor, values["WINDCOMPASS32"]));
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, windCompass));
      }
      else if (p.first == "PLACE")
      {
        std::string tag = itsStations.at(fmisid).tag;
        itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, tag));
      }
      else if (p.first == "FEELSLIKE")
      {
        my_visitor doubleVisitor;

        double wind = boost::apply_visitor(doubleVisitor, values["META_WINDSPEED"]);
        double rh = boost::apply_visitor(doubleVisitor, values["META_HUMIDITY"]);
        double temp = boost::apply_visitor(doubleVisitor, values["META_TEMPERATURE"]);

        double feelsLike = FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing);
        if (feelsLike != kFloatMissing)
        {
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, feelsLike));
        }
        else
        {
          itsTimeSeriesColumns->at(p.second).push_back(ts::TimedValue(obstime, ts::None()));
        }
      }
      else
      {
        std::ostringstream msg;
        msg << "SmartMet::QueryOpenData::fillTimeSeriesData : Unsupported special parameter '"
            << p.first << "'.";

        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }
    }

    for (const type::value_type& par : paramindex)
    {
      if (fillMissingValues)
      {
        itsTimeSeriesColumns->at(par.second).push_back(ts::TimedValue(obstime, ts::None()));
      }
      else
      {
        itsTimeSeriesColumns->at(par.second).push_back(ts::TimedValue(obstime, values[par.first]));
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr QueryOpenData::values(
    Oracle& oracle,
    SmartMet::Spine::Stations& stations,
    Settings& settings,
    const Fmi::TimeZones& timezones)
{
  try
  {
    itsTimeSeriesColumns = SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
        new SmartMet::Spine::TimeSeries::TimeSeriesVector);

    executeQuery(oracle, stations, settings, timezones);

    return itsTimeSeriesColumns;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr QueryOpenData::values(
    Oracle& oracle,
    SmartMet::Spine::Stations& stations,
    Settings& settings,
    const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
    const Fmi::TimeZones& timezones)
{
  try
  {
    itsTimeSeriesColumns = SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
        new SmartMet::Spine::TimeSeries::TimeSeriesVector);

    executeQuery(oracle, stations, settings, timeSeriesOptions, timezones);

    return itsTimeSeriesColumns;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> QueryOpenData::executeQuery(
    Oracle& oracle,
    SmartMet::Spine::Stations& stations,
    Settings& settings,
    const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
    const Fmi::TimeZones& timezones)
{
  try
  {
    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;

    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    for (const SmartMet::Spine::Station& s : stations)
    {
      itsStations.insert(std::make_pair(Fmi::to_string(s.station_id), s));
    }

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    map<string, int> specialsOrder;

    int j = 0;
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      if (!not_special(p))
      {
        string name = p.name();
        Fmi::ascii_toupper(name);
        specialsOrder.insert(std::make_pair(name, j));
      }
      j++;
    }

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    map<string, int> queryparamindex;
    string queryparams = "";
    string firstquery = "";

    oracle.makeParamIndexesForWeatherDataQC(
        settings.parameters, paramindex, specialsindex, queryparams, queryparamindex);

    if (itsTimeSeriesColumns)
    {
      for (unsigned int i = 0; i < specialsOrder.size() + paramindex.size(); i++)
      {
        itsTimeSeriesColumns->push_back(ts::TimeSeries());
      }
    }

    string query;
    if (timeSeriesOptions.all())
      query = makeSQLWithNoTimestep(qstations, settings, oracle);
    else
      query = makeSQLWithTimeSeries(qstations, settings, oracle, timeSeriesOptions, timezones);

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, query.c_str(), oracle.getConnection());
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      otl_datetime timestamp;

      int row = 0;

      otl_var_desc* desc;
      int desc_len;
      desc = stream.describe_out_vars(desc_len);

      otl_column_desc* columnDesc = stream.describe_select(desc_len);

      double value = 0;

      // The desired timeseries, or all allowed minutes if timestep=0
      SmartMet::Spine::TimeSeriesGenerator::LocalTimeList tlist;
      if (!timeSeriesOptions.all())
        tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
            timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

      auto timeIterator = std::begin(tlist);
      std::string currentFMISID = "";
      boost::local_time::local_date_time currentTime(boost::local_time::not_a_date_time);
      std::map<std::string, ts::Value> lastValues;

      // key => value
      map<string, string> values;
      // time series structure
      map<string, ts::Value> ts_values;

      while (iterator.next_row())
      {
        // Get the data!
        for (int i = 1; i <= desc_len; i++)
        {
          // Check NULL values first
          if (iterator.is_null(i))
          {
#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << settings.missingtext << std::endl;
#endif
            if (itsTimeSeriesColumns)
              ts_values[desc[i - 1].name] = ts::None();
            else
              values[desc[i - 1].name] = settings.missingtext;

            continue;
          }

          // Re-use the previously declared stream and reset flags
          ss.str("");
          ss.clear();

          if (columnDesc[i - 1].otl_var_dbtype != 8)  // otl_var_dbtype = 8 => date
          {
            iterator.get(i, value);

#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << value << std::endl;
#endif
            if (itsTimeSeriesColumns)
            {
              if (boost::lexical_cast<std::string>(desc[i - 1].name) == "FMISID")
              {
                ss << fixed << setprecision(0) << value;
                ts_values[desc[i - 1].name] = ss.str();
              }
              else
              {
                ts_values[desc[i - 1].name] = value;
              }
            }
            else
            {
              if (boost::lexical_cast<std::string>(desc[i - 1].name) == "FMISID" ||
                  boost::lexical_cast<std::string>(desc[i - 1].name).compare(0, 3, "QC_") == 0)
              {
                ss << fixed << setprecision(0) << value;
              }
              else if (boost::lexical_cast<std::string>(desc[i - 1].name) == "LAT" ||
                       boost::lexical_cast<std::string>(desc[i - 1].name) == "LON")
              {
                ss << fixed << setprecision(5) << value;
              }
              else
              {
                ss << fixed << setprecision(1) << value;
              }
              values[desc[i - 1].name] = ss.str();
            }
          }
          // Time related values must be last so that FMISID has been got
          else if (boost::lexical_cast<std::string>(desc[i - 1].name) == "OBSTIME")
          {
            iterator.get(i, timestamp);

#ifdef MYDEBUG
            std::cout << desc[i - 1].name << " -> " << timestamp << std::endl;
#endif
            std::string fmisid(getFMISID(ts_values["FMISID"]));
            ;

            // DB must provide FMISID if localtime is requested
            if (fmisid.empty() && settings.timezone == "localtime")
              throw SmartMet::Spine::Exception(
                  BCP, "FMISID is required for all stations if localtime is requested");

            std::string zone(settings.timezone == "localtime" ? itsStations.at(fmisid).timezone
                                                              : settings.timezone);
            auto localtz = timezones.time_zone_from_string(zone);

            boost::posix_time::ptime utctime = oracle.makePosixTime(timestamp, timezones, "UTC");

            if (itsTimeSeriesColumns)
            {
              currentTime = local_date_time(utctime, localtz);
              ts_values["OBSTIME"] = currentTime;

              ts_values["UTCTIME"] = oracle.timeFormatter->format(utctime);
              ts_values["LOCALTIME"] = oracle.timeFormatter->format(
                  oracle.makePosixTime(timestamp, timezones, itsStations.at(fmisid).timezone));
              ts_values["EPOCHTIME"] = oracle.makeEpochTime(utctime);
              ts_values["ORIGINTIME"] =
                  oracle.timeFormatter->format(boost::posix_time::second_clock::universal_time());
              if (settings.timezone == "localtime")
              {
                ts_values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, itsStations.at(fmisid).timezone));
              }
              else
              {
                ts_values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, settings.timezone));
              }
            }
            else
            {
              // Perhaps better to check if these are needed, date conversions are potentially slow

              values["UTCTIME"] = oracle.timeFormatter->format(utctime);

              values["LOCALTIME"] = oracle.timeFormatter->format(oracle.makePosixTime(
                  timestamp, timezones, itsStations.at(values["FMISID"]).timezone));

              values["EPOCHTIME"] = oracle.makeEpochTime(utctime);

              values["ORIGINTIME"] =
                  oracle.timeFormatter->format(boost::posix_time::second_clock::universal_time());

              if (settings.timezone == "localtime")
              {
                values["TIME"] = oracle.timeFormatter->format(oracle.makePosixTime(
                    timestamp, timezones, itsStations.at(values["FMISID"]).timezone));
              }
              else
              {
                values["TIME"] = oracle.timeFormatter->format(
                    oracle.makePosixTime(timestamp, timezones, settings.timezone));
              }
            }
          }
        }

        if (itsTimeSeriesColumns)
        {
          if (timeSeriesOptions.all())
            fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings);
          else
          {
            // Accept only times in the generated list

            if (currentFMISID != getFMISID(ts_values["FMISID"]))
              timeIterator = std::begin(tlist);

            while (timeIterator != tlist.end() && *timeIterator < currentTime)
            {
              // Insert skipped times with missing values
              ts_values["OBSTIME"] = *timeIterator;
              fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings, true);
              timeIterator++;
            }

            // Insert the actual observations only if the time matches
            if (timeIterator != tlist.end() && *timeIterator == currentTime)
            {
              ts_values["OBSTIME"] = currentTime;
              fillTimeSeriesData(ts_values, specialsOrder, paramindex, settings);
              ++timeIterator;
            }

            currentFMISID = getFMISID(ts_values["FMISID"]);
          }
        }
        else
        {
          fillSpecials(*result, values, specialsOrder, settings, row);
          fillData(*result, values, paramindex, row);
        }
      }

      iterator.detach();
      stream.close();
    }

    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (oracle.isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        oracle.reConnect();
        return executeQuery(oracle, stations, settings, timeSeriesOptions, timezones);
      }

      if (p.code == 32036)  // not connected!
      {
        oracle.attach();
        oracle.beginSession();
        executeQuery(oracle, stations, settings, timeSeriesOptions, timezones);
      }
      else
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
      }
    }

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
