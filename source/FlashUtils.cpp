#include "FlashUtils.h"
#include "FlashQuery.h"
#include <spine/Exception.h>
#include <macgyver/String.h>
#include <boost/lexical_cast.hpp>

namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
FlashUtils::FlashUtils()
{
}

boost::shared_ptr<SmartMet::Spine::Table> FlashUtils::getData(
    Oracle& oracle,
    Settings& settings,
    boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter,
    const Fmi::TimeZones& timezones)
{
  try
  {
    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    FlashQuery flashQuery;

    string query = flashQuery.createQuery(oracle, settings);

    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";

    oracle.makeParamIndexes(settings.parameters, paramindex, specialsindex, queryparams);

    auto parameterMap = createParameterMap(settings.parameters);

    otl_datetime stroke_time;
    int flash_id = 0;
    double longitude = 0;
    double latitude = 0;

    map<string, double> resultRow;

    otl_stream stream;
    try
    {
      stream.open(1, query.c_str(), oracle.getConnection());
      stream.set_commit(0);
      stream << oracle.makeOTLTime(settings.starttime) << oracle.makeOTLTime(settings.endtime);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;
      iterator.attach(stream);

      // Info about the parameters in the output
      otl_var_desc* desc;
      int desc_len;
      desc = stream.describe_out_vars(desc_len);

      int row = 0;

      while (iterator.next_row())
      {
        // Static data which is gathered always
        iterator.get(1, stroke_time);
        iterator.get(2, flash_id);
        iterator.get(3, longitude);
        iterator.get(4, latitude);

        formatTimeParameters(settings, result, stroke_time, oracle, parameterMap, row, timezones);

        boost::posix_time::ptime utctime = makeFlashTime(stroke_time, "UTC", timezones);
        auto localtz = timezones.time_zone_from_string(settings.timezone);
        local_date_time localtime(utctime, localtz);

        if (isWantedParameter("flash_id", parameterMap))
        {
          if (itsTimeSeriesColumns)
            setTimeSeriesValue(parameterMap["flash_id"],
                               ts::TimedValue(localtime, valueFormatter->format(flash_id, 0)));
          else
            result->set(parameterMap["flash_id"], row, valueFormatter->format(flash_id, 0));
        }
        if (isWantedParameter("longitude", parameterMap))
        {
          if (itsTimeSeriesColumns)
            setTimeSeriesValue(parameterMap["longitude"], ts::TimedValue(localtime, longitude));
          else
            result->set(parameterMap["longitude"], row, valueFormatter->format(longitude, 4));
        }
        if (isWantedParameter("latitude", parameterMap))
        {
          if (itsTimeSeriesColumns)
            setTimeSeriesValue(parameterMap["latitude"], ts::TimedValue(localtime, latitude));
          else
            result->set(parameterMap["latitude"], row, valueFormatter->format(latitude, 4));
        }

        double value = 0.0;
        for (int i = 5; i <= desc_len; i++)
        {
          iterator.get(i, value);
          string paramName = desc[i - 1].name;
          Fmi::ascii_tolower(paramName);  // must use lower case names

          if (itsTimeSeriesColumns)
            setTimeSeriesValue(parameterMap[paramName], ts::TimedValue(localtime, value));
          else
            result->set(parameterMap[paramName], row, valueFormatter->format(value, 4));
        }

        row++;
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
        return getData(oracle, settings, valueFormatter, timezones);
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

void FlashUtils::formatTimeParameters(const Settings& settings,
                                      boost::shared_ptr<SmartMet::Spine::Table>& result,
                                      const otl_datetime& stroke_time,
                                      const Oracle& oracle,
                                      map<string, int>& parameterMap,
                                      const int row,
                                      const Fmi::TimeZones& timezones)
{
  try
  {
    boost::posix_time::ptime utctime = makeFlashTime(stroke_time, "UTC", timezones);
    auto localtz = timezones.time_zone_from_string(settings.timezone);
    local_date_time localtime(utctime, localtz);

    if (isWantedParameter("utctime", parameterMap))
    {
      if (itsTimeSeriesColumns)
        setTimeSeriesValue(parameterMap["utctime"],
                           ts::TimedValue(localtime, oracle.timeFormatter->format(utctime)));
      else
        result->set(parameterMap["utctime"], row, oracle.timeFormatter->format(utctime));
    }
    if (isWantedParameter("time", parameterMap))
    {
      if (itsTimeSeriesColumns)
        setTimeSeriesValue(parameterMap["time"],
                           ts::TimedValue(localtime,
                                          oracle.timeFormatter->format(makeFlashTime(
                                              stroke_time, settings.timezone, timezones))));
      else
        result->set(
            parameterMap["time"],
            row,
            oracle.timeFormatter->format(makeFlashTime(stroke_time, settings.timezone, timezones)));
    }
    if (isWantedParameter("localtime", parameterMap))
    {
      if (itsTimeSeriesColumns)
        setTimeSeriesValue(parameterMap["localtime"],
                           ts::TimedValue(localtime,
                                          oracle.timeFormatter->format(makeFlashTime(
                                              stroke_time, settings.timezone, timezones))));
      else
        result->set(
            parameterMap["localtime"],
            row,
            oracle.timeFormatter->format(makeFlashTime(stroke_time, settings.timezone, timezones)));
    }
    if (isWantedParameter("epochtime", parameterMap))
    {
      if (itsTimeSeriesColumns)
        setTimeSeriesValue(parameterMap["epochtime"],
                           ts::TimedValue(localtime, oracle.makeEpochTime(utctime)));
      else
        result->set(parameterMap["epochtime"], row, oracle.makeEpochTime(utctime));
    }
    if (isWantedParameter("origintime", parameterMap))
    {
      if (itsTimeSeriesColumns)
        setTimeSeriesValue(
            parameterMap["origintime"],
            ts::TimedValue(localtime,
                           oracle.timeFormatter->format(second_clock::universal_time())));
      else
        result->set(parameterMap["origintime"],
                    row,
                    oracle.timeFormatter->format(second_clock::universal_time()));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime FlashUtils::makeFlashTime(const otl_datetime& time,
                                                   const string& timezone,
                                                   const Fmi::TimeZones& timezones) const
{
  try
  {
    boost::posix_time::time_duration td(time.hour, time.minute, time.second, 0);
    boost::gregorian::date d(boost::numeric_cast<unsigned short>(time.year),
                             boost::numeric_cast<unsigned short>(time.month),
                             boost::numeric_cast<unsigned short>(time.day));
    boost::posix_time::ptime utctime(d, td);

    // Try given time zone
    if (!timezone.empty())
    {
      auto localtz = timezones.time_zone_from_string(timezone);
      local_date_time localtime(utctime, localtz);
      return localtime.local_time();
    }

    // Fall back to UTC time if no time zone info is found
    else
    {
      return utctime;
    }
  }
  catch (...)
  {
    boost::posix_time::ptime p;
    return p;
  }
}

void FlashUtils::printParams(map<string, int>& params)
{
  try
  {
    for (const auto& p : params)
    {
      cout << p.first << ": " << p.second << endl;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

#if 0
void formatRow(map<string, double> & resultRow, map<string, int> & wantedParameters)
{
  
}
#endif

map<string, int> FlashUtils::createParameterMap(vector<SmartMet::Spine::Parameter>& parameters)
{
  try
  {
    map<string, int> parameterMap;

    string lowerCaseParameter = "";

    int counter = 0;
    for (const SmartMet::Spine::Parameter& parameter : parameters)
    {
      lowerCaseParameter = parameter.name();
      Fmi::ascii_tolower(lowerCaseParameter);
      parameterMap[lowerCaseParameter] = counter++;
    }

    return parameterMap;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool FlashUtils::isWantedParameter(const string& key, const map<string, int>& parameterMap)
{
  try
  {
    if (parameterMap.find(key) != parameterMap.end())
    {
      return true;
    }
    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void FlashUtils::setTimeSeriesValue(unsigned int column, const ts::TimedValue& tv)
{
  try
  {
    while (itsTimeSeriesColumns->size() < column + 1)
      itsTimeSeriesColumns->push_back(ts::TimeSeries());
    itsTimeSeriesColumns->at(column).push_back(tv);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr FlashUtils::values(Oracle& oracle,
                                                                    Settings& settings,
                                                                    const Fmi::TimeZones& timezones)
{
  try
  {
    itsTimeSeriesColumns = ts::TimeSeriesVectorPtr(new ts::TimeSeriesVector);

    boost::shared_ptr<SmartMet::Spine::ValueFormatter> vf(
        new SmartMet::Spine::ValueFormatter(SmartMet::Spine::ValueFormatterParam()));
    getData(oracle, settings, vf, timezones);

    return itsTimeSeriesColumns;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
