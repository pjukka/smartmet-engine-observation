#include "Oracle.h"
#include "Utils.h"

#include <spine/Convenience.h>
#include <spine/Table.h>
#include <spine/Exception.h>

#include <locus/QueryOptions.h>

#include <macgyver/Astronomy.h>
#include <macgyver/Cache.h>
#include <macgyver/String.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <boost/thread.hpp>

#include <iostream>
#include <vector>
#include <string>

//#define MYDEBUG 1

namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

using Fmi::Cache::Cache;

/*
 * STATIC CACHE AREA
 * Each cache object is thread safe.
 */

// metadata forever
static auto& globalStationByIdCache =
    *new Cache<string, SmartMet::Spine::Stations>(50000);  // stations.csv has 40000 rows
// static auto & globalStationCache = * new Cache<string,SmartMet::Spine::Stations>(10000);
static auto& globalStationsByBoundingBoxCache = *new Cache<string, SmartMet::Spine::Stations>(1000);
static auto& globalStationsByLatLonCache = *new Cache<string, SmartMet::Spine::Stations>(100000);
static auto& globalWMOToLPNNCache = *new Cache<int, int>(10000);
static auto& globalIdToLPNNCache = *new Cache<int, int>(10000);
static auto& globalIdToWMOCache = *new Cache<int, int>(10000);
static auto& globalIdToRWSIDCache = *new Cache<int, int>(10000);
static auto& globalWMOToFMISIDCache = *new Cache<int, int>(10000);
static auto& globalRWSIDToFMISIDCache = *new Cache<int, int>(10000);
static auto& globalLPNNToFMISIDCache = *new Cache<int, int>(10000);
static auto& globalStationCoordinatesCache = *new Cache<int, std::pair<double, double> >(50000);

// observations expire in a number of seconds

#if 0
typedef Cache<string,boost::shared_ptr<SmartMet::Spine::Table>,Fmi::Cache::LRUEviction,string,Fmi::Cache::InstantExpire> ObsCache;
static auto & globalQCObservationsCache = * new ObsCache(10000,60);
static auto & globalHourlyFMIObservationsCache = * new ObsCache(10000,60);
static auto & globalFMIObservationsCache = * new ObsCache(10000,60);
static auto & globalSoundingsCache = * new ObsCache(1000,60);
static auto & globalSolarObservationsCache = * new ObsCache(1000,60);
static auto & globalMinuteRadiationObservationsCache = * new ObsCache(1000,60);
static auto & globalDailyAndMonthlyObservationsCache = * new ObsCache(1000,60);
#endif

namespace
{
// Cache utility for expiring data

template <class T, class U, class V>
void insert_and_expire(T& theCache, const U& theKey, const V& theValue)
{
  theCache.insert(theKey, theValue, theKey);
  theCache.expire(theKey);
}

// Cache key generation utilities

template <typename A, typename B>
string cache_key(A a, B b)
{
  return (Fmi::to_string(a) + '|' + Fmi::to_string(b));
}

template <typename A, typename B>
string cache_key(A a, B b, const std::string& c)
{
  return (Fmi::to_string(a) + '|' + Fmi::to_string(b) + '|' + c);
}

template <typename A, typename B, typename C>
string cache_key(A a, B b, C c)
{
  return (Fmi::to_string(a) + '|' + Fmi::to_string(b) + '|' + Fmi::to_string(c));
}

#if 0
  string cache_key(const vector<int> & values)
  {
	string key = "";
	for(int value : values)
	  {
		if(!key.empty())
		  key += '|';
		key += Fmi::to_string(value);
	  }
	return key;
  }
#endif

template <typename List>
string cache_key(const List& objects)
{
  string key;
  for (const auto& ob : objects)
  {
    if (!key.empty())
      key += '|';
    key += ob.hash();
  }
  return key;
}

}  // namespace anonymous

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Oracle::Oracle(SmartMet::Engine::Geonames::Engine* geonames_,
               const string& service,
               const string& username,
               const string& password,
               const string& nls_lang,
               const boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter_)
    : valueFormatter(valueFormatter_),
      allPlaces(false),
      latest(false),
      geonames(geonames_),
      itsConnected(false),
      itsShutdownRequested(false),
      itsService(service),
      itsUsername(username),
      itsPassword(password),
      itsBoundingBoxIsGiven(false)
{
  try
  {
    // Oracle defaults its output character set to US7ASCII if NLS_LANG environmental variable is
    // not
    // set.
    // More specifically, the default value for NLS_LANG is AMERICAN_AMERICA.US7ASCII.
    // See http://www.oracle.com/technetwork/database/globalization/nls-lang-099431.html for more
    // information.
    // We want UTF8 encoded characters, so NLS_LANG must be set here with putenv.

    putenv(const_cast<char*>(nls_lang.c_str()));

    // Connection string is in form "username/password@service"

    string connection = username + "/" + password + "@" + service;

    try
    {
      thedb.otl_initialize(1);
      thedb.rlogon(connection.c_str());
      thedb.logoff();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      cerr << p.msg << endl;       // print out error message
      cerr << p.stm_text << endl;  // print out SQL that caused the error
      cerr << p.var_info << endl;  // print out the variable that caused the error
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Oracle::~Oracle()
{
  try
  {
    thedb.logoff();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Oracle::isConnected()
{
  try
  {
    try
    {
      // The SQL statement return 1 column with one value: "X".
      otl_stream s(1, "select * from dual", thedb);

      int streamVariables = 0;
      s.describe_out_vars(streamVariables);
      s.close();

      if (streamVariables == 0)
        return false;
    }
    catch (const otl_exception& e)
    {
      std::ostringstream msg;
      msg << boost::posix_time::second_clock::local_time()
          << ": Observation::Oracle connection id='" << connectionId()
          << "' is not connected!  - otl_exception: " << e.msg << "\n";
      std::cerr << msg.str();
      return false;
    }

    return true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void Oracle::shutdown()
{
  std::cout << "  -- Shutdown requested (Oracle)\n";
  itsShutdownRequested = true;
}

bool Oracle::isFatalError(int code)
{
  if (code == 3135 ||  // ORA-03135: connection lost contact
      code == 3113 ||  // ORA-03113: end-of-file on communication channel
      code == 3114)    // ORA-03114: not connected to ORACLE
  {
    return true;
  }
  return false;
}

void Oracle::reConnect()
{
  try
  {
    string connection = itsUsername + "/" + itsPassword + "@" + itsService;
    try
    {
      thedb.logoff();
      thedb.rlogon(connection.c_str());
      thedb.logoff();
      attach();
      beginSession();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      cerr << p.msg << endl;       // print out error message
      cerr << p.stm_text << endl;  // print out SQL that caused the error
      cerr << p.var_info << endl;  // print out the variable that caused the error
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::beginSession()
{
  try
  {
    if (!itsConnected)
      throw SmartMet::Spine::Exception(BCP, "Cannot begin session before connected");

    try
    {
      thedb.session_begin(itsUsername.c_str(), itsPassword.c_str());  // 0 --> auto commit off
    }
    catch (otl_exception& p)
    {
      cerr << "Unable to begin session as user " << itsUsername << ":" << endl;
      cerr << "Code: " << p.code << endl;
      cerr << p.msg << endl << p.var_info << endl;

      attach();
      beginSession();
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::endSession()
{
  try
  {
    cout << "endSession()" << endl;
    if (!itsConnected)
      throw SmartMet::Spine::Exception(BCP, "Cannot end session if not connected");

    try
    {
      thedb.rollback();
      thedb.session_end();
    }
    catch (otl_exception& p)
    {
      cerr << "Unable to end session" << endl;
      cerr << "Code: " << p.code << endl;
      cerr << p.msg << endl;  // print out error message
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::attach()
{
  try
  {
    try
    {
      thedb.server_attach(itsService.c_str());
      itsConnected = true;
    }
    catch (otl_exception& p)
    {
      cerr << "Unable to attach to database: " << endl;
      cerr << "Code: " << p.code << endl;
      cerr << p.msg << endl;  // print out error message
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::detach()
{
  try
  {
    try
    {
      thedb.server_detach();
      itsConnected = false;
    }
    catch (otl_exception& p)
    {
      cerr << p.msg;
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

otl_connect& Oracle::getConnection()
{
  return thedb;
}

void Oracle::get(const std::string& sqlStatement,
                 std::shared_ptr<QueryResultBase> qrb,
                 const Fmi::TimeZones& timezones)
{
  try
  {
    // Validity of input parameters are tested outside the method.
    try
    {
      int numberOfVariables = static_cast<int>(qrb->size());

      if (thedb.connected != 1)
        reConnect();

      otl_stream s(1, sqlStatement.c_str(), thedb);

      int streamVariables = 0;
      const otl_var_desc* describtionOfOutVars = s.describe_out_vars(streamVariables);
      int columns = 0;
      const otl_column_desc* describtionOfselectColumn = s.describe_select(columns);

      if (numberOfVariables != columns)
      {
        std::ostringstream msg;
        msg << "Oracle::get  number of stream variables and columns does not match.";

        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }

      if ((numberOfVariables == 0) or (streamVariables != numberOfVariables))
      {
        std::ostringstream msg;
        msg << "Oracle::get Invalid container size '" << numberOfVariables << "' in '"
            << typeid(*qrb).name() << "' class.";

        SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
        // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
        exception.addDetail(msg.str());
        throw exception;
      }

      for (int i = 0; i < streamVariables; i++)
        qrb->setValueVectorName(i, std::string(describtionOfOutVars[i].name));

      // FIXME Initialize the value variables !!
      while (!s.eof())
      {
        for (int i = 0; i < streamVariables; i++)
        {
          int outVarTypeID = describtionOfOutVars[i].ftype;
          if (outVarTypeID == otl_var_char)
          {
            std::string stringValue;
            s >> stringValue;
            qrb->set(i, stringValue);
          }
          else if (outVarTypeID == otl_var_double)
          {
            // Oracle store integral values as doubles e.g. number(8,0)
            // where 8 is column precision and 0 is column scale.
            // So here we will convert the values with scale value 0 to integrals.
            // NOTE: It may also be usefull to take account the precision.
            if (describtionOfselectColumn[i].scale == 0)
            {
              int64_t longValue = 0;
              s >> longValue;
              qrb->set(i, longValue);
            }
            else
            {
              double doubleValue = 0.0;
              s >> doubleValue;
              qrb->set(i, doubleValue);
            }
          }
          else if (outVarTypeID == otl_var_float)
          {
            float floatValue = 0.0f;
            s >> floatValue;
            qrb->set(i, floatValue);
          }
          else if (outVarTypeID == otl_var_int)
          {
            int32_t intValue = 0;
            s >> intValue;
            qrb->set(i, intValue);
          }
          else if (outVarTypeID == otl_var_unsigned_int)
          {
            uint32_t uintValue = 0;
            s >> uintValue;
            qrb->set(i, uintValue);
          }
          else if (outVarTypeID == otl_var_short)
          {
            int16_t shortValue = 0;
            s >> shortValue;
            qrb->set(i, shortValue);
          }
          else if (outVarTypeID == otl_var_long_int)
          {
            int64_t longValue = 0;
            s >> longValue;
            qrb->set(i, longValue);
          }
          else if (outVarTypeID == otl_var_timestamp)
          {
            otl_datetime datetimeValue;
            s >> datetimeValue;
            boost::posix_time::ptime ptimeValue = makePosixTime(datetimeValue, timezones, "UTC");
            qrb->set(i, ptimeValue);
          }
          else if (outVarTypeID == otl_var_varchar_long)
          {
            std::string stringValue;
            s >> stringValue;
            qrb->set(i, stringValue);
          }
          else if (outVarTypeID == otl_var_raw_long)
          {
            std::string stringValue;
            s >> stringValue;
            qrb->set(i, stringValue);
          }
          else if (outVarTypeID == otl_var_clob)
          {
            std::string stringValue;
            s >> stringValue;
            qrb->set(i, stringValue);
          }
          else if (outVarTypeID == otl_var_blob)
          {
            otl_lob_stream blobValue;
            s >> blobValue;
            // FIXME!!
            boost::any anyValue;
            qrb->set(i, anyValue);
          }
          /* else if (outVarTypeID == otl_var_db2time)
          {
                  otl_datetime datetimeValue;
                  s >> datetimeValue;
                  // FIXME!!
  boost::posix_time::ptime ptimeValue; // = makePosixTime(datetimeValue, "Some
  keyword");
                  qrb->set(i,ptimeValue);
          }
          else if (outVarTypeID == otl_var_db2date)
          {
                  otl_datetime datetimeValue;
                  s >> datetimeValue;
                  // FIXME!!
  boost::posix_time::ptime ptimeValue; // = makePosixTime(datetimeValue, "Some
  keyword");
                  qrb->set(i,ptimeValue);
          }
          else if (outVarTypeID == otl_var_tz_timestamp)
          {
                  otl_datetime datetimeValue;
                  s >> datetimeValue;
                  // FIXME!!
  boost::posix_time::ptime ptimeValue; // = makePosixTime(datetimeValue, "Some
  keyword");
                  qrb->set(i,ptimeValue);
          }
          else if (outVarTypeID == otl_var_ltz_timestamp)
          {
                  otl_datetime datetimeValue;
                  s >> datetimeValue;
                  // FIXME!!
  boost::posix_time::ptime ptimeValue; // = makePosixTime(datetimeValue, "Some
  keyword");
                  qrb->set(i,ptimeValue);
                  } */
          else if (outVarTypeID == otl_var_bigint)
          {
            int64_t longValue = 0;
            s >> longValue;
            qrb->set(i, longValue);
          }
          else if (outVarTypeID == otl_var_raw)
          {
            std::string stringValue;
            s >> stringValue;
            qrb->set(i, stringValue);
          }
          /* else if (outVarTypeID == otl_ubigint)
          {
                  uint64_t ulongValue = 0;
                  s >> ulongValue;
                  qrb->set(i,ulongValue);
          } */
          else
          {
            std::ostringstream msg;
            msg << "Oracle::get - Unsupported data type '" << describtionOfOutVars[i].name << "' ("
                << outVarTypeID << ").";

            SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
            // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
            exception.addDetail(msg.str());
            throw exception;
          }
        }
      }

      s.close();
    }
    catch (const otl_exception& p)
    {
      // Error Log message
      std::ostringstream cerrMsg;

      std::ostringstream msg;
      if (p.code == 32036 or p.code == 03135 or p.code == 03114)
      {
        cerrMsg << "Oracle::get - " << p.msg << "\n";
        std::cerr << cerrMsg.str();

        msg << "There is no a connection to the database.";

        SmartMet::Spine::Exception exception(BCP, "Missing database connection!", NULL);
        // exception.setExceptionCode(Obs_EngineException::MISSING_DATABASE_CONNECTION);
        exception.addDetail(msg.str());
        throw exception;
      }
      else if (p.code == 32028)
      {
        cerrMsg << "Oracle::get - " << p.msg << "\n";
        std::cerr << cerrMsg.str();

        msg << "Unsupported column data type.";

        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }
      else if (p.code == 942)
      {
        cerrMsg << "Oracle::get - " << p.msg << "\n";
        std::cerr << cerrMsg.str();

        msg << "There is no enough privileges to access the data.";
        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }

      cerrMsg << "Oracle::get - " << p.msg << "\n";
      std::cerr << cerrMsg.str();

      msg << p.msg;

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);

      std::cerr << exception.getStackTrace();

      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::readLocationsFromOracle(vector<LocationItem>& locations,
                                     const Fmi::TimeZones& timezones)
{
  try
  {
    string locationQuery =
        "SELECT location_id, fmisid, country_id, location_start, location_end, longitude, "
        "latitude, "
        "x, y, elevation, time_zone_name, time_zone_abbrev ";
    locationQuery += "FROM locations ";
    locationQuery += "WHERE location_end > SYSDATE";
    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, locationQuery.c_str(), thedb);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      otl_datetime timestamp;

      while (iterator.next_row())
      {
        LocationItem item;
        iterator.get(1, item.location_id);
        iterator.get(2, item.fmisid);
        iterator.get(3, item.country_id);
        iterator.get(4, timestamp);
        item.location_start = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(5, timestamp);
        item.location_end = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(6, item.longitude);
        iterator.get(7, item.latitude);
        iterator.get(8, item.x);
        iterator.get(9, item.y);
        iterator.get(10, item.elevation);
        iterator.get(11, item.time_zone_name);
        iterator.get(12, item.time_zone_abbrev);

        locations.push_back(item);
      }

      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      cerr << "ERROR: " << endl;
      cerr << p.msg << endl;       // print out error message
      cerr << p.stm_text << endl;  // print out SQL that caused the error
      cerr << p.var_info << endl;  // print out the variable that caused the error
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::readCacheDataFromOracle(vector<DataItem>& cacheData,
                                     boost::posix_time::ptime lastTime,
                                     const Fmi::TimeZones& timezones)
{
  try
  {
    string dataQuery =
        "SELECT station_id, measurand_id, producer_id, measurand_no, data_time, data_value, "
        "data_quality ";
    dataQuery += "FROM observation_data_v1 ";
    dataQuery += "WHERE data_time BETWEEN :in_last_time<timestamp,in> AND sysdate ";
    dataQuery += "AND data_value IS NOT NULL";

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, dataQuery.c_str(), thedb);
      stream << makeOTLTime(lastTime);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      otl_datetime timestamp;
      while (iterator.next_row() && !itsShutdownRequested)
      {
        DataItem item;
        iterator.get(1, item.fmisid);
        iterator.get(2, item.measurand_id);
        iterator.get(3, item.producer_id);
        iterator.get(4, item.measurand_no);
        iterator.get(5, timestamp);
        item.data_time = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(6, item.data_value);
        iterator.get(7, item.data_quality);
        cacheData.push_back(item);
      }

      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        reConnect();
        return readCacheDataFromOracle(cacheData, lastTime, timezones);
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

void Oracle::readFlashCacheDataFromOracle(vector<FlashDataItem>& flashCacheData,
                                          boost::posix_time::ptime lastTime,
                                          const Fmi::TimeZones& timezones)
{
  try
  {
    std::string flashDataQuery =
        "SELECT CAST(stroke_time AS DATE) AS stroke_time, flash_id, multiplicity, peak_current, "
        "sensors, freedom_degree, ellipse_angle, ellipse_major, "
        "ellipse_minor, chi_square, rise_time, ptz_time, cloud_indicator, angle_indicator, "
        "signal_indicator, timing_indicator, stroke_status, "
        "data_source, created, modified_last, modified_by, "
        "flash.stroke_location.sdo_point.x AS longitude, "
        "flash.stroke_location.sdo_point.y AS latitude, "
        "TO_NUMBER(TO_CHAR(stroke_time, 'FF9')) AS stroke_time_fractions "
        "FROM flashdata flash "
        "WHERE stroke_time BETWEEN :in_last_time<timestamp,in> AND sysdate ";

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(10000, flashDataQuery.c_str(), thedb);
      stream << makeOTLTime(lastTime);

      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      while (iterator.next_row())
      {
        otl_datetime timestamp;

        FlashDataItem item;
        iterator.get(1, timestamp);
        item.stroke_time = makePrecisionTime(timestamp);
        // item.stroke_time_fraction = timestamp.fraction;
        iterator.get(2, item.flash_id);
        iterator.get(3, item.multiplicity);
        iterator.get(4, item.peak_current);
        iterator.get(5, item.sensors);
        iterator.get(6, item.freedom_degree);
        iterator.get(7, item.ellipse_angle);
        iterator.get(8, item.ellipse_major);
        iterator.get(9, item.ellipse_minor);
        iterator.get(10, item.chi_square);
        iterator.get(11, item.rise_time);
        iterator.get(12, item.ptz_time);
        iterator.get(13, item.cloud_indicator);
        iterator.get(14, item.angle_indicator);
        iterator.get(15, item.signal_indicator);
        iterator.get(16, item.timing_indicator);
        iterator.get(17, item.stroke_status);
        iterator.get(18, item.data_source);
        iterator.get(19, timestamp);
        item.created = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(20, timestamp);
        item.modified_last = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(21, item.modified_by);
        iterator.get(22, item.longitude);
        iterator.get(23, item.latitude);
        iterator.get(24, item.stroke_time_fraction);

        flashCacheData.push_back(item);
      }

      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      cerr << "ERROR: " << endl;
      cerr << p.msg << endl;       // print out error message
      cerr << p.stm_text << endl;  // print out SQL that caused the error
      cerr << p.var_info << endl;  // print out the variable that caused the error
      throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<string>(p.msg));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::readWeatherDataQCFromOracle(vector<WeatherDataQCItem>& cacheData,
                                         boost::posix_time::ptime lastTime,
                                         const Fmi::TimeZones& timezones)
{
  try
  {
    std::string dataQuery =
        "SELECT fmisid, obstime, parameter, sensor_no, value, flag "
        "FROM weather_data_qc "
        "WHERE obstime >= :in_last_time<timestamp,in> AND obstime <= sysdate "
        "AND value IS NOT NULL";

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, dataQuery.c_str(), thedb);
      stream << makeOTLTime(lastTime);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;

      iterator.attach(stream);

      otl_datetime timestamp;

      while (iterator.next_row() && !itsShutdownRequested)
      {
        WeatherDataQCItem item;
        iterator.get(1, item.fmisid);
        iterator.get(2, timestamp);
        item.obstime = makePosixTime(timestamp, timezones, "UTC");
        iterator.get(3, item.parameter);
        iterator.get(4, item.sensor_no);
        iterator.get(5, item.value);
        iterator.get(6, item.flag);
        cacheData.push_back(item);
      }

      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        reConnect();
        return readWeatherDataQCFromOracle(cacheData, lastTime, timezones);
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

/*
 *
*/
void Oracle::getAllStations(SmartMet::Spine::Stations& stations, const Fmi::TimeZones& timezones)
{
  try
  {
    size_t original_size = stations.size();

    try
    {
      int in_group_id = 0;
      if (this->stationType == "road")
        in_group_id = 30;
      else if (this->stationType == "fmi")
        in_group_id = 10;

      otl_stream stream(1,
                        "begin "
                        ":rc<refcur,out> := STATION_QP.get_station_list_rc(:in_group_id<int,in>); "
                        "end;",
                        thedb);

      stream.set_commit(0);
      // Give parameters to otl_stream and open a reference cursor stream for reading
      otl_refcur_stream refcur;
      stream << in_group_id;
      stream >> refcur;
      // Initialize iterator for the refcur stream and attach the stream to it
      otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(refcur);

      int desc_len;
      stream.describe_out_vars(desc_len);

      double station_id = 0;
      string station_formal_name = "";
      otl_datetime station_start;
      otl_datetime station_end;
      int membership_id = 0;

      string station_status = "";
      double station_status_id = 0;

      int network_id = 0;
      otl_datetime membership_start;
      otl_datetime membership_end;
      int member_id = 0;
      string membership_code = "";

      // Iterate the rows
      while (rs.next_row())
      {
        rs.get(1, station_id);
        rs.get(2, station_formal_name);
        rs.get(3, station_start);
        rs.get(4, station_end);
        rs.get(5, membership_id);
        rs.get(6, network_id);
        rs.get(7, membership_start);
        rs.get(8, membership_end);
        rs.get(9, member_id);
        rs.get(10, membership_code);
        rs.get(11, station_status_id);
        rs.get(12, station_status);

        // Put station info to the struct
        SmartMet::Spine::Station s;
        // Reset place identifier fields fields
        s.lpnn = -1;
        s.wmo = -1;
        s.rwsid = -1;
        s.geoid = -1;

        s.station_type = "road";
        s.station_id = station_id;
        s.station_formal_name = station_formal_name;
        s.station_start = makePosixTime(station_start, timezones);
        s.station_end = makePosixTime(station_end, timezones);
        s.station_status_id = station_status_id;
        // Take account only operative stations
        if (station_status_id == 20)
          stations.push_back(s);

#ifdef MYDEBUG
        cout << "station_id: " << station_id << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "name: " << station_formal_name << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "----------------" << endl;
#endif
      }
      // Detach the iterator from stream
      rs.detach();
      stream.close();
    }

    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        stations.resize(original_size);
        return getAllStations(stations, timezones);
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

void Oracle::getStationById(
    int id, SmartMet::Spine::Stations& stations, int wmo, int lpnn, const Fmi::TimeZones& timezones)
{
  try
  {
    // Use cache if possible
    string cachekey = cache_key(id, wmo, lpnn);
    auto cacheresult = globalStationByIdCache.find(cachekey);
    if (cacheresult)
    {
      for (const SmartMet::Spine::Station& station : *cacheresult)
        stations.push_back(station);
      return;
    }

    // Query for getting nearest stations for a search key
    otl_stream stream(1,
                      "begin "
                      ":rc<refcur,out> := STATION_QP.getStation_rc(:in_station_id<int,in>); "
                      "end;",
                      thedb);

    stream.set_commit(0);

    // Give parameters to otl_stream and open a reference cursor stream for reading
    otl_refcur_stream refcur;
    try
    {
      stream << id;
      stream >> refcur;
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getStationById(id, stations, wmo, lpnn, timezones);
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

    double station_id = 0;
    double access_policy_id = 0;
    double station_status_id = 0;
    string language_code = "";
    string station_formal_name = "";
    string station_system_name = "";
    string station_global_name = "";
    string station_global_code = "";
    double target_category = 0;
    string stationary = "";
    otl_datetime modified_last;
    string modified_by = "";

    // Initialize iterator for the refcur stream and attach the stream to it
    otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;
    rs.attach(refcur);

    int desc_len;
    stream.describe_out_vars(desc_len);

    SmartMet::Spine::Stations tmpstations;

    while (rs.next_row())
    {
      try
      {
        rs.get(1, station_id);
        rs.get(2, access_policy_id);
        rs.get(3, station_status_id);
        rs.get(4, language_code);
        rs.get(5, station_formal_name);
        rs.get(6, station_system_name);
        rs.get(7, station_global_name);
        rs.get(8, station_global_code);
        rs.get(15, target_category);
        rs.get(16, stationary);
        rs.get(17, modified_last);
        rs.get(18, modified_by);

        // Put station info to the struct
        SmartMet::Spine::Station s;
        // Reset place identifier fields
        s.lpnn = -1;
        s.wmo = -1;
        s.rwsid = -1;
        s.geoid = -1;

        s.station_id = station_id;
        s.access_policy_id = access_policy_id;
        s.station_status_id = station_status_id;
        s.language_code = language_code;
        s.station_formal_name = station_formal_name;
        s.station_system_name = station_system_name;
        s.station_global_name = station_global_name;
        s.station_global_code = station_global_code;
        // s.target_category = target_category;
        s.stationary = stationary;
        s.modified_last = makePosixTime(modified_last, timezones);

        // Add extra station ids if available
        if (lpnn > 0)
        {
          s.lpnn = lpnn;
        }
        if (wmo > 0)
        {
          s.wmo = wmo;
        }

        if (timeZone.length() > 0)
        {
          s.timezone = timeZone;
        }

        // Add station to station list
        tmpstations.push_back(s);

        // All data is handled, move to next row
        rs.next_row();

        if (refcur.is_null())
        {
#ifdef MYDEBUG
          cout << "NULL" << endl;
#endif
        }

#ifdef MYDEBUG
        cout << "station_id: " << station_id << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "name: " << station_formal_name << endl;
        cout << "target_category: " << target_category << endl;
        cout << "station_global_code: " << station_global_code << endl;
        cout << "language_code: " << language_code << endl;
        cout << "access_policy_id: " << access_policy_id << endl;
        cout << "----------------" << endl;
#endif
      }
      catch (otl_exception& p)  // intercept OTL exceptions
      {
        if (isFatalError(p.code))  // reconnect if fatal error is encountered
        {
          cerr << "ERROR: " << endl;
          cerr << p.msg << endl;       // print out error message
          cerr << p.stm_text << endl;  // print out SQL that caused the error
          cerr << p.var_info << endl;  // print out the variable that caused the error

          reConnect();
          return getStationById(id, stations, wmo, lpnn, timezones);
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

    // Detach the iterator from stream
    rs.detach();
    stream.close();

    // Cache the result

    if (!tmpstations.empty())
      globalStationByIdCache.insert(cachekey, tmpstations);

    for (const SmartMet::Spine::Station& station : tmpstations)
      stations.push_back(station);
    return;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::getStation(const string& searchkey,
                        SmartMet::Spine::Stations& stations,
                        const Fmi::TimeZones& timezones)
{
  try
  {
// Use cache if possible
#if 0
    string cachekey = searchkey + '|' + cache_key(lpnns);
    auto cacheresult = globalStationCache.find(cachekey);
    if(cacheresult)
    {
      for(const SmartMet::Spine::Station & station : *cacheresult)
      stations.push_back(station);
      return;
    }
#endif

    string stationtypelist = solveStationtypeList();

    string sql = "begin ";
    if (stationtypelist.empty())
    {
      sql +=
          ":rc<refcur,out> := STATION_QP_PUB.getStationsByGroupClasses_rc(in_group_class_id_list "
          "=> "
          "'81', in_group_code_list => 'STUKRAD,STUKAIR,RWSFIN'); end;";
    }
    else
    {
      sql +=
          ":rc<refcur,out> := STATION_QP_PUB.getStationForAnySearchKey5_rc(in_search_key => NULL ";
      sql += ", in_station_type_list => '" + stationtypelist + "' ";
      sql += ", in_station_group_list => NULL ";
      // sql += ", in_valid_start => NULL ";
      // sql += ", in_valid_end => NULL ";
      // sql += ", in_country_list => '246' ";
      sql += "); ";
      sql += "end;";
    }
    otl_stream stream;
    // Give parameters to otl_stream and open a reference cursor stream for reading
    otl_refcur_stream refcur;
    try
    {
      // Query for getting nearest stations for a search key
      stream.open(1, sql.c_str(), thedb);

      stream.set_commit(0);
      stream << stationtypelist;
      stream >> refcur;
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getStation(searchkey, stations, timezones);
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

    string station_type = "";
    double station_id = 0;
    double access_policy_id = 0;
    double station_status_id = 0;
    string language_code = "";
    string station_formal_name = "";
    string station_system_name = "";
    string station_global_name = "";
    string station_global_code = "";
    otl_datetime station_start;
    otl_datetime valid_from;
    otl_datetime station_end;
    otl_datetime valid_to;
    double target_category = 0;
    string stationary = "";
    string lpnn = "";
    string wmon = "";
    double longitude = 0;
    double latitude = 0;
    int locations = 0;
    otl_datetime modified_last;
    double modified_by = 0;
    string rgb = "";

    // Initialize iterator for the refcur stream and attach the stream to it
    otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;
    rs.attach(refcur);

    int desc_len;
    stream.describe_out_vars(desc_len);

    int i = 0;

    SmartMet::Spine::Stations tmpstations;

    // Iterate the rows
    while (rs.next_row())
    {
      try
      {
        station_type = "";
        station_id = 0.0;
        lpnn = "-1";
        wmon = "-1";
        longitude = 0.0;
        latitude = 0.0;

        rs.get(1, station_type);
        rs.get(2, station_id);
        rs.get(3, access_policy_id);
        rs.get(4, station_status_id);
        rs.get(5, language_code);
        rs.get(6, station_formal_name);
        rs.get(7, station_start);
        rs.get(8, valid_from);
        rs.get(9, valid_to);
        rs.get(10, station_end);
        rs.get(11, target_category);
        rs.get(12, stationary);
        if (!rs.is_null(13))
          rs.get(13, lpnn);
        if (!rs.is_null(14))
          rs.get(14, wmon);

        if (!rs.is_null(15))
          rs.get(15, longitude);
        if (!rs.is_null(16))
          rs.get(16, latitude);

        rs.get(17, locations);
        rs.get(18, modified_last);
        rs.get(19, modified_by);
        rs.get(20, rgb);

        // Put station info to the struct
        SmartMet::Spine::Station s;
        // Reset place identifier fields
        s.lpnn = Fmi::stoi(lpnn);
        s.wmo = (wmon.size() != 5 ? -1 : Fmi::stoi(wmon));
        s.rwsid = -1;
        s.geoid = -1;
        s.distance = "-1";
        s.stationDirection = -1;

        s.station_type = station_type;
        s.station_id = station_id;
        s.fmisid = boost::numeric_cast<int>(station_id);
        s.access_policy_id = access_policy_id;
        s.station_status_id = station_status_id;
        s.language_code = language_code;
        s.station_formal_name = station_formal_name;
        s.station_start = makePosixTime(station_start, timezones);
        s.station_end = makePosixTime(station_end, timezones);
        s.longitude_out = longitude;
        s.latitude_out = latitude;
        s.target_category = target_category;
        s.stationary = stationary;
        s.modified_last = makePosixTime(modified_last, timezones);

        i++;

        tmpstations.push_back(s);

        if (refcur.is_null())
        {
#ifdef MYDEBUG
          cout << "NULL" << endl;
#endif
        }
#ifdef MYDEBUG
        cout << "station_type: " << station_type << endl;
        cout << "station_id: " << station_id << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "name: " << station_formal_name << endl;
        cout << valid_from << " - ";
        cout << valid_to << endl;
        cout << "target_category: " << target_category << endl;
        cout << "----------------" << endl;

#endif
      }
      catch (otl_exception& p)  // intercept OTL exceptions
      {
        if (isFatalError(p.code))  // reconnect if fatal error is encountered
        {
          cerr << "ERROR: " << endl;
          cerr << p.msg << endl;       // print out error message
          cerr << p.stm_text << endl;  // print out SQL that caused the error
          cerr << p.var_info << endl;  // print out the variable that caused the error

          reConnect();
          return getStation(searchkey, stations, timezones);
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

    // Detach the iterator from stream
    rs.detach();
    stream.close();

// Cache the result
#if 0
    if(!tmpstations.empty())
    globalStationCache.insert(cachekey,tmpstations);
#endif

    for (const SmartMet::Spine::Station& station : tmpstations)
      stations.push_back(station);
    return;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// We will not cache the result of this query since the heavy tasks are already cached
void Oracle::getStationByGeoid(SmartMet::Spine::Stations& stations,
                               const int geoid,
                               const Fmi::TimeZones& timezones)
{
  try
  {
    // FmiNames library caches this search
    Locus::QueryOptions opts;
    opts.SetLanguage(this->language);
    opts.SetResultLimit(1);
    opts.SetCountries("");
    opts.SetFullCountrySearch(true);

    auto places = geonames->idSearch(opts, geoid);
    if (!places.empty())
    {
      for (const auto& place : places)
      {
        // This search is cached too
        SmartMet::Spine::Stations newstations = getStationsByLatLon(
            place->latitude, place->longitude, this->numberOfStations, timezones);
        if (!newstations.empty())
        {
          // Set geoid to be the actual requested geoid for the first station!
          newstations.front().geoid = geoid;
          // WHY....!!!?
          // stations.push_back(newstations.front());
          for (const SmartMet::Spine::Station& s : newstations)
          {
            stations.push_back(s);
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

// We cache the result just in case some bounding boxes are used repeatedly
SmartMet::Spine::Stations Oracle::getStationsByBoundingBox(const map<string, double>& boundingBox,
                                                           const Fmi::TimeZones& timezones)
{
  try
  {
    // Try the cache first

    string cachekey;
    for (const auto& value : boundingBox)
    {
      if (!cachekey.empty())
        cachekey += '|';
      cachekey += value.first;
      cachekey += ':';
      cachekey += Fmi::to_string(value.second);
    }

    auto cacheresult = globalStationsByBoundingBoxCache.find(cachekey);
    if (cacheresult)
      return *cacheresult;

    // Search the database

    otl_stream stream(1,
                      "begin "
                      ":rc<refcur,out> := "
                      "STATION_QP_pub.getStationsInsideBBox_rc(:in_min_longitude<double,in>, "
                      ":in_min_latitude<double,in>, :in_max_longitude<double,in>, "
                      ":in_max_latitude<double,in>, :in_station_type_list<char[30],in>); "
                      "end;",
                      thedb);
    stream.set_commit(0);

    string in_station_type = solveStationtypeList();

    // Give parameters to otl_stream and open a reference cursor stream for reading
    otl_refcur_stream refcur;
    try
    {
      stream << boundingBox.at("minx") << boundingBox.at("miny") << boundingBox.at("maxx")
             << boundingBox.at("maxy") << in_station_type;
      stream >> refcur;
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getStationsByBoundingBox(boundingBox, timezones);
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

    string station_type = "";
    double station_id = 0;
    double access_policy_id = 0;
    double station_status_id = 0;
    string language_code = "";
    string station_formal_name = "";
    string station_system_name = "";
    string station_global_name = "";
    string station_global_code = "";
    otl_datetime station_start;
    otl_datetime valid_from;
    otl_datetime valid_to;
    otl_datetime station_end;
    double target_category = 0;
    string stationary = "";
    int lpnn_out = -1;
    double station_elevation = 0;
    otl_datetime modified_last;
    double modified_by = 0;
    double latitude_out;
    double longitude_out;

    // Initialize iterator for the refcur stream and attach the stream to it
    otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;
    rs.attach(refcur);

    int desc_len;
    stream.describe_out_vars(desc_len);

    // The query fills Stations vector
    SmartMet::Spine::Stations stations;

    // Iterate the rows
    while (rs.next_row())
    {
      try
      {
        rs.get(1, station_type);
        rs.get(2, station_id);
        rs.get(3, access_policy_id);
        rs.get(4, station_status_id);
        rs.get(5, language_code);
        rs.get(6, station_formal_name);
        rs.get(7, station_system_name);
        rs.get(8, station_global_name);
        rs.get(9, station_global_code);
        rs.get(10, station_start);
        rs.get(11, valid_from);
        rs.get(12, valid_to);
        rs.get(13, station_end);
        rs.get(14, target_category);
        rs.get(15, stationary);
        rs.get(16, lpnn_out);
        rs.get(17, longitude_out);
        rs.get(18, latitude_out);
        rs.get(19, modified_last);
        rs.get(20, modified_by);

        // Put station info to the struct
        SmartMet::Spine::Station s;

        // Reset place identifier fields
        s.lpnn = -1;
        s.wmo = -1;
        s.rwsid = -1;
        s.geoid = -1;

        s.station_type = station_type;
        s.station_id = station_id;
        s.access_policy_id = access_policy_id;
        s.station_status_id = station_status_id;
        s.language_code = language_code;
        s.station_formal_name = station_formal_name;
        s.station_system_name = station_system_name;
        s.station_global_name = station_global_name;
        s.station_global_code = station_global_code;
        s.station_start = makePosixTime(station_start, timezones);
        s.station_end = makePosixTime(station_end, timezones);
        s.target_category = target_category;
        s.stationary = stationary;
        s.latitude_out = latitude_out;
        s.longitude_out = longitude_out;
        s.station_elevation = station_elevation;
        s.modified_last = makePosixTime(modified_last, timezones);
        s.modified_by = modified_by;

        // Add info from SmartMet::Spine::LocationPtr
        addInfoToStation(s, latitude_out, longitude_out);
        // Take account only operative stations
        if (station_status_id == 20)
        {
          stations.push_back(s);
        }
        // For foreign stations, the operative status is not updated,
        // thus we have to include all stations
        else if (this->stationType == "foreign" || this->stationType == "elering")
        {
          stations.push_back(s);
        }

#ifdef MYDEBUG
        cout << "station_type: " << station_type << endl;
        cout << "station_id: " << station_id << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "name: " << station_formal_name << endl;
        cout << station_start << " - ";
        cout << station_end << endl;
        cout << "target_category: " << target_category << endl;
        cout << "----------------" << endl;

#endif
      }

      catch (otl_exception& p)  // intercept OTL exceptions
      {
        if (isFatalError(p.code))  // reconnect if fatal error is encountered
        {
          cerr << "ERROR: " << endl;
          cerr << p.msg << endl;       // print out error message
          cerr << p.stm_text << endl;  // print out SQL that caused the error
          cerr << p.var_info << endl;  // print out the variable that caused the error

          reConnect();
          return getStationsByBoundingBox(boundingBox, timezones);
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

    // Detach the iterator from stream
    rs.detach();
    stream.close();

    // Cache the result

    if (!stations.empty())
      globalStationsByBoundingBoxCache.insert(cachekey, stations);

    return stations;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations Oracle::getStationsByLatLon(const double lat,
                                                      const double lon,
                                                      const int numberofstations,
                                                      const Fmi::TimeZones& timezones)
{
  try
  {
    // Search the cache first

    string cachekey = cache_key(lat,
                                lon,
                                this->stationType + "|" + Fmi::to_string(numberofstations) + "|" +
                                    Fmi::to_string(this->maxDistance));
    auto cacheresult = globalStationsByLatLonCache.find(cachekey);
    if (cacheresult)
      return *cacheresult;

    // Query for getting nearest stations for point
    otl_stream stream(
        1,
        "begin "
        ":rc<refcur,out> := STATION_QP_PUB.getNearestStationsForPoint2_rc(:in_latitude<double,in>, "
        ":in_longitude<double,in>, :in_station_type<int,in>, :in_valid_date<timestamp,in>, "
        ":in_max_distance<double,in>, :in_max_rownum<int,in>); "
        "end;",
        thedb);
    stream.set_commit(0);

    int in_station_type = 0;
    in_station_type = solveStationtype();

    boost::posix_time::ptime now(second_clock::universal_time());
    otl_datetime in_valid_date = makeOTLTime(now);

    // Give parameters to otl_stream and open a reference cursor stream for reading
    otl_refcur_stream refcur;
    try
    {
      stream << lat << lon << in_station_type << in_valid_date << this->maxDistance
             << numberofstations;
      stream >> refcur;
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getStationsByLatLon(lat, lon, numberofstations, timezones);
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

    string station_type = "";
    double distance = 0;
    double station_id = 0;
    double access_policy_id = 0;
    double station_status_id = 0;
    string language_code = "";
    string station_formal_name = "";
    string station_system_name = "";
    string station_global_name = "";
    string station_global_code = "";
    otl_datetime station_start;
    otl_datetime station_end;
    double target_category = 0;
    string stationary = "";
    double latitude_out = 0;
    double longitude_out = 0;
    double station_elevation = 0;
    otl_datetime modified_last;
    double modified_by = 0;

    // Initialize iterator for the refcur stream and attach the stream to it
    otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;
    rs.attach(refcur);

    int desc_len;
    stream.describe_out_vars(desc_len);

    // The query fills Stations vector
    SmartMet::Spine::Stations stations;

    // Iterate the rows
    while (rs.next_row())
    {
      try
      {
        rs.get(1, station_type);
        rs.get(2, distance);
        rs.get(3, station_id);
        rs.get(4, access_policy_id);
        rs.get(5, station_status_id);
        rs.get(6, language_code);
        rs.get(7, station_formal_name);
        rs.get(8, station_system_name);
        rs.get(9, station_global_name);
        rs.get(10, station_global_code);
        rs.get(11, station_start);
        rs.get(12, station_end);
        rs.get(13, target_category);
        rs.get(14, stationary);
        rs.get(15, longitude_out);
        rs.get(16, latitude_out);
        rs.get(17, station_elevation);
        rs.get(18, modified_last);
        rs.get(19, modified_by);

        // Put station info to the struct
        SmartMet::Spine::Station s;

        // Reset place identifier fields
        s.lpnn = -1;
        s.wmo = -1;
        s.rwsid = -1;
        s.geoid = -1;

        s.station_type = station_type;
        s.distance = Fmi::to_string(distance);
        s.station_id = station_id;
        s.access_policy_id = access_policy_id;
        s.station_status_id = station_status_id;
        s.language_code = language_code;
        s.station_formal_name = station_formal_name;
        s.station_system_name = station_system_name;
        s.station_global_name = station_global_name;
        s.station_global_code = station_global_code;
        s.station_start = makePosixTime(station_start, timezones);
        s.station_end = makePosixTime(station_end, timezones);
        s.target_category = target_category;
        s.stationary = stationary;
        s.latitude_out = latitude_out;
        s.longitude_out = longitude_out;
        s.station_elevation = station_elevation;
        s.modified_last = makePosixTime(modified_last, timezones);
        s.modified_by = modified_by;

        // Add info from SmartMet::Spine::LocationPtr
        addInfoToStation(s, latitude_out, longitude_out);

        // Take account only operative stations
        if (station_status_id == 20)
        {
          stations.push_back(s);
        }
        // For certain station types, the operative status is not updated,
        // thus we have to include all stations
        else if (this->stationType == "foreign" || this->stationType == "elering" ||
                 this->stationType == "research" || this->stationType == "syke")
        {
          stations.push_back(s);
        }

        if (refcur.is_null())
        {
#ifdef MYDEBUG
          cout << "NULL" << endl;
#endif
        }
#ifdef MYDEBUG
        cout << "station_type: " << station_type << endl;
        cout << "station_id: " << station_id << endl;
        cout << "station_status_id: " << station_status_id << endl;
        cout << "name: " << station_formal_name << endl;
        cout << station_start << " - ";
        cout << station_end << endl;
        cout << "target_category: " << target_category << endl;
        cout << "----------------" << endl;

#endif
      }
      catch (otl_exception& p)  // intercept OTL exceptions
      {
        if (isFatalError(p.code))  // reconnect if fatal error is encountered
        {
          cerr << "ERROR: " << endl;
          cerr << p.msg << endl;       // print out error message
          cerr << p.stm_text << endl;  // print out SQL that caused the error
          cerr << p.var_info << endl;  // print out the variable that caused the error

          reConnect();
          return getStationsByLatLon(lat, lon, numberofstations, timezones);
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

    // Detach the iterator from stream
    rs.detach();
    stream.close();

    // Cache the result

    if (!stations.empty())
      globalStationsByLatLonCache.insert(cachekey, stations);

    return stations;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 *
*/
void Oracle::getNearestStationsForPoint(const SmartMet::Spine::LocationPtr& location,
                                        SmartMet::Spine::Stations& stations,
                                        int numberofstations,
                                        const Fmi::TimeZones& timezones)
{
  try
  {
    SmartMet::Spine::Stations newStations =
        getStationsByLatLon(location->latitude, location->longitude, numberofstations, timezones);
    for (const SmartMet::Spine::Station& s : newStations)
    {
      stations.push_back(s);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getWeatherDataQCObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalQCObservationsCache.find(cachekey);
    if(cacheresult)
    {
      return *cacheresult;
    }
#endif

    // Read from database

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

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
    makeParamIndexesForWeatherDataQC(
        params, paramindex, specialsindex, queryparams, queryparamindex);
    // Add wind direction always to calculate windcompass, even a fake one
    if (this->stationType == "road" || this->stationType == "mareograph" ||
        this->stationType == "buoy")
    {
      firstquery =
          "max(case when wd.parameter='TSUUNT' then wd.value end) as meta_wind_direction, ";
    }
    else if (this->stationType == "foreign")
    {
      firstquery = "max(case when wd.parameter='WD' then wd.value end) as meta_wind_direction, ";
    }
    else if (this->stationType == "elering")
    {
      firstquery =
          "max(case when wd.parameter='WD_MEAN_1H' then wd.value end) as meta_wind_direction, ";
    }

    otl_stream query;
    try
    {
      string qs = "";
      // If only latest observations are wanted, we need two additional subqueries
      if (latest)
      {
        qs += "select fmisid,ot,lat,lon,elevation,";
        pair<string, int> p;
        string qp = "";
        if (this->stationType == "road" || this->stationType == "foreign" ||
            this->stationType == "elering" || this->stationType == "mareograph" ||
            this->stationType == "buoy")
        {
          qp = "meta_wind_direction, ";
        }
        for (const SmartMet::Spine::Parameter& param : params)
        {
          if (not_special(param))
          {
            qp += param.name() + ",";
          }
        }
        qp = qp.substr(0, qp.length() - 1);
        qs += qp + " ";

        qs +=
            "from ("
            "select "
            "max(ot)over(partition by fmisid) max_ot, "
            "fmisid,ot,lat,lon,elevation,";
        qs += qp;
        qs += " from (";
      }

      // The basic case, where all observations between two timestamps are wanted
      else
      {
        qs += "select * from (";
      }

      qs +=
          "SELECT "
          "wd.fmisid as fmisid,"
          "wd.obstime as ot,"
          "loc.latitude as lat, "
          "loc.longitude as lon, "
          "loc.elevation as elevation, ";

      string qp = "";
      pair<string, int> p;
      // Add always the metaparameters, even for mareographs and buoys
      if (this->stationType == "road" || this->stationType == "mareograph" ||
          this->stationType == "buoy")
      {
        queryparams += "'TSUUNT',";
        string fullname =
            "max(case when wd.parameter='TSUUNT' and wd.sensor_no=1 then wd.value end) as "
            "meta_wind_direction,";
        qp += fullname;
      }
      else if (this->stationType == "foreign")
      {
        queryparams += "'WD',";
        string fullname =
            "max(case when wd.parameter='WD' then wd.value end) as meta_wind_direction,";
        qp += fullname;
      }
      else if (this->stationType == "elering")
      {
        queryparams += "'WD_MEAN_1H',";
        string fullname =
            "max(case when wd.parameter='WD_MEAN_1H' then wd.value end) as meta_wind_direction,";
        qp += fullname;
      }
      queryparams = queryparams.substr(0, queryparams.length() - 1);

      for (const auto& param : queryparamindex)
      {
        qp += param.first + ",";
      }
      qp = qp.substr(0, qp.length() - 1);
      qs += qp + " ";
      qs +=
          "FROM "
          "weather_data_qc wd "
          "join locations loc on(loc.fmisid = wd.fmisid) "
          "WHERE ";
      if (latest)
        qs += "wd.obstime >= :in_starttime<timestamp,in> ";
      else
        qs += "wd.obstime BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";
      // Use only given timeStep. Timestep is given in minutes
      if (this->timeStep != 0)
      {
        qs +=
            "and mod(60*to_number(to_char(wd.obstime, 'hh24'))+to_number(to_char(wd.obstime, "
            "'mi')), " +
            Fmi::to_string(this->timeStep) + ") = 0 ";
      }

      qs +=
          "AND "
          "wd.fmisid in (" +
          qstations +
          ") "
          "AND "
          "sysdate BETWEEN loc.location_start and loc.location_end "
          "AND "
          "upper(parameter) IN (" +
          queryparams +
          ") "
          "group by wd.fmisid, wd.obstime, loc.latitude, loc.longitude, loc.elevation"
          ") "
          "order by fmisid asc,ot asc";
      if (latest)
      {
        qs += ") where ot in (max_ot)";
      }

#ifdef MYDEBUG
      cout << qs << endl;
#endif

      query.open(1, qs.c_str(), thedb);
      query.set_commit(0);

      otl_datetime timestamp;

      if (latest)
        query << this->startTime;
      else
        query << this->startTime << this->endTime;  // << << this->timeStep;

      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;

      rs.attach(query);
      int row = 0;
      int id = 0;
      double winddirection = -1;
      int level = 0;
      double lat = 0;
      double lon = 0;
      double elevation = 0;
      int metacount = 6;
      int sensor_no = 0;  // fake

      int oldId = 0;
      while (rs.next_row())
      {
        // First fetch the meta parameters.
        rs.get(1, id);
        rs.get(2, timestamp);
        // Location info can be null in database, so we'll have to check it
        if (rs.is_null(3))
          lat = 0;
        else
          rs.get(3, lat);

        if (rs.is_null(4))
          lon = 0;
        else
          rs.get(4, lon);

        if (rs.is_null(5))
        {
          rs.get(5, elevation);
          elevation = 0;
        }
        else
        {
          rs.get(5, elevation);
        }
        if (!rs.is_null(6))
          rs.get(6, winddirection);
        else
        {
          rs.get(6, winddirection);
          winddirection = -1;
        }

        // Set station's latitude and longitude
        stationindex[id].latitude_out = lat;
        stationindex[id].longitude_out = lon;
        stationindex[id].station_elevation = elevation;

        if (oldId != id)
        {
          addInfoToStation(stationindex[id], lat, lon);
          oldId = id;
        }

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);

        winddirection = -1;
      }
      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getWeatherDataQCObservations(params, stations, timezones);
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

#if 0
    insert_and_expire(globalQCObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getHourlyFMIObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
    // Check for empty stations vector
    if (stations.empty())
    {
      boost::shared_ptr<SmartMet::Spine::Table> empty(new SmartMet::Spine::Table);
      return empty;
    }

// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalHourlyFMIObservationsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // Read from database

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    otl_stream query;
    try
    {
      string qs =
          "SELECT "
          "lpnn as l,obstime as ot, hw.wd_avg as wind_direction_meta, ";
      string qp = "";
      for (const auto& p : paramindex)
      {
        qp += p.first + ",";
      }
      qp = qp.substr(0, qp.length() - 1);

      qs += qp + " ";
      qs +=
          " FROM "
          "hourly_weather_qc hw "
          "WHERE "
          "hw.lpnn in (" +
          qstations +
          ") "
          "AND "
          "hw.obstime BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";
      if (this->timeStep != 0)
      {
        qs +=
            "and mod(60*to_number(to_char(hw.obstime, 'hh24'))+to_number(to_char(hw.obstime, "
            "'mi')), " +
            Fmi::to_string(this->timeStep) + ") = 0 ";
      }
      qs += "order by l,ot";

#ifdef MYDEBUG
      cout << qs << endl;
#endif
      query.open(1, qs.c_str(), thedb);

      query.set_commit(0);

      otl_datetime timestamp;

      query << this->startTime << this->endTime;

      int row = 0;        // row counter
      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      int level = 0;
      double winddirection = -1;
      int metacount = 3;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);
      while (rs.next_row())
      {
        rs.get(1, id);
        rs.get(2, timestamp);
        if (!rs.is_null(3))
          rs.get(3, winddirection);

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);

        winddirection = -1;
      }

      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getHourlyFMIObservations(params, stations, timezones);
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

#if 0
    insert_and_expire(globalHourlyFMIObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getFMIObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
    // Check for empty stations vector
    if (stations.empty())
    {
      boost::shared_ptr<SmartMet::Spine::Table> empty(new SmartMet::Spine::Table);
      return empty;
    }

// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalFMIObservationsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // NOTICE: qc tables use lpnn number as station identifier.

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    // Short names for rain
    string qp = "";
    bool useRain = false;
    for (const auto& s : paramindex)
    {
      if (s.first == "R_1H" || s.first == "R_12H" || s.first == "R_24H")
      {
        useRain = true;
      }
      qp += s.first + ",";
    }

    qp = qp.substr(0, qp.length() - 1);
    // Form the query
    otl_stream query;

    try
    {
      string queryString = "";
      // If only latest observations are wanted, we need two additional subqueries
      if (latest)
      {
        queryString +=
            "select "
            "lpnn,ot,stationlat,stationlon,meta_wind_direction,meta_ws,meta_t,meta_rh,distance";

        if (qp.size() > 0)
        {
          queryString += "," + qp + " ";
        }

        queryString +=
            "from ("
            "select "
            "max(ot)over(partition by lpnn) max_ot, "
            "lpnn,ot,stationlat,stationlon,distance,meta_wind_direction, meta_ws,meta_t,meta_rh,";
        if (useRain)
        {
          replaceRainParameters(qp);
        }
        queryString = trimCommasFromEnd(queryString);
        if (qp.size() > 0)
        {
          queryString += "," + qp + " ";
        }

        queryString += " from (";
      }

      // The basic case, where all observations between two timestamps are wanted
      else
      {
        queryString +=
            "select "
            "lpnn,ot,stationlat,stationlon,meta_wind_direction,meta_ws,meta_t,meta_rh,distance";
        if (useRain)
        {
          replaceRainParameters(qp);
        }
        queryString = trimCommasFromEnd(queryString);
        if (qp.size() > 0)
        {
          queryString += "," + qp + " ";
        }

        queryString += " from (";
      }

      queryString += "SELECT ";
      if (this->latest)
      {
        queryString += "w.lpnn, w.obstime as ot, ";
      }
      else
      {
        queryString += "a.lpnns as lpnn, a.intervals as ot, ";
      }
      queryString +=
          "TRUNC(s.lat/100)+(60*100*(s.lat/100-TRUNC(s.lat/100))+coalesce(s.lat_sec,0))/3600 AS "
          "stationlat, "
          "TRUNC(s.lon/100)+(60*100*(s.lon/100-TRUNC(s.lon/100))+coalesce(s.lon_sec,0))/3600 AS "
          "stationlon, "
          "w.wd_10min as meta_wind_direction, w.ws_10min as meta_ws, w.t as meta_t, w.rh as "
          "meta_rh,";

      // The following is needed when sorting the data by station's distance from selected point
      queryString += getDistanceSql(stations);

      // Then add other queried parameters
      queryString += queryparams;
      queryString = trimCommasFromEnd(queryString);
      queryString += " FROM ";

      if (!this->latest)
      {
        queryString += getIntervalSql(qstations, timezones);
        queryString +=
            "left outer join weather_qc w on (a.lpnns = w.lpnn and a.intervals = w. obstime) \n ";

        queryString +=
            "left outer join prec_int_qc shortprec "
            "on (w.lpnn = shortprec.lpnn and w.obstime = shortprec.obstime and shortprec.gauge = "
            "50) "
            "\n"
            "left outer join prec_qc longprec_weigh_50 "
            "on (w.lpnn = longprec_weigh_50.lpnn and w.obstime = longprec_weigh_50.obstime and "
            "longprec_weigh_50.gauge = 50) \n"
            "left outer join prec_qc longprec_weigh_60 "
            "on (w.lpnn = longprec_weigh_60.lpnn and w.obstime = longprec_weigh_60.obstime and "
            "longprec_weigh_60.gauge = 60) \n"
            "left outer join prec_qc longprec_manual "
            "on (w.lpnn = longprec_manual.lpnn and w.obstime = longprec_manual.obstime and "
            "longprec_manual.gauge = 10) \n"
            "left outer join hourly_weather_qc hourly "
            "on (w.lpnn = hourly.lpnn and w.obstime = hourly.obstime) \n"
            /* This is very slow, maybe indexes are not ok in daily_qc?
               "left outer join daily_qc daily "
               "on (w.lpnn = daily.lpnn and w.obstime = daily.dayx) "
            */
            "left outer join sreg s on (a.lpnns = s.lpnn)\n";
      }
      else
      {
        queryString += " weather_qc w \n ";

        queryString +=
            "left outer join prec_int_qc shortprec "
            "on (w.lpnn = shortprec.lpnn and w.obstime = shortprec.obstime and shortprec.gauge = "
            "50) "
            "\n"
            "left outer join prec_qc longprec_weigh_50 "
            "on (w.lpnn = longprec_weigh_50.lpnn and w.obstime = longprec_weigh_50.obstime and "
            "longprec_weigh_50.gauge = 50) \n"
            "left outer join prec_qc longprec_weigh_60 "
            "on (w.lpnn = longprec_weigh_60.lpnn and w.obstime = longprec_weigh_60.obstime and "
            "longprec_weigh_60.gauge = 60) \n"
            "left outer join prec_qc longprec_manual "
            "on (w.lpnn = longprec_manual.lpnn and w.obstime = longprec_manual.obstime and "
            "longprec_manual.gauge = 10) \n"
            "left outer join hourly_weather_qc hourly "
            "on (w.lpnn = hourly.lpnn and w.obstime = hourly.obstime) \n"
            /* This is very slow, maybe indexes are not ok in daily_qc?
               "left outer join daily_qc daily "
               "on (w.lpnn = daily.lpnn and w.obstime = daily.dayx) "
            */
            "join sreg s on (w.lpnn = s.lpnn and w.lpnn in (" +
            qstations + ")) \n";
        queryString += "where w.obstime >= :in_starttime<timestamp,in> ";
      }

      queryString += ") order by distance,lpnn,ot";

      if (latest)
      {
        queryString += ") where ot in (max_ot)";
      }

#ifdef MYDEBUG
      cout << queryString << endl;
#endif

      query.open(1, queryString.c_str(), thedb);

      query.set_commit(0);
      if (latest)
      {
        query << this->startTime;
      }

      otl_datetime timestamp;

      int row = 0;        // row counter
      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      int level = 0;
      double winddirection = -1;
      double windspeed = kFloatMissing;
      double temperature = kFloatMissing;
      double rh = kFloatMissing;
      double distance = -1;
      int metacount = 9;
      double lat = 0.0;
      double lon = 0.0;

      int oldId = 0;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);

      while (rs.next_row())
      {
        // Skip the row if there is no coordinate information in the result row
        if (rs.is_null(3) || rs.is_null(4))
          continue;

        rs.get(1, id);
        rs.get(2, timestamp);
        rs.get(3, lat);
        rs.get(4, lon);

        if (!rs.is_null(5))
          rs.get(5, winddirection);  // Insert wind direction to metaparameters for later use

        if (!rs.is_null(6))
          rs.get(6, windspeed);  // Insert wind speed to metaparameters for later use

        if (!rs.is_null(7))
          rs.get(7, temperature);  // Insert temperature to metaparameters for later use

        if (!rs.is_null(8))
          rs.get(8, rh);  // Insert relative humidity to metaparameters for later use

        rs.get(9, distance);

        // Set station's latitude and longitude
        stationindex[id].latitude_out = lat;
        stationindex[id].longitude_out = lon;

        if (oldId != id)
        {
          addInfoToStation(stationindex[id], lat, lon);
          oldId = id;
        }

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones,
                windspeed,
                temperature,
                rh);

        winddirection = -1;
        windspeed = kFloatMissing;
        temperature = kFloatMissing;
        rh = kFloatMissing;
      }

      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getFMIObservations(params, stations, timezones);
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

#if 0
    insert_and_expire(globalFMIObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getSoundings(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalSoundingsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // Read the database

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    // Form the query
    otl_stream query;
    try
    {
      // Date is separated to two columns in database, dayx and hour
      string qs =
          "SELECT "
          "luotaus.lpnn as l,(luotaus.dayx+hour/3600/24) as dayx, ldat.no, ";
      for (const SmartMet::Spine::Parameter& p : params)
      {
        if (not_special(p))
        {
          string translated = translateParameter(p.name());
          qs += translated + " as " + translated + ",";
        }
      }
      qs = qs.substr(0, qs.length() - 1);
      qs +=
          " FROM "
          "ldat_2011 ldat, "
          "luotaus_2011 luotaus "
          "WHERE "
          "luotaus.lpnn in (" +
          qstations +
          ") "
          "AND "
          "luotaus.seq_no = ldat.seq_no "
          "AND "
          "luotaus.dayx BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";

#ifdef MYDEBUG
      cout << qs << endl;
#endif
      query.open(1, qs.c_str(), thedb);

      query.set_commit(0);

      otl_datetime timestamp;

      // Give time options to query
      if (latest)
      {
        query << this->startTime;
      }
      else
      {
        query << this->startTime << this->endTime;
      }

      int row = 0;        // row counter
      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      int level = 0;
      double winddirection = -1;

      int metacount = 3;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);
      while (rs.next_row())
      {
        rs.get(1, id);
        rs.get(2, timestamp);
        rs.get(3, level);

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);
      }

      rs.detach();
      query.close();
    }

    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getSoundings(params, stations, timezones);
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

#if 0
    insert_and_expire(globalSoundingsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getSolarObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
    // Check for empty stations vector
    if (stations.empty())
    {
      boost::shared_ptr<SmartMet::Spine::Table> empty(new SmartMet::Spine::Table);
      return empty;
    }

// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalSolarObservationsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // NOTICE: View hrad_qc uses lpnn number as station identifier.

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    // Form the query
    otl_stream query;
    try
    {
      string qs =
          "SELECT "
          "hr.lpnn as l, hr.obstime as ot, "
          "TRUNC(s.lat/100)+(60*100*(s.lat/100-TRUNC(s.lat/100))+coalesce(s.lat_sec,0))/3600 AS "
          "stationlat, "
          "TRUNC(s.lon/100)+(60*100*(s.lon/100-TRUNC(s.lon/100))+coalesce(s.lon_sec,0))/3600 AS "
          "stationlon, ";
      for (const SmartMet::Spine::Parameter& p : params)
      {
        if (not_special(p))
        {
          string translated = translateParameter(p.name());
          qs += translated + " as " + translated + ",";
        }
      }
      qs = qs.substr(0, qs.length() - 1);
      qs +=
          " FROM "
          "hrad_qc hr, sreg s "
          "WHERE "
          "hr.lpnn in (" +
          qstations +
          ") "
          "AND "
          "hr.lpnn = s.lpnn "
          "AND "
          "hr.obstime BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";
      if (this->timeStep != 0)
      {
        qs +=
            "and mod(60*to_number(to_char(hr.obstime, 'hh24'))+to_number(to_char(hr.obstime, "
            "'mi')), " +
            Fmi::to_string(this->timeStep) + ") = 0 ";
      }
      qs += "order by l, ot";
#ifdef MYDEBUG
      cout << qs << endl;
#endif
      query.open(1, qs.c_str(), thedb);

      query.set_commit(0);

      otl_datetime timestamp;

      // Give time options to query
      query << this->startTime << this->endTime;

      int row = 0;

      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      double winddirection = -1;
      double lat = 0;
      double lon = 0;
      int metacount = 4;

      int level = 0;

      int oldId = 0;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);
      while (rs.next_row())
      {
        rs.get(1, id);
        rs.get(2, timestamp);
        rs.get(3, lat);
        rs.get(4, lon);
        stationindex[id].latitude_out = lat;
        stationindex[id].longitude_out = lon;

        // Append info to station only once
        if (oldId != id)
        {
          addInfoToStation(stationindex[id], lat, lon);
          oldId = id;
        }

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);
      }

      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getSolarObservations(params, stations, timezones);
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

#if 0
    insert_and_expire(globalSolarObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getMinuteRadiationObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const Fmi::TimeZones& timezones)
{
  try
  {
    // Check for empty stations vector
    if (stations.empty())
    {
      boost::shared_ptr<SmartMet::Spine::Table> empty(new SmartMet::Spine::Table);
      return empty;
    }

// Try the cache first
#if 0
    string cachekey = cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalMinuteRadiationObservationsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // NOTICE: View minute_rad_qc uses lpnn number as station identifier.

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    // Form the query
    otl_stream query;
    try
    {
      string qs =
          "SELECT "
          "mr.lpnn as l, mr.obstime as ot, "
          "TRUNC(s.lat/100)+(60*100*(s.lat/100-TRUNC(s.lat/100))+coalesce(s.lat_sec,0))/3600 AS "
          "stationlat, "
          "TRUNC(s.lon/100)+(60*100*(s.lon/100-TRUNC(s.lon/100))+coalesce(s.lon_sec,0))/3600 AS "
          "stationlon, ";
      for (const SmartMet::Spine::Parameter& p : params)
      {
        if (not_special(p))
        {
          string translated = translateParameter(p.name());
          qs += translated + " as " + translated + ",";
        }
      }
      qs = qs.substr(0, qs.length() - 1);
      qs +=
          " FROM "
          "minute_rad_qc mr, sreg s "
          "WHERE "
          "mr.lpnn in (" +
          qstations +
          ") "
          "AND "
          "mr.lpnn = s.lpnn "
          "AND "
          "mr.obstime BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";
      if (this->timeStep != 0)
      {
        qs +=
            "and mod(60*to_number(to_char(mr.obstime, 'hh24'))+to_number(to_char(mr.obstime, "
            "'mi')), " +
            Fmi::to_string(this->timeStep) + ") = 0 ";
      }
      qs += "order by l,ot";
#ifdef MYDEBUG
      cout << qs << endl;
#endif
      query.open(1, qs.c_str(), thedb);

      query.set_commit(0);

      otl_datetime timestamp;

      // Give time options to query
      query << this->startTime << this->endTime;

      int row = 0;

      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      double winddirection = -1;
      double lat = 0;
      double lon = 0;
      int metacount = 4;

      int level = 0;

      int oldId = 0;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);
      while (rs.next_row())
      {
        rs.get(1, id);
        rs.get(2, timestamp);
        rs.get(3, lat);
        rs.get(4, lon);
        stationindex[id].latitude_out = lat;
        stationindex[id].longitude_out = lon;

        // Append info to station only once
        if (oldId != id)
        {
          addInfoToStation(stationindex[id], lat, lon);
          oldId = id;
        }

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);
      }

      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getMinuteRadiationObservations(params, stations, timezones);
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

#if 0
    insert_and_expire(globalMinuteRadiationObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// TODO: Cache the result for a short period of time

boost::shared_ptr<SmartMet::Spine::Table> Oracle::getDailyAndMonthlyObservations(
    const vector<SmartMet::Spine::Parameter>& params,
    const SmartMet::Spine::Stations& stations,
    const string& type,
    const Fmi::TimeZones& timezones)
{
  try
  {
    // Check for empty stations vector
    if (stations.empty())
    {
      boost::shared_ptr<SmartMet::Spine::Table> empty(new SmartMet::Spine::Table);
      return empty;
    }

// Try the cache first
#if 0
    string cachekey = type + '|' + cache_key(stations) + '|' + cache_key(params);
    auto cacheresult = globalDailyAndMonthlyObservationsCache.find(cachekey);
    if(cacheresult)
    return *cacheresult;
#endif

    // NOTICE: View not_missing_lammitystarve uses lpnn number as station identifier.
    string table = "";
    if (type == "lammitystarve")
    {
      table = "not_missing_lammitystarve daily";
    }
    else if (type == "daily")
    {
      table = "daily_qc daily";
    }
    else if (type == "monthly")
    {
      table = "mon_dat_qc daily";
    }

    boost::shared_ptr<SmartMet::Spine::Table> result(new SmartMet::Spine::Table);

    stringstream ss;
    map<double, SmartMet::Spine::Station> stationindex;
    for (const SmartMet::Spine::Station& s : stations)
    {
      stationindex.insert(std::make_pair(s.lpnn, s));
      ss << s.lpnn << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // Make parameter indexes and query parameters
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";
    makeParamIndexes(params, paramindex, specialsindex, queryparams);

    // Form the query
    otl_stream query;
    try
    {
      string qs =
          "SELECT "
          "daily.lpnn as l,dayx, "
          "TRUNC(s.lat/100)+(60*100*(s.lat/100-TRUNC(s.lat/100))+coalesce(s.lat_sec,0))/3600 AS "
          "stationlat, "
          "TRUNC(s.lon/100)+(60*100*(s.lon/100-TRUNC(s.lon/100))+coalesce(s.lon_sec,0))/3600 AS "
          "stationlon, ";
      for (const SmartMet::Spine::Parameter& p : params)
      {
        if (not_special(p))
        {
          string translated = translateParameter(p.name());
          qs += translated + " as " + translated + ",";
        }
      }
      qs = qs.substr(0, qs.length() - 1);
      qs += " FROM sreg s,";
      qs += table;
      qs +=
          " WHERE "
          "daily.lpnn in (" +
          qstations +
          ") "
          "AND "
          "daily.lpnn = s.lpnn "
          "AND "
          "daily.dayx BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> "
          "order by l,dayx";
#ifdef MYDEBUG
      cout << qs << endl;
#endif
      query.open(1, qs.c_str(), thedb);

      query.set_commit(0);

      otl_datetime timestamp;

      // Give time options to query
      query << this->startTime << this->endTime;

      int row = 0;        // row counter
      int id = 0;         // this is lpnn
      int sensor_no = 0;  // fake
      int level = 0;
      double lat = 0;
      double lon = 0;
      double winddirection = -1;
      int metacount = 4;

      int oldId = 0;

      // Use otl_stream_read_iterator to read resulting rows
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);
      while (rs.next_row())
      {
        rs.get(1, id);
        rs.get(2, timestamp);
        rs.get(3, lat);
        rs.get(4, lon);
        stationindex[id].latitude_out = lat;
        stationindex[id].longitude_out = lon;

        // Append info to station only once
        if (oldId != id)
        {
          addInfoToStation(stationindex[id], lat, lon);
          oldId = id;
        }

        makeRow(*result,
                metacount,
                paramindex,
                specialsindex,
                stationindex,
                timestamp,
                sensor_no,
                winddirection,
                level,
                id,
                row,
                rs,
                query,
                stationindex[id],
                timezones);
      }

      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getDailyAndMonthlyObservations(params, stations, type, timezones);
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

#if 0
    insert_and_expire(globalDailyAndMonthlyObservationsCache,cachekey,result);
#endif

    return result;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Utility methods

/* Translates parameter names to match the parameter name in the database.
 * If the name is not found in parameter map, return the given name.
 */

string Oracle::translateParameter(const string& paramname)
{
  try
  {
    // All parameters are in lower case in parametermap
    string p = Fmi::ascii_tolower_copy(paramname);
    if (!parameterMap[p][this->stationType].empty())
      return parameterMap[p][this->stationType];
    else
      return p;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::makeParamIndexes(const vector<SmartMet::Spine::Parameter>& params,
                              map<string, int>& paramindex,
                              map<int, string>& specialsindex,
                              string& queryparameters)
{
  try
  {
    // In order to get the parameters in same order as in the original query string,
    // we need a few additional data structures for keeping track of things
    int i = 0;
    list<string> queryParameterList;
    for (const SmartMet::Spine::Parameter& p : params)
    {
      if (not_special(p))
      {
        string oraclename;
        // Rain parameters are a special case, we must explicitly state
        // the table they are going to be get
        bool isRain = false;
        string paramName = "";
        string rainParam = p.name();
        Fmi::ascii_tolower(rainParam);
        if (rainParam == "r_1h" || rainParam == "precipitation1h")
        {
          isRain = true;
          oraclename += "longprec_weigh_50.r_1h as r1h50,longprec_weigh_60.r_1h as r1h60,";
          paramName = "r_1h";
        }
        else if (rainParam == "r_12h")
        {
          isRain = true;
          oraclename += "longprec_weigh_50.r_12h as r12h50,longprec_weigh_60.r_12h as r12h60,";
          paramName = "r_12h";
        }
        else if (rainParam == "r_24h")
        {
          isRain = true;
          oraclename += "longprec_manual.r_24h as r_24h,";
          paramName = "r_24h";
        }
        else
        {
          oraclename = translateParameter(p.name());
        }

        if (!isRain)
        {
          if (this->stationType == "foreign" || this->stationType == "road" ||
              this->stationType == "elering")
          {
            string tmp(oraclename);
            Fmi::ascii_toupper(tmp);
            string rainParameterInOracle = "'" + tmp + "' as " + tmp;
            queryParameterList.push_back(trimCommasFromEnd(rainParameterInOracle));
          }
          else
          {
            queryParameterList.push_back(trimCommasFromEnd(oraclename));
          }
          Fmi::ascii_toupper(oraclename);

          paramindex.insert(std::make_pair(oraclename, i));
        }
        else
        {
          queryParameterList.push_back(trimCommasFromEnd(oraclename));
          Fmi::ascii_toupper(paramName);
          paramindex.insert(std::make_pair(paramName, i));
        }
      }
      else
      {
        specialsindex.insert(std::make_pair(i, p.name()));
      }
      i++;
    }

    string q = "";
    list<string> used;
    for (const string& p : queryParameterList)
    {
      if (find(used.begin(), used.end(), p) == used.end())
      {
        q += p + ",";
        used.push_back(p);
      }
    }

    size_t end = q.find_last_not_of(",");
    queryparameters = q.substr(0, end + 1);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::makeParamIndexesForWeatherDataQC(const vector<SmartMet::Spine::Parameter>& params,
                                              map<string, int>& paramindex,
                                              map<int, string>& specialsindex,
                                              string& queryparameters,
                                              map<string, int>& queryparamindex)

{
  try
  {
    // In order to get the parameters in same order as in the original query string,
    // we need a few additional data structures for keeping track of things
    int i = 0;
    int sensor_no;
    string parametername = "";
    string parameter = "";
    for (const SmartMet::Spine::Parameter& p : params)
    {
      if (not_special(p))
      {
        // Sensor numbers are only in use for road data
        if (this->stationType == "road")
        {
          parameter = parseParameterName(p.name());
          sensor_no = parseSensorNumber(p.name());
        }
        else
        {
          parameter = p.name();
          sensor_no = 1;
        }

        string oraclename = translateParameter(parameter);
        Fmi::ascii_toupper(oraclename);
        parametername = p.name();
        Fmi::ascii_toupper(parametername);

        // TODO: remove this kludge
        if (oraclename == "KITKA")
        {
          sensor_no = 3;
        }

        string fullname = "max(case when upper(wd.parameter)='" + oraclename + "'";
        fullname += " and sensor_no=" + Fmi::to_string(sensor_no) + " then wd.value end) as \"" +
                    parametername + "\"";
        queryparamindex.insert(std::make_pair(fullname, i));
        paramindex.insert(std::make_pair(parametername, i));

        queryparameters += "'" + oraclename + "',";
      }

      else
      {
        specialsindex.insert(std::make_pair(i, p.name()));
      }
      i++;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// BUG? Why is value in the API?
std::string Oracle::makeSpecialParameter(const SmartMet::Spine::Station& station,
                                         const string& parameter,
                                         double /* value */)
{
  try
  {
    if (parameter == "name")
    {
      if (station.requestedName.length() > 0)
      {
        return station.requestedName;
      }
      else
      {
        return station.station_formal_name;
      }
    }
    if (parameter == "geoid")
    {
      return Fmi::to_string(station.geoid);
    }
    if (parameter == "stationname")
    {
      return station.station_formal_name;
    }
    else if (parameter == "distance")
    {
      if (station.distance.empty() == false && itsBoundingBoxIsGiven == false)
      {
        return valueFormatter->format(Fmi::stod(station.distance), 1);
      }
      else
      {
        return this->missingText;
      }
    }
    else if (parameter == "stationary")
    {
      return station.stationary;
    }
    else if (parameter == "stationlongitude" || parameter == "stationlon")
    {
      return valueFormatter->format(station.longitude_out, 5);
    }
    else if (parameter == "stationlatitude" || parameter == "stationlat")
    {
      return valueFormatter->format(station.latitude_out, 5);
    }
    else if (parameter == "longitude" || parameter == "lon")
    {
      return valueFormatter->format(station.requestedLon, 5);
    }
    else if (parameter == "latitude" || parameter == "lat")
    {
      return valueFormatter->format(station.requestedLat, 5);
    }
    else if (parameter == "elevation")
    {
      return valueFormatter->format(station.station_elevation, 1);
    }
    else if (parameter == "wmo")
    {
      if (station.wmo <= 0)
      {
        return this->missingText;
      }
      else
      {
        return valueFormatter->format(station.wmo, 0);
      }
    }
    else if (parameter == "lpnn")
    {
      return valueFormatter->format(station.lpnn, 0);
    }
    else if (parameter == "fmisid")
    {
      return valueFormatter->format(station.station_id, 0);
    }
    else if (parameter == "rwsid")
    {
      return valueFormatter->format(station.rwsid, 0);
    }
    // modtime is only for timeseries compatibility
    else if (parameter == "modtime")
      return "";

    else if (parameter == "model")
      return this->stationType;

    // origintime is always the current time
    else if (parameter == "origintime")
    {
      return timeFormatter->format(second_clock::universal_time());
    }
    else if (parameter == "timestring")
    {
      return "";
    }
    else if (parameter == "tz")
    {
      if (this->timeZone == "localtime")
        return station.timezone;
      else
        return this->timeZone;
    }
    else if (parameter == "place")
    {
      return station.tag;
    }
    else if (parameter == "region")
    {
      return station.region;
    }
    else if (parameter == "iso2")
    {
      return station.iso2;
    }
    else if (parameter == "direction")
    {
      if (!itsBoundingBoxIsGiven)
      {
        return valueFormatter->format(station.stationDirection, 1);
      }
      else
      {
        return this->missingText;
      }
    }
    else if (parameter == "country")
    {
      return station.country;
    }
    else
      return this->missingText;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// BUG? Why is value in the API?
SmartMet::Spine::TimeSeries::Value Oracle::makeSpecialParameterVariant(
    const SmartMet::Spine::Station& station, const string& parameter, double /* value */)
{
  try
  {
    if (parameter == "name")
    {
      if (station.requestedName.length() > 0)
        return station.requestedName;
      else
        return station.station_formal_name;
    }
    else if (parameter == "geoid")
      return station.geoid;

    else if (parameter == "stationname")
      return station.station_formal_name;

    else if (parameter == "distance")
    {
      if (station.distance.empty() == false && itsBoundingBoxIsGiven == false)
        return valueFormatter->format(Fmi::stod(station.distance), 1);
      else
        return ts::None();
    }
    else if (parameter == "stationary")
      return station.stationary;

    else if (parameter == "stationlongitude" || parameter == "stationlon")
      return station.longitude_out;

    else if (parameter == "stationlatitude" || parameter == "stationlat")
      return station.latitude_out;

    else if (parameter == "longitude" || parameter == "lon")
      return station.requestedLon;

    else if (parameter == "latitude" || parameter == "lat")
      return station.requestedLat;

    else if (parameter == "elevation")
      return station.station_elevation;

    else if (parameter == "wmo")
    {
      if (station.wmo <= 0)
        return ts::None();
      else
        return station.wmo;
    }

    else if (parameter == "lpnn")
      return station.lpnn;

    else if (parameter == "fmisid")
      return station.station_id;

    else if (parameter == "rwsid")
      return station.rwsid;

    // modtime is only for timeseries compatibility
    else if (parameter == "modtime")
      return "";

    else if (parameter == "model")
      return this->stationType;

    // origintime is always the current time
    else if (parameter == "origintime")
      return timeFormatter->format(second_clock::universal_time());

    else if (parameter == "timestring")
      return "";

    else if (parameter == "tz")
    {
      if (this->timeZone == "localtime")
        return station.timezone;
      else
        return this->timeZone;
    }

    else if (parameter == "region")
      return station.region;

    else if (parameter == "iso2")
      return station.iso2;

    else if (parameter == "direction")
    {
      if (!itsBoundingBoxIsGiven)
        return valueFormatter->format(station.stationDirection, 1);
      else
        return ts::None();
    }

    else if (parameter == "country")
      return station.country;

    else if (parameter == "place")
    {
      return station.tag;
    }

    return ts::None();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Set time interval for database query.
 */

void Oracle::setTimeInterval(const ptime& theStartTime, const ptime& theEndTime, int theTimeStep)
{
  try
  {
    this->exactStartTime = makeOTLTime(theStartTime);
    this->timeStep = (this->latest ? 1 : theTimeStep);
    this->startTime = makeOTLTime(theStartTime);
    this->endTime = makeOTLTime(theEndTime);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::setDatabaseTableName(const std::string& name)
{
  itsDatabaseTableName = name;
}

const std::string Oracle::getDatabaseTableName() const
{
  return itsDatabaseTableName;
}

otl_datetime Oracle::makeOTLTimeNow() const
{
  try
  {
    boost::posix_time::ptime now(second_clock::universal_time());
    return makeOTLTime(now);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Construct otl_datetime from boost::posix_time.
*/
otl_datetime Oracle::makeOTLTime(const ptime& time) const
{
  try
  {
    return otl_datetime(time.date().year(),
                        time.date().month(),
                        time.date().day(),
                        time.time_of_day().hours(),
                        time.time_of_day().minutes(),
                        time.time_of_day().seconds());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Construct otl_datetime which is rounded to an even hour from posix_time
 */
otl_datetime Oracle::makeRoundedOTLTime(const ptime& time) const
{
  try
  {
    int starthour = static_cast<int>(
        floor((time.time_of_day().hours() * 60 + time.time_of_day().minutes()) / 60));

    return otl_datetime(
        time.date().year(), time.date().month(), time.date().day(), starthour, 0, 0);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string Oracle::makeStringTime(const otl_datetime& time) const
{
  try
  {
    char timestamp[100];
    sprintf(
        timestamp, "%d%02d%02d%02d%02d", time.year, time.month, time.day, time.hour, time.minute);

    return timestamp;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string Oracle::makeStringTimeWithSeconds(const otl_datetime& time) const
{
  try
  {
    char timestamp[100];
    sprintf(timestamp,
            "%d%02d%02d%02d%02d%02d",
            time.year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second);

    return timestamp;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime Oracle::makePrecisionTime(const otl_datetime& time)

{
  try
  {
    // boost::posix_time::time_duration td(time.hour, time.minute, time.second, time.fraction);
    boost::posix_time::time_duration td =
        boost::posix_time::hours(time.hour) + boost::posix_time::minutes(time.minute) +
        boost::posix_time::seconds(time.second) +
        boost::posix_time::microseconds(static_cast<int>(time.fraction / 1000));

    boost::gregorian::date d(boost::numeric_cast<unsigned short>(time.year),
                             boost::numeric_cast<unsigned short>(time.month),
                             boost::numeric_cast<unsigned short>(time.day));
    boost::posix_time::ptime utctime(d, td);
    return utctime;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Construct boost::posix_time from otl_datetime.
*/
boost::posix_time::ptime Oracle::makePosixTime(const otl_datetime& time,
                                               const Fmi::TimeZones& timezones,
                                               const string& timezone) const
{
  try
  {
    try
    {
      boost::posix_time::time_duration td(time.hour, time.minute, 0, 0);
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
    catch (std::exception&)
    {
      boost::posix_time::ptime p;
      return p;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Construct unix epoch time from posix_time
 */

string Oracle::makeEpochTime(const boost::posix_time::ptime& time) const
{
  try
  {
    boost::posix_time::ptime time_t_epoch(date(1970, 1, 1));
    time_duration diff = time - time_t_epoch;

    return Fmi::to_string(diff.total_seconds());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Oracle::formatDate(const boost::local_time::local_date_time& ltime, string format)
{
  try
  {
    ostringstream os;
    os.imbue(std::locale(this->locale,
                         new boost::date_time::time_facet<local_date_time, char>(format.c_str())));
    os << ltime;
    return os.str();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Oracle::windCompass8(double direction)
{
  try
  {
    if (direction < 0)
      return this->missingText;
    static const string names[] = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

    int i = static_cast<int>((direction + 22.5) / 45) % 8;
    return names[i];
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Oracle::windCompass16(double direction)
{
  try
  {
    if (direction < 0)
      return this->missingText;
    static const string names[] = {"N",
                                   "NNE",
                                   "NE",
                                   "ENE",
                                   "E",
                                   "ESE",
                                   "SE",
                                   "SSE",
                                   "S",
                                   "SSW",
                                   "SW",
                                   "WSW",
                                   "W",
                                   "WNW",
                                   "NW",
                                   "NNW"};

    int i = static_cast<int>((direction + 11.25) / 22.5) % 16;
    return names[i];
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Oracle::windCompass32(const double direction)
{
  try
  {
    if (direction < 0)
      return this->missingText;
    static const string names[] = {"N", "NbE", "NNE", "NEbN", "NE", "NEbE", "ENE", "EbN",
                                   "E", "EbS", "ESE", "SEbE", "SE", "SEbS", "SSE", "SbE",
                                   "S", "SbW", "SSW", "SWbS", "SW", "SWbW", "WSW", "WbS",
                                   "W", "WbN", "WNW", "NWbW", "NW", "NWbN", "NNW", "NbW"};

    int i = static_cast<int>((direction + 5.625) / 11.25) % 32;
    return names[i];
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::replaceRainParameters(string& queryParameters)
{
  try
  {
    // Replace the original rain parameters from parameter list with SQL which determines
    // which gauge must be used for rain measurement
    size_t pos = queryParameters.find("R_1H,");
    if (pos != string::npos)
    {
      queryParameters.replace(
          pos, 5, " case when r1h50 is not null then r1h50 else r1h60 end as r_1h,");
    }
    pos = queryParameters.find("R_1H");
    if (pos != string::npos)
    {
      queryParameters.replace(
          pos, 4, " case when r1h50 is not null then r1h50 else r1h60 end as r_1h");
    }
    pos = queryParameters.find("R_12H,");
    if (pos != string::npos)
    {
      queryParameters.replace(
          pos, 6, " case when r12h50 is not null then r12h50 else r12h60 end as r_12h,");
    }
    pos = queryParameters.find("R_12H");
    if (pos != string::npos)
    {
      queryParameters.replace(
          pos, 5, " case when r12h50 is not null then r12h50 else r12h60 end as r_12h");
    }
    pos = queryParameters.find("R_24H,");
    if (pos != string::npos)
    {
      queryParameters.replace(pos, 6, " r_24h,");
    }
    pos = queryParameters.find("R_24H");
    if (pos != string::npos)
    {
      queryParameters.replace(pos, 5, " r_24h");
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Helper method to get a sql snippet which helps to sort stations by distance
 */

string Oracle::getDistanceSql(const SmartMet::Spine::Stations& stations)
{
  try
  {
    string distanceSql = "";
    distanceSql += "CASE ";
    for (const SmartMet::Spine::Station& station : stations)
    {
      distanceSql += "WHEN s.lpnn = ";
      distanceSql += Fmi::to_string(station.lpnn);
      distanceSql += " THEN (SELECT ";
      // Reset all distances to zero if bounding box is given in query
      // because it simplifies the API.
      if (itsBoundingBoxIsGiven)
      {
        distanceSql += "0";
      }
      else if (!station.distance.empty())
      {
        distanceSql += station.distance;
      }
      else
      {
        distanceSql += "0";
      }
      distanceSql += " FROM dual) ";
    }
    distanceSql += "END as distance, ";
    return distanceSql;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Helper method to get a sql snippet which creates a date interval to be used
 * to patch missing observations with null values.
 */

string Oracle::getIntervalSql(const string& qstations, const Fmi::TimeZones& timezones)
{
  try
  {
    // The following solves how many date intervals we must have
    time_duration td = makePosixTime(this->endTime, timezones, this->timeZone) -
                       makePosixTime(this->startTime, timezones, this->timeZone);
    int totalIntervals =
        static_cast<int>(td.total_seconds() / 60.0 / static_cast<double>(this->timeStep)) + 1;

    string intervalSql = "";
    intervalSql += "(\n";
    intervalSql += "select l.lpnns, d.intervals \n";
    intervalSql += "from \n";
    intervalSql += "(select s.lpnn as lpnns from sreg s where s.lpnn in (" + qstations + ")\n";
    intervalSql += ") l,\n";
    intervalSql += "(\n";
    intervalSql += "select rownum * interval '" + Fmi::to_string(this->timeStep) + "' minute + ";
    intervalSql += " to_date('" + makeStringTime(this->startTime) +
                   "', 'YYYYMMDDHH24MI') - interval '" + Fmi::to_string(this->timeStep) +
                   "' minute as intervals ";
    intervalSql += " from dual connect by rownum <= " + Fmi::to_string(totalIntervals) + "\n";
    intervalSql += ") d \n";
    intervalSql += " where d.intervals >=  to_date('" +
                   makeStringTimeWithSeconds(this->exactStartTime) + "', 'YYYYMMDDHH24MISS') \n";
    intervalSql += ") a \n";

    return intervalSql;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Get a row from database and format it as a Table
*/

void Oracle::makeRow(SmartMet::Spine::Table& result,
                     const int& metacount,
                     map<string, int>& paramindex,
                     map<int, string>& specialsindex,
                     map<double, SmartMet::Spine::Station>& stationindex,
                     otl_datetime& timestamp,  // this is in utc time
                     int sensor_no,
                     double winddirection,
                     int level,
                     int id,
                     int& rownumber,
                     otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream>& rs,
                     otl_stream& query,
                     const SmartMet::Spine::Station& station,
                     const Fmi::TimeZones& timezones,
                     double windspeed,
                     double temperature,
                     double relativehumidity)
{
  try
  {
    // If all parameters are NULL, do not show the row
    // unless the time step is explicitly requested (other than 1).
    bool resultIsEmpty = true;

    for (unsigned int k = metacount + 1; k < paramindex.size() + metacount + 1; k++)
    {
      if (!rs.is_null(k))
      {
        resultIsEmpty = false;
      }
    }
    if (resultIsEmpty)
    {
      if (this->timeStep == 1)
      {
        return;
      }
    }

    boost::posix_time::ptime utctime = makePosixTime(timestamp, timezones, "UTC");

    auto localtz = timezones.time_zone_from_string(this->timeZone);

    local_date_time localtime(utctime, localtz);
    boost::posix_time::ptime ltime;
    if (this->timeZone == "localtime")
    {
      ltime = makePosixTime(timestamp, timezones, station.timezone);
    }
    else
    {
      ltime = makePosixTime(timestamp, timezones, this->timeZone);
    }

    // If hour option is given, use only given hours
    if (!this->hours.empty())
    {
      // Use given time zone
      int currentHour = ltime.time_of_day().hours();
      if (std::find(hours.begin(), hours.end(), currentHour) == hours.end() ||
          timestamp.minute != 0)
      {
        return;
      }
    }
    // If weekday option is given, use only given days
    if (!this->weekdays.empty())
    {
      // Use given time zone
      int currentDay = ltime.date().day_of_week();
      if (std::find(weekdays.begin(), weekdays.end(), currentDay) == weekdays.end())
      {
        return;
      }
    }

    double obs = 0.0;  // value fetched from database
    otl_var_desc* desc;
    int desc_len;
    desc = query.describe_out_vars(desc_len);

    int column;
    string parameter;

    for (const auto& iter : specialsindex)
    {
      column = iter.first;
      parameter = iter.second;
      // Check time related parameters first (and sensor_no, level)
      if (parameter == "time")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(column, ts::TimedValue(localtime, timeFormatter->format(ltime)));
        else
          result.set(column, rownumber, timeFormatter->format(ltime));
      }
      else if (parameter == "sensor_no")
      {
        if (itsTimeSeriesColumns)
        {
          setTimeSeriesStationValue(station.fmisid, column, ts::TimedValue(localtime, sensor_no));
          setTimeSeriesValue(column, ts::TimedValue(localtime, sensor_no));
        }
        else
          result.set(column, rownumber, valueFormatter->format(sensor_no, 0));
      }
      else if (parameter == "level")
      {
        if (itsTimeSeriesColumns)
        {
          setTimeSeriesStationValue(station.fmisid, column, ts::TimedValue(localtime, level));
          setTimeSeriesValue(column, ts::TimedValue(localtime, level));
        }
        else
          result.set(column, rownumber, valueFormatter->format(level, 0));
      }
      else if (parameter == "xmltime")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(column, ts::TimedValue(localtime, timeFormatter->format(ltime)));
        else
          result.set(column, rownumber, timeFormatter->format(ltime));
      }
      else if (parameter == "localtime")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(column,
                             ts::TimedValue(localtime,
                                            timeFormatter->format(makePosixTime(
                                                timestamp, timezones, station.timezone))));
        else
          result.set(column,
                     rownumber,
                     timeFormatter->format(makePosixTime(timestamp, timezones, station.timezone)));
      }
      else if (parameter == "utctime")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime,
                             timeFormatter->format(makePosixTime(timestamp, timezones, "UTC"))));
        else
          result.set(
              column, rownumber, timeFormatter->format(makePosixTime(timestamp, timezones, "UTC")));
      }
      else if (parameter == "epochtime")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime, makeEpochTime(makePosixTime(timestamp, timezones, "UTC"))));
        else
          result.set(column, rownumber, makeEpochTime(makePosixTime(timestamp, timezones, "UTC")));
      }
      else if (parameter == "weekday" || parameter == "wday")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(column, ts::TimedValue(localtime, formatDate(localtime, "%A")));
        else
          result.set(column, rownumber, formatDate(localtime, "%A"));
      }

      else if (parameter == "dark")
      {
        auto solarpos =
            Fmi::Astronomy::solar_position(utctime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column, ts::TimedValue(localtime, Fmi::to_string(static_cast<int>(solarpos.dark()))));
        else
          result.set(column, rownumber, Fmi::to_string(static_cast<int>(solarpos.dark())));
      }
      else if (parameter == "sunelevation")
      {
        auto solarpos =
            Fmi::Astronomy::solar_position(utctime, station.longitude_out, station.latitude_out);

        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column, ts::TimedValue(localtime, valueFormatter->format(solarpos.elevation, 0)));
        else
          result.set(column, rownumber, valueFormatter->format(solarpos.elevation, 0));
      }

      else if (parameter == "sundeclination")
      {
        auto solarpos =
            Fmi::Astronomy::solar_position(utctime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column, ts::TimedValue(localtime, valueFormatter->format(solarpos.declination, 0)));
        else
          result.set(column, rownumber, valueFormatter->format(solarpos.declination, 0));
      }

      else if (parameter == "sunazimuth")
      {
        auto solarpos =
            Fmi::Astronomy::solar_position(utctime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column, ts::TimedValue(localtime, valueFormatter->format(solarpos.azimuth, 0)));
        else
          result.set(column, rownumber, valueFormatter->format(solarpos.azimuth, 0));
      }
      else if (parameter == "moonphase")
      {
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime,
                             valueFormatter->format(Fmi::Astronomy::moonphase(utctime), 0)));
        else
          result.set(
              column, rownumber, valueFormatter->format(Fmi::Astronomy::moonphase(utctime), 0));
      }

      else if (parameter == "sunrise")
      {
        auto solartime =
            Fmi::Astronomy::solar_time(localtime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime, timeFormatter->format(solartime.sunrise.local_time())));
        else
          result.set(column, rownumber, timeFormatter->format(solartime.sunrise.local_time()));
      }

      else if (parameter == "sunset")
      {
        auto solartime =
            Fmi::Astronomy::solar_time(localtime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime, timeFormatter->format(solartime.sunset.local_time())));
        else
          result.set(column, rownumber, timeFormatter->format(solartime.sunset.local_time()));
      }
      else if (parameter == "noon")
      {
        auto solartime =
            Fmi::Astronomy::solar_time(localtime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime, timeFormatter->format(solartime.noon.local_time())));
        else
          result.set(column, rownumber, timeFormatter->format(solartime.noon.local_time()));
      }
      else if (parameter == "sunrisetoday")
      {
        auto solartime =
            Fmi::Astronomy::solar_time(localtime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime,
                             Fmi::to_string(static_cast<int>(solartime.sunrise_today()))));
        else
          result.set(
              column, rownumber, Fmi::to_string(static_cast<int>(solartime.sunrise_today())));
      }
      else if (parameter == "sunsettoday")
      {
        auto solartime =
            Fmi::Astronomy::solar_time(localtime, station.longitude_out, station.latitude_out);
        if (itsTimeSeriesColumns)
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime,
                             Fmi::to_string(static_cast<int>(solartime.sunset_today()))));
        else
          result.set(column, rownumber, Fmi::to_string(static_cast<int>(solartime.sunset_today())));
      }

#if 0
      else if(parameter == "daylength")
      {
        auto solartime = Fmi::Astronomy::solar_time(localtime,station.longitude_out,station.latitude_out);
        // TODO: check this works
        result.set(column,rownumber,timeFormatter->format(solartime.daylength()));
      }
#endif

      else if (parameter == "windcompass8")
      {
        if (itsTimeSeriesColumns)
        {
          std::string ret = windCompass8(winddirection);
          if (ret == this->missingText)
            setTimeSeriesValue(column, ts::TimedValue(localtime, ts::None()));
          else
            setTimeSeriesValue(column, ts::TimedValue(localtime, ret));
        }
        else
          result.set(column, rownumber, windCompass8(winddirection));
      }
      else if (parameter == "windcompass16")
      {
        if (itsTimeSeriesColumns)
        {
          std::string ret = windCompass16(winddirection);
          if (ret == this->missingText)
            setTimeSeriesValue(column, ts::TimedValue(localtime, ts::None()));
          else
            setTimeSeriesValue(column, ts::TimedValue(localtime, ret));
        }
        else
          result.set(column, rownumber, windCompass16(winddirection));
      }
      else if (parameter == "windcompass32")
      {
        if (itsTimeSeriesColumns)
        {
          std::string ret = windCompass32(winddirection);
          if (ret == this->missingText)
            setTimeSeriesValue(column, ts::TimedValue(localtime, ts::None()));
          else
            setTimeSeriesValue(column, ts::TimedValue(localtime, ret));
        }
        else
          result.set(column, rownumber, windCompass32(winddirection));
      }
      else if (parameter == "feelslike")
      {
        float feelslike;

        if (windspeed == kFloatMissing || temperature == kFloatMissing ||
            relativehumidity == kFloatMissing)
        {
          feelslike = kFloatMissing;
        }
        else
        {
          feelslike = FmiFeelsLikeTemperature(
              windspeed, relativehumidity, temperature, kFloatMissing);  // Ignore radiation
        }

        if (itsTimeSeriesColumns)
        {
          if (feelslike == kFloatMissing)
            setTimeSeriesValue(column, ts::TimedValue(localtime, ts::None()));
          else
            setTimeSeriesValue(column, ts::TimedValue(localtime, feelslike));
        }
        else
          result.set(column, rownumber, std::to_string(feelslike));
      }
      // Then check special parameters
      else
      {
        if (itsTimeSeriesColumns)
        {
          setTimeSeriesStationValue(
              station.fmisid,
              column,
              ts::TimedValue(localtime, makeSpecialParameterVariant(stationindex[id], parameter)));
          setTimeSeriesValue(
              column,
              ts::TimedValue(localtime, makeSpecialParameterVariant(stationindex[id], parameter)));
        }
        else
          result.set(column, rownumber, makeSpecialParameter(stationindex[id], parameter));
      }
    }

    // Then fetch the actual values from database. The parameter value index start with
    // metacount+1, where metacount is the number of meta parameters.

    for (unsigned int k = metacount + 1; k < paramindex.size() + metacount + 1; k++)
    {
      unsigned int columnnumber(paramindex[desc[k - 1].name]);

      if (rs.is_null(k))
      {
        if (itsTimeSeriesColumns)
        {
          setTimeSeriesStationValue(
              station.fmisid, columnnumber, ts::TimedValue(localtime, ts::None()));
          setTimeSeriesValue(columnnumber, ts::TimedValue(localtime, ts::None()));
        }
        else
          result.set(columnnumber, rownumber, this->missingText);
      }
      else
      {
        rs.get(k, obs);

        if (itsTimeSeriesColumns)
        {
          setTimeSeriesStationValue(station.fmisid, columnnumber, ts::TimedValue(localtime, obs));
          setTimeSeriesValue(columnnumber, ts::TimedValue(localtime, obs));
        }
        else
        {
          // TODO add default formatting options to conf file
          if (boost::lexical_cast<std::string>(desc[k - 1].name) == "KITKA")
          {
            result.set(columnnumber, rownumber, valueFormatter->format(obs, 2));
          }
          else
          {
            result.set(columnnumber, rownumber, valueFormatter->format(obs, 1));
          }
        }
      }
    }

    // If we get to this point, only then enlarge the rownumber
    rownumber++;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// BUG? latitude and longitude are not used
void Oracle::addInfoToStation(SmartMet::Spine::Station& station,
                              const double /* latitude */,
                              const double /* longitude */)
{
  try
  {
    const std::string lang = (this->language.empty() ? "fi" : language);

    Locus::QueryOptions opts;
    opts.SetLanguage("fmisid");
    opts.SetResultLimit(1);
    opts.SetCountries("");
    opts.SetFullCountrySearch(true);
    opts.SetFeatures("SYNOP");
    opts.SetSearchVariants(true);

    SmartMet::Spine::LocationList places;

    try
    {
      // Search by fmisid.
      std::string fmisid_s = Fmi::to_string(station.fmisid);
      SmartMet::Spine::LocationList suggest = geonames->nameSearch(opts, fmisid_s);

      opts.SetLanguage(lang);

      if (not suggest.empty())
      {
        // When language is "fmisid" the name is the fmisid.
        if (suggest.front()->name == fmisid_s)
          places = geonames->idSearch(opts, suggest.front()->geoid);
      }

      // Trying to find a location of station by assuming the geoid is
      // negative value of fmisid.
      if (places.empty())
      {
        places = geonames->idSearch(opts, -station.fmisid);
      }

      // Next looking for a nearest station inside 50 meter radius.
      // There might be multiple stations at the same positon so the possibility to get
      // a wrong geoid is big.
      if (places.empty())
      {
        places = geonames->latlonSearch(opts,
                                        boost::numeric_cast<float>(station.latitude_out),
                                        boost::numeric_cast<float>(station.longitude_out),
                                        0.05);
      }

      // As a fallback we will try to find neasert populated place.
      // There is some places this will also fail e.g. South Pole (0.0, -90).
      if (places.empty())
      {
        opts.SetFeatures("PPL");
        places = geonames->latlonSearch(opts,
                                        boost::numeric_cast<float>(station.latitude_out),
                                        boost::numeric_cast<float>(station.longitude_out));
      }
    }
    catch (...)
    {
      return;
    }

    for (const auto& place : places)
    {
      station.country = place->country;
      station.geoid = place->geoid;
      station.requestedLat = place->latitude;
      station.requestedLon = place->longitude;
      station.requestedName = place->name;
      station.timezone = place->timezone;
      station.region = place->area;
      station.station_elevation = place->elevation;
    }
    calculateStationDirection(station);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Fills station with related information from given location
 */

void Oracle::addInfoToStation(SmartMet::Spine::Station& station,
                              const SmartMet::Spine::LocationPtr& location)
{
  try
  {
    station.geoid = location->geoid;
    station.requestedName = location->name;
    station.requestedLon = location->longitude;
    station.requestedLat = location->latitude;
    // Get station direction from given coordinates
    calculateStationDirection(station);

    station.iso2 = location->iso2;

    if (location->area.empty())
    {
      // This reintroduces an older bug/feature where
      // the name of the location is given as a region
      // if it doesn't belong to any administrative region.
      // (i.e. Helsinki doesn't have region, Kumpula has.)
      //
      // Also checking whether the loc.name has valid data,
      // if it's empty as well - which shoudn't occur - we return missingText
      if (location->name.empty())
      {
        station.region = this->missingText;
      }
      else
      {
        // Place name known, administrative region unknown.
        station.region = location->name;
      }
    }
    else
    {
      // Administrative region known.
      station.region = location->area;
    }

    // Get more info from fminames database
    Locus::QueryOptions opts;
    opts.SetLanguage(this->language);
    opts.SetResultLimit(1);
    opts.SetCountries("");
    opts.SetFullCountrySearch(true);

    auto places = geonames->latlonSearch(opts,
                                         boost::numeric_cast<float>(station.latitude_out),
                                         boost::numeric_cast<float>(station.longitude_out));

    for (const auto& place : places)
    {
      station.country = place->country;
      // TODO get rid of the magic number -1 (uninitialized)
      if (station.geoid == -1)
      {
        station.geoid = place->geoid;
      }
    }
    station.timezone = location->timezone;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::translateWMOToLPNN(const vector<int>& wmos, SmartMet::Spine::Stations& stations)
{
  try
  {
    size_t original_size = stations.size();

    otl_stream s(1,
                 "begin "
                 ":rc<double,out> := STATION_QP.getLPNNforWMON(:in_wmon<int,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);

    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int lpnn = -1;
    otl_datetime in_valid_date = makeOTLTimeNow();
    for (int wmo : wmos)
    {
      auto cacheresult = globalWMOToLPNNCache.find(wmo);
      if (cacheresult)
      {
        SmartMet::Spine::Station station;
        station.lpnn = *cacheresult;
        station.wmo = wmo;
        stations.push_back(station);
      }
      else
      {
        // Search the database
        try
        {
          s << wmo << in_valid_date;
          si.attach(s);
          while (si.next_row())
          {
            lpnn = -1;
            si.get(1, lpnn);
            SmartMet::Spine::Station station;
            station.lpnn = lpnn;
            station.wmo = wmo;
            stations.push_back(station);
            globalWMOToLPNNCache.insert(wmo, lpnn);
          }
        }
        catch (otl_exception& p)  // intercept OTL exceptions
        {
          if (isFatalError(p.code))  // reconnect if fatal error is encountered
          {
            cerr << "ERROR: " << endl;
            cerr << p.msg << endl;       // print out error message
            cerr << p.stm_text << endl;  // print out SQL that caused the error
            cerr << p.var_info << endl;  // print out the variable that caused the error

            reConnect();
            stations.resize(original_size);
            return translateWMOToLPNN(wmos, stations);
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
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::translateToLPNN(SmartMet::Spine::Stations& stations)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<double,out> := STATION_QP.getLPNN(:in_station_id<double,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int lpnn = -1;
    otl_datetime in_valid_date = makeOTLTimeNow();

    int i = 0;
    for (SmartMet::Spine::Station& info : stations)
    {
      // If station already has lpnn number, don't get it again
      if (info.lpnn <= 0)
      {
        // BUG? Why is the id a double??
        auto cacheresult = globalIdToLPNNCache.find(boost::numeric_cast<int>(info.station_id));
        if (cacheresult)
        {
          info.lpnn = *cacheresult;
        }
        else
        {
          // Search database
          try
          {
            s << info.station_id << in_valid_date;
            si.attach(s);
            while (si.next_row())
            {
              lpnn = -1;
              si.get(1, lpnn);
              info.lpnn = lpnn;
              i++;
              // BUG? Why is the id a double??
              globalIdToLPNNCache.insert(boost::numeric_cast<int>(info.station_id), lpnn);
            }
          }
          catch (otl_exception& p)  // intercept OTL exceptions
          {
            if (isFatalError(p.code))  // reconnect if fatal error is encountered
            {
              cerr << "ERROR: " << endl;
              cerr << p.msg << endl;       // print out error message
              cerr << p.stm_text << endl;  // print out SQL that caused the error
              cerr << p.var_info << endl;  // print out the variable that caused the error

              reConnect();
              return translateToLPNN(stations);
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
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::translateToWMO(SmartMet::Spine::Stations& stations)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<double,out> := STATION_QP.getWMON(:in_station_id<double,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int wmo = 0;
    otl_datetime in_valid_date = makeOTLTimeNow();

    int i = 0;
    for (SmartMet::Spine::Station& info : stations)
    {
      if (info.wmo <= 0)
      {
        // BUG? Why is the id a double??
        auto cacheresult = globalIdToWMOCache.find(boost::numeric_cast<int>(info.station_id));
        if (cacheresult)
          info.wmo = *cacheresult;
        else
        {
          try
          {
            s << info.station_id << in_valid_date;
            si.attach(s);
            while (si.next_row())
            {
              si.get(1, wmo);
              info.wmo = wmo;
              i++;
              // BUG? Why is the id a double??
              globalIdToWMOCache.insert(boost::numeric_cast<int>(info.station_id), wmo);
            }
          }
          catch (otl_exception& p)  // intercept OTL exceptions
          {
            if (isFatalError(p.code))  // reconnect if fatal error is encountered
            {
              cerr << "ERROR: " << endl;
              cerr << p.msg << endl;       // print out error message
              cerr << p.stm_text << endl;  // print out SQL that caused the error
              cerr << p.var_info << endl;  // print out the variable that caused the error

              reConnect();
              return translateToWMO(stations);
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
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::translateToRWSID(SmartMet::Spine::Stations& stations)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<double,out> := STATION_QP.getRWSID(:in_station_id<double,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int rwsid = 0;
    otl_datetime in_valid_date = makeOTLTimeNow();

    int i = 0;
    for (SmartMet::Spine::Station& info : stations)
    {
      if (info.rwsid <= 0)
      {
        // BUG? Why is the id a double??
        auto cacheresult = globalIdToRWSIDCache.find(boost::numeric_cast<int>(info.station_id));
        if (cacheresult)
          info.rwsid = *cacheresult;
        else
        {
          try
          {
            s << info.station_id << in_valid_date;
            si.attach(s);
            while (si.next_row())
            {
              si.get(1, rwsid);
              info.rwsid = rwsid;
              i++;
              // BUG? Why is the id a double??
              globalIdToRWSIDCache.insert(boost::numeric_cast<int>(info.station_id), rwsid);
            }
          }
          catch (otl_exception& p)  // intercept OTL exceptions
          {
            if (isFatalError(p.code))  // reconnect if fatal error is encountered
            {
              cerr << "ERROR: " << endl;
              cerr << p.msg << endl;       // print out error message
              cerr << p.stm_text << endl;  // print out SQL that caused the error
              cerr << p.var_info << endl;  // print out the variable that caused the error

              reConnect();
              return translateToRWSID(stations);
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
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

vector<int> Oracle::translateWMOToFMISID(const vector<int>& wmos)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<int,out> := STATION_QP.getFMISIDforWMON(:in_station_id<int,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int fmisid = 0;
    otl_datetime in_valid_date = makeOTLTimeNow();

    vector<int> fmisids;

    for (int wmo : wmos)
    {
      auto cacheresult = globalWMOToFMISIDCache.find(wmo);
      if (cacheresult)
        fmisids.push_back(*cacheresult);
      else
      {
        try
        {
          s << wmo << in_valid_date;
          si.attach(s);
          while (si.next_row())
          {
            si.get(1, fmisid);
            fmisids.push_back(fmisid);
            globalWMOToFMISIDCache.insert(wmo, fmisid);
          }
        }
        catch (otl_exception& p)  // intercept OTL exceptions
        {
          if (isFatalError(p.code))  // reconnect if fatal error is encountered
          {
            cerr << "ERROR: " << endl;
            cerr << p.msg << endl;       // print out error message
            cerr << p.stm_text << endl;  // print out SQL that caused the error
            cerr << p.var_info << endl;  // print out the variable that caused the error

            reConnect();
            return translateWMOToFMISID(wmos);
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
    }
    return fmisids;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

vector<int> Oracle::translateRWSIDToFMISID(const vector<int>& wmos)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<int,out> := STATION_QP.getFMISIDforRWSID(:in_station_id<int,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int fmisid = 0;
    otl_datetime in_valid_date = makeOTLTimeNow();

    vector<int> fmisids;

    for (int wmo : wmos)
    {
      auto cacheresult = globalRWSIDToFMISIDCache.find(wmo);
      if (cacheresult)
        fmisids.push_back(*cacheresult);
      else
      {
        try
        {
          s << wmo << in_valid_date;
          si.attach(s);
          while (si.next_row())
          {
            si.get(1, fmisid);
            fmisids.push_back(fmisid);
            globalRWSIDToFMISIDCache.insert(wmo, fmisid);
          }
        }
        catch (otl_exception& p)  // intercept OTL exceptions
        {
          if (isFatalError(p.code))  // reconnect if fatal error is encountered
          {
            cerr << "ERROR: " << endl;
            cerr << p.msg << endl;       // print out error message
            cerr << p.stm_text << endl;  // print out SQL that caused the error
            cerr << p.var_info << endl;  // print out the variable that caused the error

            reConnect();
            return translateRWSIDToFMISID(wmos);
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
    }
    return fmisids;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

vector<int> Oracle::translateLPNNToFMISID(const vector<int>& lpnns)
{
  try
  {
    otl_stream s(1,
                 "begin "
                 ":rc<int,out> := STATION_QP.getFMISIDforLPNN(:in_station_id<int,in>, "
                 ":in_valid_date<timestamp,in>); "
                 "end;",
                 thedb);
    s.set_commit(0);
    otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> si;
    int fmisid = 0;
    otl_datetime in_valid_date = makeOTLTimeNow();

    vector<int> fmisids;

    for (int lpnn : lpnns)
    {
      auto cacheresult = globalLPNNToFMISIDCache.find(lpnn);
      if (cacheresult)
        fmisids.push_back(*cacheresult);
      else
      {
        try
        {
          s << lpnn << in_valid_date;
          si.attach(s);
          while (si.next_row())
          {
            si.get(1, fmisid);
            fmisids.push_back(fmisid);
            globalLPNNToFMISIDCache.insert(lpnn, fmisid);
          }
        }
        catch (otl_exception& p)  // intercept OTL exceptions
        {
          if (isFatalError(p.code))  // reconnect if fatal error is encountered
          {
            cerr << "ERROR: " << endl;
            cerr << p.msg << endl;       // print out error message
            cerr << p.stm_text << endl;  // print out SQL that caused the error
            cerr << p.var_info << endl;  // print out the variable that caused the error

            reConnect();
            return translateLPNNToFMISID(lpnns);
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
    }
    return fmisids;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Calculates station direction in degrees from given coordinates

void Oracle::calculateStationDirection(SmartMet::Spine::Station& station)
{
  try
  {
    double direction;
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude_out);
    double lat2 = deg2rad(station.latitude_out);

    double dlon = lon2 - lon1;

    direction = rad2deg(
        atan2(sin(dlon) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)));

    if (direction < 0)
    {
      direction += 360.0;
    }

    station.stationDirection = std::round(10.0 * direction) / 10.0;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Utility method to convert degrees to radians

double Oracle::deg2rad(double deg)
{
  return (deg * PI / 180);
}

// Utility method to convert radians to degrees

double Oracle::rad2deg(double rad)
{
  return (rad * 180 / PI);
}

// Return the correct number for stationtype

int Oracle::solveStationtype()
{
  try
  {
    int stationTypeId = 0;
    if (this->stationType == "road")
    {
      stationTypeId = 141;
    }
    // AWS, SYNOP, and CLIM
    else if (this->stationType == "fmi" || this->stationType == "hourly" ||
             this->stationType == "daily" || this->stationType == "monthly" ||
             this->stationType == "opendata" || this->stationType == "opendata_daily" ||
             this->stationType == "opendata_minute")
    {
      stationTypeId = -5;
    }
    // AWS, SYNOP and CLIM, AVI
    else if (this->stationType == "lammitystarve")
    {
      stationTypeId = -5;
    }
    else if (this->stationType == "solar" || this->stationType == "minute_rad")
    {
      stationTypeId = -2;
    }
    else if (this->stationType == "foreign")
    {
      stationTypeId = 142;
    }
    else if (this->stationType == "sounding")
    {
      stationTypeId = 127;
    }

    else if (this->stationType == "elering")
    {
      stationTypeId = 142;
    }
    else if (this->stationType == "PREC")
    {
      stationTypeId = 124;
    }
    else if (this->stationType == "MAST")
    {
      stationTypeId = 132;
    }
    else if (this->stationType == "mareograph" || this->stationType == "opendata_mareograph")
    {
      stationTypeId = 133;
    }
    else if (this->stationType == "buoy" || this->stationType == "opendata_buoy")
    {
      stationTypeId = 137;
    }
    else if (this->stationType == "research")
    {
      stationTypeId = 146;
    }
    else if (this->stationType == "syke")
    {
      stationTypeId = 641;
    }

    return stationTypeId;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Oracle::solveStationtypeList()
{
  try
  {
    string stationTypeList;
    if (this->stationType == "fmi" || this->stationType == "lammitystarve" ||
        this->stationType == "hourly" || this->stationType == "daily" ||
        this->stationType == "solar" || this->stationType == "minute_rad" ||
        this->stationType == "monthly" || this->stationType == "opendata" ||
        this->stationType == "opendata_daily" || this->stationType == "opendata_minute")
    {
      stationTypeList = "121, 122, 123, 224";
    }
    else if (this->stationType == "road")
    {
      stationTypeList = "141";
    }
    else if (this->stationType == "foreign")
    {
      stationTypeList = "142";
    }
    else if (this->stationType == "sounding")
    {
      stationTypeList = "127";
    }
    else if (this->stationType == "elering")
    {
      stationTypeList = "142";
    }
    else if (this->stationType == "PREC")
    {
      stationTypeList = "124";
    }
    else if (this->stationType == "MAST")
    {
      stationTypeList = "132";
    }
    else if (this->stationType == "mareograph" || this->stationType == "opendata_mareograph")
    {
      stationTypeList = "133";
    }
    else if (this->stationType == "buoy" || this->stationType == "opendata_buoy")
    {
      stationTypeList = "137";
    }
    else if (this->stationType == "syke")
    {
      stationTypeList = "641";
    }
    else if (this->stationType == "all")
    {
      stationTypeList = "";
    }

    return stationTypeList;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

map<string, double> Oracle::getStationCoordinates(int fmisid)
{
  try
  {
    std::map<string, double> latlon;

    auto cacheresult = globalStationCoordinatesCache.find(fmisid);
    if (cacheresult)
    {
      latlon["lat"] = cacheresult->first;
      latlon["lon"] = cacheresult->second;
      return latlon;
    }

    double lat = 0;
    double lon = 0;

    string querystring =
        "select latitude,longitude from locations where fmisid = " + Fmi::to_string(fmisid);
    otl_stream query;
    try
    {
      query.open(1, querystring.c_str(), thedb);
      query.set_commit(0);
      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> rs;
      rs.attach(query);

      while (rs.next_row())
      {
        rs.get(1, lat);
        rs.get(2, lon);
      }
      rs.detach();
      query.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        reConnect();
        return getStationCoordinates(fmisid);
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

    // Cache the result

    globalStationCoordinatesCache.insert(fmisid, std::make_pair(lat, lon));

    latlon.insert(std::make_pair("lat", lat));
    latlon.insert(std::make_pair("lon", lon));
    return latlon;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::setTimeSeriesStationValue(int fmisid, unsigned int column, const ts::TimedValue& tv)
{
  try
  {
    ts::TimeSeriesVectorPtr tsv;

    if (itsTimeSeriesStationColumns.find(fmisid) == itsTimeSeriesStationColumns.end())
    {
      ts::TimeSeriesVectorPtr tsvt(new ts::TimeSeriesVector());
      itsTimeSeriesStationColumns.insert(make_pair(fmisid, tsvt));
      tsv = tsvt;
    }
    else
    {
      tsv = itsTimeSeriesStationColumns[fmisid];
    }

    while (tsv->size() < column + 1)
      tsv->push_back(ts::TimeSeries());
    tsv->at(column).push_back(tv);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Oracle::setTimeSeriesValue(unsigned int column, const ts::TimedValue& tv)
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

ts::TimeSeriesVectorPtr Oracle::values(Settings& settings,
                                       const SmartMet::Spine::Stations& stations,
                                       const Fmi::TimeZones& timezones)
{
  try
  {
    itsTimeSeriesColumns = ts::TimeSeriesVectorPtr(new ts::TimeSeriesVector);

    // Road, foreign and mareograph stations use FMISID numbers, so LPNN translation is not needed.
    // They also use same cldb table, so all observation types can be fetched with the same method.
    if (settings.stationtype == "road" || settings.stationtype == "foreign" ||
        settings.stationtype == "elering" || settings.stationtype == "mareograph" ||
        settings.stationtype == "buoy")
    {
      getWeatherDataQCObservations(settings.parameters, stations, timezones);
    }

    // Stations maintained by FMI
    else if (settings.stationtype == "fmi")
    {
      getFMIObservations(settings.parameters, stations, timezones);
    }
    // Stations which measure solar radiation settings.parameters
    else if (settings.stationtype == "solar")
    {
      getSolarObservations(settings.parameters, stations, timezones);
    }
    else if (settings.stationtype == "minute_rad")
    {
      getMinuteRadiationObservations(settings.parameters, stations, timezones);
    }

    // Hourly data
    else if (settings.stationtype == "hourly")
    {
      getHourlyFMIObservations(settings.parameters, stations, timezones);
    }
    // Sounding data
    else if (settings.stationtype == "sounding")
    {
      getSoundings(settings.parameters, stations, timezones);
    }
    // Daily data
    else if (settings.stationtype == "daily")
    {
      getDailyAndMonthlyObservations(settings.parameters, stations, "daily", timezones);
    }
    else if (settings.stationtype == "lammitystarve")
    {
      getDailyAndMonthlyObservations(settings.parameters, stations, "lammitystarve", timezones);
    }
    else if (settings.stationtype == "monthly")
    {
      getDailyAndMonthlyObservations(settings.parameters, stations, "monthly", timezones);
    }

    return itsTimeSeriesColumns;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

FlashCounts Oracle::getFlashCount(const boost::posix_time::ptime& starttime,
                                  const boost::posix_time::ptime& endtime,
                                  const SmartMet::Spine::TaggedLocationList& locations)
{
  try
  {
    FlashCounts flashcounts;
    flashcounts.flashcount = 0;
    flashcounts.strokecount = 0;
    flashcounts.iccount = 0;

    std::string sqltemplate =
        "SELECT "
        "SUM(CASE WHEN flash.multiplicity > 0 THEN 1 ELSE 0 END) AS flashcount, "
        "SUM(CASE WHEN flash.multiplicity = 0 THEN 1 ELSE 0 END) AS strokecount, "
        "SUM(CASE WHEN flash.cloud_indicator = 1 THEN 1 ELSE 0 END) AS iccount "
        "FROM flashdata flash "
        "WHERE flash.stroke_time BETWEEN :in_starttime<timestamp,in> AND "
        ":in_endtime<timestamp,in> ";

    if (!locations.empty())
    {
      for (auto tloc : locations)
      {
        if (tloc.loc->type == SmartMet::Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          std::string radius = Fmi::to_string(tloc.loc->radius);
          sqltemplate +=
              " AND SDO_WITHIN_DISTANCE(flash.stroke_location, SDO_GEOMETRY(2001, 8307, "
              "SDO_POINT_TYPE(" +
              lon + ", " + lat + ", NULL), NULL, NULL), 'distance = " + radius +
              " unit = km') = 'TRUE'";
        }
        if (tloc.loc->type == SmartMet::Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          SmartMet::Spine::BoundingBox bbox(bboxString);

          sqltemplate += "AND flash.stroke_location.sdo_point.x BETWEEN " +
                         Fmi::to_string(bbox.xMin) + " AND " + Fmi::to_string(bbox.xMax) +
                         " AND flash.stroke_location.sdo_point.y BETWEEN " +
                         Fmi::to_string(bbox.yMin) + " AND " + Fmi::to_string(bbox.yMax);
        }
      }
    }

    otl_stream stream;

    try
    {
      stream.set_commit(0);
      stream.open(1000, sqltemplate.c_str(), thedb);
      stream << makeOTLTime(starttime) << makeOTLTime(endtime);

      otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream> iterator;
      iterator.attach(stream);

      otl_datetime timestamp;

      while (iterator.next_row())
      {
        if (!iterator.is_null(1))
          iterator.get(1, flashcounts.flashcount);
        if (!iterator.is_null(2))
          iterator.get(2, flashcounts.strokecount);
        if (!iterator.is_null(3))
          iterator.get(3, flashcounts.iccount);
      }

      iterator.detach();
      stream.close();
    }
    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (isFatalError(p.code))
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        reConnect();
        return getFlashCount(starttime, endtime, locations);
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

    return flashcounts;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
