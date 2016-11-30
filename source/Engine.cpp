#include "Engine.h"
#include "Oracle.h"
#include "OracleConnectionPool.h"
#include "FlashUtils.h"
#include "QueryObservableProperty.h"
#include "QueryResult.h"
#include "QueryOpenData.h"

#include <spine/ConfigBase.h>
#include <spine/Convenience.h>
#include <spine/ValueFormatter.h>
#include <spine/Table.h>
#include <spine/Thread.h>
#include <spine/Station.h>
#include <spine/Parameter.h>
#include <spine/TimeSeriesOutput.h>
#include <spine/Exception.h>

#include <macgyver/Geometry.h>
#include <macgyver/String.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread.hpp>
#include <boost/optional.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/filesystem.hpp>

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

// #define MYDEBUG 1

namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// ----------------------------------------------------------------------
/*!
 * \brief Round down the given time to start of day
 */
// ----------------------------------------------------------------------

boost::posix_time::ptime day_start(const boost::posix_time::ptime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  return ptime(t.date(), hours(0));
}

// ----------------------------------------------------------------------
/*!
 * \brief Round up the given time to end of day
 */
// ----------------------------------------------------------------------

boost::posix_time::ptime day_end(const boost::posix_time::ptime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  auto tmp = ptime(t.date(), hours(0));
  tmp += days(1);
  return tmp;
}

Engine::Engine(const std::string& configfile)
    : configFile(configfile), itsDatabaseRegistry(new DBRegistry())
{
  try
  {
    readConfigFile(configFile);
    itsDatabaseRegistry->loadConfigurations(itsDBRegistryFolderPath);
    createSerializedStationsDirectory();

    itsReady = true;

    // Verify multithreading is possible
    if (!sqlite_api::sqlite3_threadsafe())
      throw SmartMet::Spine::Exception(BCP, "Installed sqlite is not thread safe");

    // Switch from serialized to multithreaded access

    int err;

    if (this->threading_mode == "MULTITHREAD")
      err = sqlite_api::sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
    else if (this->threading_mode == "SERIALIZED")
      err = sqlite_api::sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    else
      throw SmartMet::Spine::Exception(BCP,
                                       "Unknown sqlite threading mode: " + this->threading_mode);

    if (err != 0)
      throw SmartMet::Spine::Exception(BCP,
                                       "Failed to set sqlite3 multithread mode to " +
                                           this->threading_mode + ", exit code = " +
                                           Fmi::to_string(err));

    // Enable or disable memory statistics

    err = sqlite_api::sqlite3_config(SQLITE_CONFIG_MEMSTATUS, this->memstatus);
    if (err != 0)
      throw SmartMet::Spine::Exception(
          BCP, "Failed to initialize sqlite3 memstatus mode, exit code " + Fmi::to_string(err));

    // Initialize the caches
    initializeCache();

    // Make one connection so that SOCI can initialize its once-types without
    // worrying about race conditions. This prevents a "database table is locked"
    // warning during startup if the engine is simultaneously bombed with requests.

    SpatiaLite connection(
        itsSpatiaLiteFile, maxInsertSize, synchronous, journal_mode, shared_cache, cache_timeout);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::init()
{
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void Engine::shutdown()
{
  try
  {
    std::cout << "  -- Shutdown requested (Observation)\n";

    itsShutdownRequested = true;

    // Shutting down Oracle connections

    if (itsPool != NULL)
      itsPool->shutdown();

    // Shutting down SpatiaLite connections

    if (itsSpatiaLitePool != NULL)
      itsSpatiaLitePool->shutdown();

    // Waiting active threads to terminate

    while (itsActiveThreadCount > 0)
    {
      boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::createSerializedStationsDirectory()
{
  try
  {
    boost::filesystem::path path = boost::filesystem::path(itsSerializedStationsFile);
    boost::filesystem::path directory = path.parent_path();
    if (!boost::filesystem::is_directory(directory))
    {
      boost::filesystem::create_directories(directory);
      logMessage("Created directory " + directory.string());
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::unserializeStations()
{
  try
  {
    boost::filesystem::path path = boost::filesystem::path(itsSerializedStationsFile);
    if (!boost::filesystem::is_empty(path))
    {
      std::ifstream file(itsSerializedStationsFile);
      boost::archive::xml_iarchive archive(file);
      jss::shared_ptr<StationInfo> stationinfo = jss::make_shared<StationInfo>();
      archive& BOOST_SERIALIZATION_NVP(stationinfo->stations);
      for (const SmartMet::Spine::Station& station : stationinfo->stations)
      {
        stationinfo->index[station.station_id] = station;
      }
      //  This is atomic
      itsStationInfo = stationinfo;
      logMessage("Unserialized stations successfully from " + path.string());
    }
    else
    {
      logMessage("No serialized station file found from " + path.string());
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Unserialization failed!", NULL);
  }
}

void Engine::serializeStations(SmartMet::Spine::Stations& stations)
{
  try
  {
    boost::filesystem::path path = boost::filesystem::path(itsSerializedStationsFile);
    boost::filesystem::path directory = path.parent_path();
    if (!boost::filesystem::is_directory(directory))
    {
      createSerializedStationsDirectory();
    }
    std::ofstream file(itsSerializedStationsFile);
    boost::archive::xml_oarchive archive(file);
    archive& BOOST_SERIALIZATION_NVP(stations);
    logMessage("Serialized station info to " + path.string());
  }
  catch (...)
  {
    SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
    std::cerr << exception.getStackTrace();
  }
}

void Engine::updateFlashCacheFromOracle()
{
  try
  {
    vector<FlashDataItem> flashCacheData;

    boost::shared_ptr<Oracle> db = itsPool->getConnection();

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    boost::posix_time::ptime last_time(spatialitedb->getLatestFlashTime());

    // Making sure that we do not request more data than we actually store into the cache.
    boost::posix_time::ptime min_last_time = boost::posix_time::second_clock::universal_time() -
                                             boost::posix_time::hours(spatialiteFlashCacheDuration);
    if (last_time < min_last_time)
      last_time = min_last_time;

    // Big update every 5 minutes to get delayed observations.
    // Using a static is safe, only 1 thread calls this method

    static int update_count = 0;
    bool long_update = (update_count++ % 5 == 0);

    if (long_update)
      last_time -= minutes(10);
    else
      last_time -= minutes(2);

    if (last_time.is_not_a_date_time())
    {
      last_time = boost::posix_time::second_clock::universal_time() -
                  boost::posix_time::hours(this->spatialiteFlashCacheDuration);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();
      db->readFlashCacheDataFromOracle(flashCacheData, last_time, itsTimeZones);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine read " << flashCacheData.size() << " FLASH observations from last "
                  << (long_update ? "60 minutes" : "5 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();
      spatialitedb->fillFlashDataCache(flashCacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine wrote " << flashCacheData.size() << " FLASH observations from last "
                  << (long_update ? "60 minutes" : "5 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    // Delete too old flashes from the SpatiaLite database
    boost::posix_time::ptime timetokeep =
        last_time - boost::posix_time::hours(this->spatialiteFlashCacheDuration);
    spatialitedb->cleanFlashDataCache(timetokeep);

    // Update the time interval which is available from the SpatiaLite database. Note! Atomic reset
    flash_period = jss::make_shared<boost::posix_time::time_period>(timetokeep, last_time);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Unserialization failed!", NULL);
  }
}

void Engine::updateObservationCacheFromOracle()
{
  try
  {
    if (itsShutdownRequested)
      return;

    vector<DataItem> cacheData;

    boost::shared_ptr<Oracle> db = itsPool->getConnection();

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    boost::posix_time::ptime last_time = spatialitedb->getLatestObservationTime();

    // Making sure that we do not request more data than we actually store into the cache.
    boost::posix_time::ptime min_last_time = boost::posix_time::second_clock::universal_time() -
                                             boost::posix_time::hours(spatialiteCacheDuration);
    if (last_time < min_last_time)
      last_time = min_last_time;

    // Big update every 5 minutes to get delayed observations.
    // Using a static is safe, only 1 thread calls this method

    static int update_count = 0;
    bool long_update = (update_count++ % 10 == 0);

    if (long_update)
      last_time -= hours(3);
    else
      last_time -= minutes(3);

    if (last_time.is_not_a_date_time())
    {
      last_time = boost::posix_time::second_clock::universal_time() - boost::posix_time::hours(24);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();
      db->readCacheDataFromOracle(cacheData, last_time, itsTimeZones);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine read " << cacheData.size() << " FIN observations from last "
                  << (long_update ? "3 hours" : "3 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();
      spatialitedb->fillDataCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine wrote " << cacheData.size() << " FIN observations from last "
                  << (long_update ? "3 hours" : "3 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Update the time interval which is available from the SpatiaLite database
    boost::posix_time::ptime timetokeep =
        last_time - boost::posix_time::hours(this->spatialiteCacheDuration);

    // Delete too old observations from the SpatiaLite database
    spatialitedb->cleanDataCache(timetokeep);

    // Update the time interval which is available from the SpatiaLite observation_data table
    spatialite_period = jss::make_shared<boost::posix_time::time_period>(timetokeep, last_time);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Unserialization failed!", NULL);
  }
}

void Engine::updateWeatherDataQCCacheFromOracle()
{
  try
  {
    if (itsShutdownRequested)
      return;

    vector<WeatherDataQCItem> cacheData;

    boost::shared_ptr<Oracle> db = itsPool->getConnection();

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    boost::posix_time::ptime last_time = spatialitedb->getLatestWeatherDataQCTime();

    // Making sure that we do not request more data than we actually store into the cache.
    boost::posix_time::ptime min_last_time = boost::posix_time::second_clock::universal_time() -
                                             boost::posix_time::hours(spatialiteCacheDuration);
    if (last_time < min_last_time)
      last_time = min_last_time;

    // Big update every 5 minutes to get delayed observations.
    // Using a static is safe, only 1 thread calls this method

    static int update_count = 0;
    bool long_update = (update_count++ % 10 == 0);

    if (long_update)
      last_time -= hours(3);
    else
      last_time -= minutes(10);

    {
      auto begin = std::chrono::high_resolution_clock::now();
      db->readWeatherDataQCFromOracle(cacheData, last_time, itsTimeZones);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine read " << cacheData.size() << " EXT observations from last "
                  << (long_update ? "3 hours" : "10 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      spatialitedb->fillWeatherDataQCCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (timer)
        std::cout << "Engine wrote " << cacheData.size() << " EXT observations from last "
                  << (long_update ? "3 hours" : "10 minutes") << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the SpatiaLite database
    boost::posix_time::ptime timetokeep =
        last_time - boost::posix_time::hours(this->spatialiteCacheDuration);
    spatialitedb->cleanWeatherDataQCCache(timetokeep);

    // Update the time interval which is available from the SpatiaLite QC table. Note: atomic reset!
    qcdata_period = jss::make_shared<boost::posix_time::time_period>(timetokeep, last_time);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Unserialization failed!", NULL);
  }
}

void Engine::cacheFromOracle()
{
  try
  {
    if (!connectionsOK)
    {
      errorLog("cacheFromOracle(): No connection to Oracle");
      return;
    }

    if (itsShutdownRequested)
      return;

    // Updates are disabled for example in regression tests and sometimes when profiling
    if (disableUpdates)
      return;

    logMessage("Loading locations table from CLDB...");

    boost::shared_ptr<Oracle> db = itsPool->getConnection();
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    vector<LocationItem> locations;

    db->readLocationsFromOracle(locations, itsTimeZones);
    spatialitedb->fillLocationCache(locations);

    logMessage("Locations cached to SpatiaLite.");

    logMessage("Loading observation cache from CLDB...");

    updateObservationCacheFromOracle();

    logMessage("Observations cached to SpatiaLite.");

    itsUpdateCacheLoopThread.reset(new boost::thread(
        boost::bind(&SmartMet::Engine::Observation::Engine::updateCacheLoop, this)));
    itsUpdateWeatherDataQCCacheLoopThread.reset(new boost::thread(
        boost::bind(&SmartMet::Engine::Observation::Engine::updateWeatherDataQCCacheLoop, this)));
    itsUpdateFlashCacheLoopThread.reset(new boost::thread(
        boost::bind(&SmartMet::Engine::Observation::Engine::updateFlashCacheLoop, this)));
    itsPreloadStationThread.reset(new boost::thread(
        boost::bind(&SmartMet::Engine::Observation::Engine::preloadStations, this)));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateCacheLoop()
{
  try
  {
    itsActiveThreadCount++;
    while (!itsShutdownRequested)
    {
      try
      {
        updateObservationCacheFromOracle();
      }
      catch (std::exception& err)
      {
        logMessage(std::string("updateObservationCacheFromOracle(): ") + err.what());
      }
      catch (...)
      {
        logMessage("updateObservationCacheFromOracle(): unknown error");
      }

      // Total time to sleep in milliseconds
      int remaining = finUpdateInterval * 1000;
      while (remaining > 0 && !itsShutdownRequested)
      {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateFlashCacheLoop()
{
  try
  {
    itsActiveThreadCount++;
    while (!itsShutdownRequested)
    {
      try
      {
        updateFlashCacheFromOracle();
      }
      catch (std::exception& err)
      {
        logMessage(std::string("updateFlashCacheFromOracle(): ") + err.what());
      }
      catch (...)
      {
        logMessage("updateFlashCacheFromOracle(): unknown error");
      }

      // Total time to sleep in milliseconds
      int remaining = flashUpdateInterval * 1000;
      while (remaining > 0 && !itsShutdownRequested)
      {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateWeatherDataQCCacheLoop()
{
  try
  {
    itsActiveThreadCount++;
    while (!itsShutdownRequested)
    {
      try
      {
        updateWeatherDataQCCacheFromOracle();
      }
      catch (std::exception& err)
      {
        logMessage(std::string("updateWeatherDataQCCacheFromOracle(): ") + err.what());
      }
      catch (...)
      {
        logMessage("updateWeatherDataQCCacheFromOracle(): unknown error");
      }

      // Total time to sleep in milliseconds
      int remaining = extUpdateInterval * 1000;
      while (remaining > 0 && !itsShutdownRequested)
      {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::timeIntervalIsCached(const boost::posix_time::ptime& starttime,
                                  const boost::posix_time::ptime& endtime)
{
  try
  {
    // copies both begin & end, makes resetting the period thread safe
    auto period = spatialite_period.load();

    // the first write sets the available period, must return false until that is done
    if (!period)
      return false;

    // we need only the beginning though
    return (starttime >= period->begin());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::flashIntervalIsCached(const boost::posix_time::ptime& starttime,
                                   const boost::posix_time::ptime& endtime)
{
  try
  {
    // Atomic copy in case we need begin and end
    auto period = flash_period.load();

    // the first write sets the available period, must return false until that is done
    if (!period)
      return false;

    return (starttime >= period->begin());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime& starttime,
                                               const boost::posix_time::ptime& endtime)
{
  try
  {
    // Atomic copy in case we need begin and end
    auto period = qcdata_period.load();

    // the first write sets the available period, must return false until that is done
    if (!period)
      return false;

    return (starttime >= period->begin());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::preloadStations()
{
  try
  {
    // We have no Oracle connections, we cannot update
    if (!connectionsOK)
    {
      errorLog("preloadStations(): No connection to Oracle.");
      return;
    }

    if (itsShutdownRequested)
      return;

    itsActiveThreadCount++;

    if (!itsPreloaded || forceReload)
    {
      logMessage("Preloading stations...");
      boost::shared_ptr<Oracle> db = itsPool->getConnection();

      db->maxDistance = 5000000;
      const string place = "Helsinki";
      const string lang = "fi";

      SmartMet::Spine::LocationPtr loc = geonames->nameSearch(place, lang);

      jss::shared_ptr<StationInfo> newStationInfo = jss::make_shared<StationInfo>();

      // Get all the stations
      db->stationType = "all";
      db->getStation("", newStationInfo->stations, itsTimeZones);

      // Get wmo and lpnn and rwsid identifiers too
      db->translateToWMO(newStationInfo->stations);
      db->translateToLPNN(newStationInfo->stations);
      db->translateToRWSID(newStationInfo->stations);

      for (SmartMet::Spine::Station& station : newStationInfo->stations)
      {
        if (itsShutdownRequested)
        {
          itsActiveThreadCount--;
          return;
        }

        if (station.station_type == "AWS" or station.station_type == "SYNOP" or
            station.station_type == "CLIM" or station.station_type == "AVI")
        {
          station.isFMIStation = true;
        }
        else if (station.station_type == "MAREO")
        {
          station.isMareographStation = true;
        }
        else if (station.station_type == "BUOY")
        {
          station.isBuoyStation = true;
        }
        else if (station.station_type == "RWS" or station.station_type == "EXTRWS")
        {
          station.isRoadStation = true;
        }
        else if (station.station_type == "EXTWATER")
        {
          station.isSYKEStation = true;
        }
        else if (station.station_type == "EXTSYNOP")
        {
          station.isForeignStation = true;
        }

        if (itsShutdownRequested)
          throw SmartMet::Spine::Exception(
              BCP, "Engine: Aborting station preload due to shutdown request");
        db->addInfoToStation(station, station.latitude_out, station.longitude_out);

        newStationInfo->index[station.fmisid] = station;
      }

      // Serialize stations to disk and swap
      // the contents into itsPreloadedStations
      serializeStations(newStationInfo->stations);

      // Update stations to SpatiaLite database
      logMessage("Updating stations to SpatiaLite databases...");
      boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
      spatialitedb->updateStationsAndGroups(newStationInfo->stations);

      // Note: This is atomic
      itsStationInfo = newStationInfo;

      // Doesn't really matter that these aren't atomic
      itsPreloaded = true;
      itsReady = true;
      forceReload = false;

      logMessage("Preloading stations done.");
    }
  }
  catch (...)
  {
    SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
    std::cout << exception.getStackTrace();
  }
  itsActiveThreadCount--;
}

bool Engine::stationExistsInTimeRange(const SmartMet::Spine::Station& station,
                                      const boost::posix_time::ptime& starttime,
                                      const boost::posix_time::ptime& endtime)
{
  try
  {
    // No up-to-date existence info for these station types
    if (station.isRoadStation || station.isSYKEStation)
    {
      return true;
    }
    if ((starttime < station.station_end && endtime > station.station_end) ||
        (starttime < station.station_end && !station.station_end.is_not_a_date_time()) ||
        (endtime > station.station_start && starttime < station.station_start) ||
        (starttime > station.station_start && endtime < station.station_start))
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

bool Engine::stationHasRightType(const SmartMet::Spine::Station& station, const Settings& settings)
{
  try
  {
    if ((settings.stationtype == "fmi" || settings.stationtype == "opendata" ||
         settings.stationtype == "opendata_minute" || settings.stationtype == "opendata_daily" ||
         settings.stationtype == "daily" || settings.stationtype == "hourly" ||
         settings.stationtype == "monthly" || settings.stationtype == "lammitystarve" ||
         settings.stationtype == "solar" || settings.stationtype == "minute_rad") &&
        station.isFMIStation)
    {
      return true;
    }
    else if (settings.stationtype == "foreign")
    {
      return true;
    }
    else if (settings.stationtype == "road" && station.isRoadStation)
    {
      return true;
    }
    else if ((settings.stationtype == "mareograph" ||
              settings.stationtype == "opendata_mareograph") &&
             station.isMareographStation)
    {
      return true;
    }
    else if ((settings.stationtype == "buoy" || settings.stationtype == "opendata_buoy") &&
             station.isBuoyStation)
    {
      return true;
    }
    else if (settings.stationtype == "syke" && station.isSYKEStation)
    {
      return true;
    }
    else if (settings.stationtype == "MAST")
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

void Engine::getStations(SmartMet::Spine::Stations& stations, Settings& settings)
{
  try
  {
    boost::shared_ptr<Oracle> db = itsPool->getConnection();
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    getStations(settings, stations, *db, spatialitedb);
    stations = removeDuplicateStations(stations);
  }
  catch (...)
  {
    SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
    errorLog(exception.what());
    throw exception;
  }
}

SmartMet::Spine::Stations Engine::getStationsByArea(const Settings& settings, const string& areaWkt)
{
  try
  {
    SmartMet::Spine::Stations stations;
    Settings tempSettings = settings;
    try
    {
      auto stationgroupCodeSet =
          itsStationtypeConfig.getGroupCodeSetByStationtype(tempSettings.stationtype);
      tempSettings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                             stationgroupCodeSet->end());
    }
    catch (...)
    {
      return stations;
    }

    if (areaWkt.empty())
      return stations;

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    try
    {
      auto info = itsStationInfo.load();
      return spatialitedb->findStationsInsideArea(settings, areaWkt, info->index);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    return stations;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::getStationsByBoundingBox(SmartMet::Spine::Stations& stations, const Settings& settings)
{
  try
  {
    Settings tempSettings = settings;
    try
    {
      auto stationgroupCodeSet =
          itsStationtypeConfig.getGroupCodeSetByStationtype(tempSettings.stationtype);
      tempSettings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                             stationgroupCodeSet->end());
    }
    catch (...)
    {
      return;
    }

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    try
    {
      auto info = itsStationInfo.load();
      SmartMet::Spine::Stations stationList =
          spatialitedb->findStationsInsideBox(settings, info->index);
      for (const SmartMet::Spine::Station& station : stationList)
        stations.push_back(station);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::getStationsByRadius(SmartMet::Spine::Stations& stations,
                                 const Settings& settings,
                                 double longitude,
                                 double latitude)
{
  try
  {
    // Copy original data atomically so that a reload may simultaneously swap
    auto info = itsStationInfo.load();

    // Now we can safely use "outdated" data even though the stations may have simultaneously been
    // updated

    for (const SmartMet::Spine::Station& station : info->stations)
    {
      if (stationHasRightType(station, settings))
      {
        double distance = Fmi::Geometry::GeoDistance(
            longitude, latitude, station.longitude_out, station.latitude_out);
        if (distance < settings.maxdistance)
        {
          stations.push_back(station);
        }
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::stationIsInBoundingBox(const SmartMet::Spine::Station& station,
                                    map<string, double> boundingBox)
{
  try
  {
    if (station.latitude_out <= boundingBox["maxy"] &&
        station.latitude_out >= boundingBox["miny"] &&
        station.longitude_out <= boundingBox["maxx"] &&
        station.longitude_out >= boundingBox["minx"])
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

void Engine::logMessage(const string& message)
{
  try
  {
    if (!quiet)
      cout << SmartMet::Spine::log_time_str() << " [Engine] " << message << endl;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::errorLog(const string& message)
{
  try
  {
    cerr << SmartMet::Spine::log_time_str() << " [Engine] Error: " << message << endl;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::initializePool(int poolSize)
{
  try
  {
    logMessage("Initializing connection pool...");
    itsPool = new OracleConnectionPool(
        geonames, this->service, this->username, this->password, this->nls_lang, itsPoolSize);
    itsPool->setGetConnectionTimeOutSeconds(
        this->itsOracleConnectionPoolGetConnectionTimeOutSeconds);

    if (itsPool->initializePool(itsPoolSize))
    {
      connectionsOK = true;
    }

    itsSpatiaLitePool = new SpatiaLiteConnectionPool(itsSpatiaLitePoolSize,
                                                     itsSpatiaLiteFile,
                                                     maxInsertSize,
                                                     synchronous,
                                                     journal_mode,
                                                     shared_cache,
                                                     cache_timeout);

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
    spatialitedb->createTables();

    boost::posix_time::ptime last_time(spatialitedb->getLatestObservationTime());
    boost::posix_time::ptime timetokeep =
        last_time - boost::posix_time::hours(this->spatialiteCacheDuration);

    // Resets the period atomically
    spatialite_period = jss::make_shared<boost::posix_time::time_period>(timetokeep, last_time);

    // Check first if we already have stations in SpatiaLite db so that we know if we can use it
    // before loading station info
    size_t stationCount = spatialitedb->getStationCount();
    if (stationCount >
        1)  // Arbitrary number because we cannot know how many stations there must be
      itsSpatiaLiteHasStations = true;

    for (int i = 0; i < this->itsSpatiaLitePoolSize; i++)
    {
      boost::shared_ptr<SpatiaLite> db = itsSpatiaLitePool->getConnection();
    }

    logMessage("Connection pool ready.");
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::initializeCache()
{
  try
  {
    if (this->stationCacheSize)
    {
      stationCache.resize(boost::numeric_cast<std::size_t>(this->stationCacheSize));
    }
    else
    {
      stationCache.resize(1000);
    }
    if (this->boundingBoxCacheSize)
    {
      boundingBoxCache.resize(boost::numeric_cast<std::size_t>(this->boundingBoxCacheSize));
    }
    else
    {
      boundingBoxCache.resize(1000);
    }
#ifdef ENABLE_TABLE_CACHE
    if (this->resultCacheSize)
    {
      resultCache.resize(boost::numeric_cast<std::size_t>(this->resultCacheSize));
    }
    else
    {
      resultCache.resize(1000);
    }
#endif
    if (this->locationCacheSize)
    {
      locationCache.resize(boost::numeric_cast<std::size_t>(this->locationCacheSize));
    }
    else
    {
      locationCache.resize(1000);
    }

    itsQueryResultBaseCache.resize(itsQueryResultBaseCacheSize);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::ready() const
{
  return itsReady;
}

void Engine::setGeonames(SmartMet::Engine::Geonames::Engine* geonames_)
{
  try
  {
    boost::mutex::scoped_lock lock(ge_mutex);
    if (geonames == NULL)
    {
      geonames = geonames_;

      // Connection pool can be initialized only afer geonames is set
      initializePool(itsPoolSize);

      // Read itsPreloadedStations from disk if available
      unserializeStations();

      // boost::thread
      // initializeThread(boost::bind(&SmartMet::Engine::Observation::Engine::preloadStations,
      // this));
      if (not disableUpdates)
      {
        itsActiveThreadCount++;
        cacheFromOracle();
        itsActiveThreadCount--;
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::setBoundingBoxIsGiven(bool value, Oracle& db, Settings& settings)
{
  try
  {
    db.setBoundingBoxIsGiven(value);
    settings.boundingBoxIsGiven = value;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::setSettings(Settings& settings, Oracle& db)
{
  try
  {
    db.timeZone = settings.timezone;
    db.stationType = settings.stationtype;
    db.maxDistance = settings.maxdistance;
    db.allPlaces = settings.allplaces;
    db.latest = settings.latest;

    ptime startTime = second_clock::universal_time() - boost::posix_time::hours(24);
    ptime endTime = second_clock::universal_time();
    int timeStep = 1;
    if (!settings.starttime.is_not_a_date_time())
    {
      startTime = settings.starttime;
    }
    if (!settings.endtime.is_not_a_date_time())
    {
      endTime = settings.endtime;
    }
    if (settings.timestep >= 0)
    {
      timeStep = settings.timestep;
    }

    db.setTimeInterval(startTime, endTime, timeStep);

    if (!settings.timeformat.empty())
    {
      db.timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));
    }
    else
    {
      db.timeFormatter.reset(Fmi::TimeFormatter::create(db.timeFormat));
    }

    db.hours = settings.hours;
    db.weekdays = settings.weekdays;
    if (db.language.length() > 0)
    {
      db.language = settings.language;
    }
    else
    {
      settings.language = "fi";
      db.language = "fi";
    }

    db.missingText = settings.missingtext;
    db.locale = std::locale(settings.localename.c_str());
    db.numberOfStations = settings.numberofstations;
    db.parameterMap = this->parameterMap;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

FlashCounts Engine::getFlashCount(const boost::posix_time::ptime& starttime,
                                  const boost::posix_time::ptime& endtime,
                                  const SmartMet::Spine::TaggedLocationList& locations)
{
  try
  {
    Settings settings;
    settings.stationtype = "flash";

    if (flashIntervalIsCached(starttime, endtime))
    {
      boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
      return spatialitedb->getFlashCount(starttime, endtime, locations);
    }
    else
    {
      boost::shared_ptr<Oracle> oracledb = itsPool->getConnection();
      return oracledb->getFlashCount(starttime, endtime, locations);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<vector<ObservableProperty> > Engine::observablePropertyQuery(
    vector<string>& parameters, const string language)
{
  try
  {
    boost::shared_ptr<SmartMet::Spine::ValueFormatter> valueFormatter;
    boost::shared_ptr<Oracle> db = itsPool->getConnection();

    QueryObservableProperty qop;

    db->stationType = "metadata";

    db->parameterMap = this->parameterMap;

    boost::shared_ptr<vector<ObservableProperty> > data;

    try
    {
      data = qop.executeQuery(*db, parameters, language);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    return data;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations Engine::getStationsByTaggedLocations(
    const SmartMet::Spine::TaggedLocationList& taggedLocations,
    boost::shared_ptr<SpatiaLite> spatialitedb,
    const int numberofstations,
    const string& stationtype,
    const int maxdistance,
    const std::set<std::string>& stationgroup_codes,
    const boost::posix_time::ptime& starttime,
    const boost::posix_time::ptime& endtime)
{
  try
  {
    SmartMet::Spine::Stations stations;

    auto stationstarttime = day_start(starttime);
    auto stationendtime = day_end(endtime);

    for (const SmartMet::Spine::TaggedLocation& tloc : taggedLocations)
    {
      // BUG? Why is maxdistance int?
      string locationCacheKey = getLocationCacheKey(tloc.loc->geoid,
                                                    numberofstations,
                                                    stationtype,
                                                    maxdistance,
                                                    stationstarttime,
                                                    stationendtime);
      auto cachedStations = locationCache.find(locationCacheKey);

      if (cachedStations)
      {
        for (SmartMet::Spine::Station& cachedStation : *cachedStations)
        {
          cachedStation.tag = tloc.tag;
          stations.push_back(cachedStation);
        }
      }
      else
      {
        auto info = itsStationInfo.load();
        auto newStations = spatialitedb->findNearestStations(tloc.loc,
                                                             info->index,
                                                             maxdistance,
                                                             numberofstations,
                                                             stationgroup_codes,
                                                             stationstarttime,
                                                             stationendtime);

        if (!newStations.empty())
        {
          for (SmartMet::Spine::Station& s : newStations)
          {
            s.tag = tloc.tag;
            stations.push_back(s);
          }
          locationCache.insert(locationCacheKey, newStations);
        }
      }
    }
    return stations;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::getStations(Settings& settings,
                         SmartMet::Spine::Stations& stations,
                         Oracle& db,
                         boost::shared_ptr<SpatiaLite> spatialitedb)
{
  try
  {
    try
    {
      // Convert the stationtype in th setting to station group codes. SpatiaLite station search is
      // using the codes.
      auto stationgroupCodeSet =
          itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
      settings.stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());
    }
    catch (...)
    {
      return;
    }

    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

    // Reset boundingBoxIsGiven boolean, because Oracle instances are never
    // destructed and using bounding box once would result that the boolean would be set always in
    // the
    // future
    setBoundingBoxIsGiven(false, db, settings);

#ifdef MYDEBUG
    cout << "station search start" << endl;
#endif
    // Get all stations by different methods

    // 1) get all places for given station type or
    // get nearest stations by named locations (i.e. by its coordinates)
    // We are also getting all stations for a stationtype, don't bother to continue with other means
    // to find stations.

    auto info = itsStationInfo.load();

    if (settings.allplaces)
    {
      SmartMet::Spine::Stations allStationsFromGroups = spatialitedb->findAllStationsFromGroups(
          settings.stationgroup_codes, info->index, stationstarttime, stationendtime);

      stations = allStationsFromGroups;
      return;
    }
    else
    {
      auto taggedStations = getStationsByTaggedLocations(settings.taggedLocations,
                                                         spatialitedb,
                                                         settings.numberofstations,
                                                         settings.stationtype,
                                                         settings.maxdistance,
                                                         settings.stationgroup_codes,
                                                         settings.starttime,
                                                         settings.endtime);
      for (const auto& s : taggedStations)
      {
        stations.push_back(s);
      }

      // TODO: Remove this legacy support for Locations when obsplugin is deprecated
      if (!settings.locations.empty())
      {
        for (const auto& loc : settings.locations)
        {
          // BUG? Why is maxdistance int?
          string locationCacheKey =
              getLocationCacheKey(loc->geoid,
                                  settings.numberofstations,
                                  settings.stationtype,
                                  boost::numeric_cast<int>(settings.maxdistance),
                                  stationstarttime,
                                  stationendtime);
          auto cachedStations = locationCache.find(locationCacheKey);

          if (cachedStations)
          {
            for (SmartMet::Spine::Station& cachedStation : *cachedStations)
            {
              cachedStation.tag = settings.missingtext;
              stations.push_back(cachedStation);
            }
          }
          else
          {
            SmartMet::Spine::Stations newStations;

            newStations = spatialitedb->findNearestStations(loc,
                                                            info->index,
                                                            settings.maxdistance,
                                                            settings.numberofstations,
                                                            settings.stationgroup_codes,
                                                            stationstarttime,
                                                            stationendtime);

            if (!newStations.empty())
            {
              for (SmartMet::Spine::Station& newStation : newStations)
              {
                newStation.tag = settings.missingtext;
                stations.push_back(newStation);
              }
              locationCache.insert(locationCacheKey, newStations);
            }
          }
        }
      }

      for (const auto& coordinate : settings.coordinates)
      {
        SmartMet::Spine::Stations newStations;

        newStations = spatialitedb->findNearestStations(coordinate.at("lat"),
                                                        coordinate.at("lon"),
                                                        info->index,
                                                        settings.maxdistance,
                                                        settings.numberofstations,
                                                        settings.stationgroup_codes,
                                                        stationstarttime,
                                                        stationendtime);

        if (!newStations.empty())
        {
          for (const SmartMet::Spine::Station& newStation : newStations)
          {
            stations.push_back(newStation);
          }
        }
      }
    }

    std::vector<int> fmisid_collection;

    // 2) Get stations by WMO or RWSID identifier
    if (!settings.wmos.empty())
    {
      vector<int> fmisids;
      if (settings.stationtype == "road")
      {
        fmisids = db.translateRWSIDToFMISID(settings.wmos);
      }
      else
      {
        fmisids = db.translateWMOToFMISID(settings.wmos);
      }
      for (int fmisid : fmisids)
        fmisid_collection.push_back(fmisid);
    }

    // 3) Get stations by LPNN number
    if (!settings.lpnns.empty())
    {
      vector<int> fmisids = db.translateLPNNToFMISID(settings.lpnns);
      for (int fmisid : fmisids)
        fmisid_collection.push_back(fmisid);
    }

    // 4) Get stations by FMISID number
    for (int fmisid : settings.fmisids)
    {
      SmartMet::Spine::Station s;
      if (spatialitedb->getStationById(s, fmisid, settings.stationgroup_codes))
      {  // Chenking that some station group match.
        fmisid_collection.push_back(s.station_id);
      }
    }

    // Find station data by using fmisid
    std::vector<SmartMet::Spine::Station> station_collection;
    for (int fmisid : fmisid_collection)
    {
      // Search the requested station.
      boost::optional<SmartMet::Spine::Station> station = stationCache.find(fmisid);
      if (station)
      {
        stations.push_back(station.get());
        station_collection.push_back(station.get());
      }
      else
      {
        SmartMet::Spine::Station s;

        if (not spatialitedb->getStationById(s, fmisid, settings.stationgroup_codes))
          continue;

        stations.push_back(s);
        station_collection.push_back(s);
        stationCache.insert(s.fmisid, s);
      }
    }

    // Find the nearest stations of the requested station(s) if wanted.
    // Default value for settings.numberofstations is 1.
    if (settings.numberofstations > 1)
    {
      for (const SmartMet::Spine::Station& s : station_collection)
      {
        auto newStations = spatialitedb->findNearestStations(s.latitude_out,
                                                             s.longitude_out,
                                                             info->index,
                                                             settings.maxdistance,
                                                             settings.numberofstations,
                                                             settings.stationgroup_codes,
                                                             stationstarttime,
                                                             stationendtime);

        for (const SmartMet::Spine::Station& nstation : newStations)
          stations.push_back(nstation);
      }
    }

    // 5) Get stations by geoid

    if (!settings.geoids.empty())
    {
      // SpatiaLite
      for (int geoid : settings.geoids)
      {
        Locus::QueryOptions opts;
        opts.SetLanguage(settings.language);
        opts.SetResultLimit(1);
        opts.SetCountries("");
        opts.SetFullCountrySearch(true);

        auto places = geonames->idSearch(opts, geoid);
        if (!places.empty())
        {
          for (const auto& place : places)
          {
            auto newStations = spatialitedb->findNearestStations(place,
                                                                 info->index,
                                                                 settings.maxdistance,
                                                                 settings.numberofstations,
                                                                 settings.stationgroup_codes,
                                                                 stationstarttime,
                                                                 stationendtime);

            if (!newStations.empty())
            {
              newStations.front().geoid = geoid;
              for (SmartMet::Spine::Station& s : newStations)
              {
                // Set tag to be the requested geoid
                s.tag = Fmi::to_string(geoid);
                stations.push_back(s);
              }
            }
          }
        }
      }
    }

    // 6) Get stations if bounding box is given

    if (!settings.boundingBox.empty())
    {
      setBoundingBoxIsGiven(true, db, settings);

      // boost::lock_guard<boost::mutex> lock(itsBoundingBoxSearchMutex);
      getStationsByBoundingBox(stations, settings);
    }

    // 7) For output purposes, translate the station id to WMO only if needed.
    // LPNN conversion is done anyway, because the data is sorted by LPNN number
    // in data views. For road weather or mareograph stations this is not applicable, because
    // there is no LPNN or WMO numbers for them
    if (settings.stationtype != "road" && settings.stationtype != "mareograph")
    {
      for (const SmartMet::Spine::Parameter& p : settings.parameters)
      {
        if (p.name() == "wmo")
        {
          db.translateToWMO(stations);
          break;
        }
      }
    }

    // 8) For road stations, translate FMISID to RWSID if requested
    if (settings.stationtype == "road")
    {
      for (const SmartMet::Spine::Parameter& p : settings.parameters)
      {
        if (p.name() == "rwsid")
        {
          db.translateToRWSID(stations);
          break;
        }
      }
    }

#ifdef MYDEBUG
    cout << "total number of stations: " << stations.size() << endl;
    cout << "station search end" << endl;
    cout << "observation query start" << endl;
#endif
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Engine::getLocationCacheKey(int geoID,
                                   int numberOfStations,
                                   string stationType,
                                   int maxDistance,
                                   const boost::posix_time::ptime& starttime,
                                   const boost::posix_time::ptime& endtime)
{
  try
  {
    string locationCacheKey = "";

    locationCacheKey += Fmi::to_string(geoID);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_string(numberOfStations);
    locationCacheKey += "-";
    locationCacheKey += stationType;
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_string(maxDistance);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_iso_string(starttime);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_iso_string(endtime);
    return locationCacheKey;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Engine::getBoundingBoxCacheKey(const Settings& settings)
{
  try
  {
    string boundingBoxCacheKey = "";

    boundingBoxCacheKey += settings.stationtype;

    boundingBoxCacheKey += Fmi::to_string(settings.boundingBox.at("minx"));
    boundingBoxCacheKey += Fmi::to_string(settings.boundingBox.at("maxx"));
    boundingBoxCacheKey += Fmi::to_string(settings.boundingBox.at("miny"));
    boundingBoxCacheKey += Fmi::to_string(settings.boundingBox.at("maxy"));

    return boundingBoxCacheKey;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string Engine::getCacheKey(const Settings& settings)
{
  try
  {
    string cacheKey = "";

    cacheKey += settings.stationtype + "|";
    cacheKey += makeStringTime(makeOTLTime(settings.starttime)) + "|";
    cacheKey += makeStringTime(makeOTLTime(settings.endtime)) + "|";
    cacheKey += Fmi::to_string(settings.numberofstations) + "|";
    cacheKey += settings.localename + "|";
    cacheKey += Fmi::to_string(settings.maxdistance) + "|";
    cacheKey += settings.missingtext + "|";
    cacheKey += settings.timeformat + "|";
    cacheKey += Fmi::to_string(settings.timestep) + "|";
    cacheKey += settings.timestring + "|";
    cacheKey += settings.timezone + "|";
    cacheKey += settings.language + "|";

    if (!settings.boundingBox.empty())
    {
      cacheKey += Fmi::to_string(settings.boundingBox.at("minx")) + "|";
      cacheKey += Fmi::to_string(settings.boundingBox.at("maxx")) + "|";
      cacheKey += Fmi::to_string(settings.boundingBox.at("miny")) + "|";
      cacheKey += Fmi::to_string(settings.boundingBox.at("maxy")) + "|";
    }

    if (!settings.fmisids.empty())
    {
      for (int fmisid : settings.fmisids)
      {
        cacheKey += Fmi::to_string(fmisid) + "|";
      }
    }

    if (!settings.parameters.empty())
    {
      for (const SmartMet::Spine::Parameter& parameter : settings.parameters)
      {
        cacheKey += parameter.name() + "|";
      }
    }

    if (!settings.weekdays.empty())
    {
      for (int weekday : settings.weekdays)
      {
        cacheKey += Fmi::to_string(weekday) + "|";
      }
    }

    if (!settings.hours.empty())
    {
      for (int hour : settings.hours)
      {
        cacheKey += Fmi::to_string(hour) + "|";
      }
    }

    if (!settings.wmos.empty())
    {
      for (int wmo : settings.wmos)
      {
        cacheKey += Fmi::to_string(wmo) + "|";
      }
    }

    if (!settings.lpnns.empty())
    {
      for (int lpnn : settings.lpnns)
      {
        cacheKey += Fmi::to_string(lpnn) + "|";
      }
    }

    if (!settings.geoids.empty())
    {
      for (int geoid : settings.geoids)
      {
        cacheKey += Fmi::to_string(geoid) + "|";
      }
    }

    if (!settings.locations.empty())
    {
      for (const SmartMet::Spine::LocationPtr& location : settings.locations)
      {
        cacheKey += Fmi::to_string(location->latitude) + "|";
        cacheKey += Fmi::to_string(location->longitude) + "|";
      }
    }

    if (!settings.coordinates.empty())
    {
      for (const auto& coordinate : settings.coordinates)
      {
        cacheKey += Fmi::to_string(coordinate.at("lat")) + "|";
        cacheKey += Fmi::to_string(coordinate.at("lon")) + "|";
      }
    }

    return cacheKey;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void printSettings(const Settings& settings, Oracle& oracle)
{
  try
  {
    cout << settings.stationtype << endl;
    cout << oracle.makeStringTime(oracle.makeOTLTime(settings.starttime)) << endl;
    cout << oracle.makeStringTime(oracle.makeOTLTime(settings.endtime)) << endl;
    cout << Fmi::to_string(settings.numberofstations) << endl;
    cout << settings.localename << endl;
    cout << Fmi::to_string(settings.maxdistance) << endl;
    cout << settings.missingtext << endl;
    cout << settings.timeformat << endl;
    cout << Fmi::to_string(settings.timestep) << endl;
    cout << settings.timestring << endl;
    cout << settings.timezone << endl;

    if (!settings.boundingBox.empty())
    {
      cout << Fmi::to_string(settings.boundingBox.at("minx")) << endl;
      cout << Fmi::to_string(settings.boundingBox.at("maxx")) << endl;
      cout << Fmi::to_string(settings.boundingBox.at("miny")) << endl;
      cout << Fmi::to_string(settings.boundingBox.at("maxy")) << endl;
    }

    if (!settings.fmisids.empty())
    {
      for (int fmisid : settings.fmisids)
      {
        cout << Fmi::to_string(fmisid) << endl;
      }
    }

    if (!settings.parameters.empty())
    {
      for (const SmartMet::Spine::Parameter& parameter : settings.parameters)
      {
        cout << parameter.name() << endl;
      }
    }

    if (!settings.weekdays.empty())
    {
      for (int weekday : settings.weekdays)
      {
        cout << Fmi::to_string(weekday) << endl;
      }
    }

    if (!settings.wmos.empty())
    {
      for (int wmo : settings.wmos)
      {
        cout << Fmi::to_string(wmo) << endl;
      }
    }

    if (!settings.lpnns.empty())
    {
      for (int lpnn : settings.lpnns)
      {
        cout << Fmi::to_string(lpnn) << endl;
      }
    }

    if (!settings.geoids.empty())
    {
      for (int geoid : settings.geoids)
      {
        cout << Fmi::to_string(geoid) << endl;
      }
    }

    if (!settings.locations.empty())
    {
      for (const SmartMet::Spine::LocationPtr& location : settings.locations)
      {
        cout << Fmi::to_string(location->latitude) << endl;
        cout << Fmi::to_string(location->longitude) << endl;
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::reloadStations()
{
  try
  {
    forceReload = true;
    boost::thread initializeThread(
        boost::bind(&SmartMet::Engine::Observation::Engine::preloadStations, this));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SmartMet::Spine::Table> Engine::makeQuery(
    Settings& settings, boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter)
{
  try
  {
    boost::shared_ptr<SmartMet::Spine::Table> data;

    if (!connectionsOK)
    {
      errorLog("[Observation] No connections to Oracle database!");
      return data;
    }

#ifdef ENABLE_TABLE_CACHE
// Try cache first
// TODO DISABLE CACHE WHILE TESTING
/*
auto cacheKey = getCacheKey(settings);
auto cacheResult = resultCache.find(cacheKey);

if(cacheResult)
      {
        return *cacheResult;
      }

*/

#endif

    boost::shared_ptr<Oracle> db = itsPool->getConnection();

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    // Not fully functional yet
    /*
    if(settings.stationtype == "reload")
          {
            if(reloadStations())
                  {
                    logMessage("RELOAD OK");
                  }
          }
    */

    if (settings.stationtype == "flash")
    {
      FlashUtils flashUtils;
      try
      {
        data = flashUtils.getData(*db, settings, valueFormatter, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        throw exception;
      }

#ifdef ENABLE_TABLE_CACHE
      resultCache.insert(getCacheKey(settings), data);
#endif

      return data;
    }

    setSettings(settings, *db);

    // This contains all stations relevant to each request type.
    SmartMet::Spine::Stations stations;

    try
    {
      getStations(settings, stations, *db, spatialitedb);
      stations = removeDuplicateStations(stations);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    // Return empty data if there are no stations close enough
    if (stations.empty())
    {
      data.reset(new SmartMet::Spine::Table);
      return data;
    }

    if (itsStationtypeConfig.getUseCommonQueryMethod(settings.stationtype))
    {
      QueryOpenData opendata;

      try
      {
        if (settings.producer_ids.empty())
          settings.producer_ids =
              *itsStationtypeConfig.getProducerIdSetByStationtype(settings.stationtype);

        db->setDatabaseTableName(
            *itsStationtypeConfig.getDatabaseTableNameByStationtype(settings.stationtype));
        data = opendata.executeQuery(*db, stations, settings, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        db->setDatabaseTableName("");
        throw exception;
      }

#ifdef ENABLE_TABLE_CACHE
      resultCache.insert(getCacheKey(settings), data);
#endif
      db->setDatabaseTableName("");

      return data;
    }

#if 0
    // If there are no stations in station list, throw an exception
    if(stations.empty())
    {
        throw SmartMet::Spine::Exception(BCP,"Engine: No stations found for any place identifier in query " + Fmi::to_string(theRequest.getOriginalQueryString()));
    }
#endif

    try
    {
      // Road, foreign and mareograph stations use FMISID numbers, so LPNN translation is not
      // needed.
      // They also use same cldb table, so all observation types can be fetched with the same
      // method.
      if (settings.stationtype == "road" || settings.stationtype == "foreign" ||
          settings.stationtype == "elering" || settings.stationtype == "mareograph" ||
          settings.stationtype == "buoy")
      {
        data = db->getWeatherDataQCObservations(settings.parameters, stations, itsTimeZones);
      }

      // Stations maintained by FMI
      else if (settings.stationtype == "fmi")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        data = db->getFMIObservations(settings.parameters, stations, itsTimeZones);
      }
      // Stations which measure solar radiation settings.parameters
      else if (settings.stationtype == "solar")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        data = db->getSolarObservations(settings.parameters, stations, itsTimeZones);
      }
      else if (settings.stationtype == "minute_rad")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        data = db->getMinuteRadiationObservations(settings.parameters, stations, itsTimeZones);
      }

      // Hourly data
      else if (settings.stationtype == "hourly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        data = db->getHourlyFMIObservations(settings.parameters, stations, itsTimeZones);
      }
      // Sounding data
      else if (settings.stationtype == "sounding")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        data = db->getSoundings(settings.parameters, stations, itsTimeZones);
      }
      // Daily data
      else if (settings.stationtype == "daily")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        string type = "daily";
        data =
            db->getDailyAndMonthlyObservations(settings.parameters, stations, type, itsTimeZones);
      }
      else if (settings.stationtype == "lammitystarve")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        string type = "lammitystarve";
        data =
            db->getDailyAndMonthlyObservations(settings.parameters, stations, type, itsTimeZones);
      }
      else if (settings.stationtype == "monthly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        string type = "monthly";
        data =
            db->getDailyAndMonthlyObservations(settings.parameters, stations, type, itsTimeZones);
      }
      else
      {
        throw SmartMet::Spine::Exception(BCP,
                                         "Engine: invalid stationtype: " + settings.stationtype);
      }
    }

    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

#ifdef MYDEBUG
    cout << "observation query end" << endl;
#endif

#ifdef ENABLE_TABLE_CACHE
    // Cache results
    resultCache.insert(getCacheKey(settings), data);
#endif

    // Return Table to obsplugin which outputs it
    return data;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::dataAvailableInSpatiaLite(const Settings& settings)
{
  try
  {
    // If stationtype is cached and if we have requested time interval in SpatiaLite, get all data
    // from there
    if (settings.stationtype == "opendata" || settings.stationtype == "fmi" ||
        settings.stationtype == "opendata_mareograph" || settings.stationtype == "opendata_buoy" ||
        settings.stationtype == "research" || settings.stationtype == "syke")
    {
      if (timeIntervalIsCached(settings.starttime, settings.endtime))
      {
        return true;
      }
    }
    else if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
             timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
    {
      return true;
    }
    else if (settings.stationtype == "flash" &&
             flashIntervalIsCached(settings.starttime, settings.endtime))
      return true;

    // Either the stationtype is not cached or the requested time interval is not cached
    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr Engine::flashValuesFromSpatiaLite(
    Settings& settings)
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
    ret = spatialitedb->getCachedFlashData(settings, parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Get locations and data from SpatiaLite, do not use Oracle at all
// FMISID is used as main id in SpatiaLite tables

ts::TimeSeriesVectorPtr Engine::valuesFromSpatiaLite(Settings& settings)
{
  try
  {
    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
    SmartMet::Spine::Stations stations = getStationsFromSpatiaLite(settings, spatialitedb);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, parameterMap, itsTimeZones);
        return ret;
      }

      ret = spatialitedb->getCachedData(stations, settings, parameterMap, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations Engine::getStationsFromSpatiaLite(
    Settings& settings, boost::shared_ptr<SpatiaLite> spatialitedb)
{
  try
  {
    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

    SmartMet::Spine::Stations stations;

    try
    {
      auto stationgroupCodeSet =
          itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
      settings.stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());
    }
    catch (...)
    {
      return stations;
    }

    auto info = itsStationInfo.load();

    if (settings.allplaces)
    {
      SmartMet::Spine::Stations allStationsFromGroups = spatialitedb->findAllStationsFromGroups(
          settings.stationgroup_codes, info->index, settings.starttime, settings.starttime);
      return allStationsFromGroups;
    }

    SmartMet::Spine::Stations tmpIdStations;

    auto taggedStations = getStationsByTaggedLocations(settings.taggedLocations,
                                                       spatialitedb,
                                                       settings.numberofstations,
                                                       settings.stationtype,
                                                       settings.maxdistance,
                                                       settings.stationgroup_codes,
                                                       settings.starttime,
                                                       settings.endtime);

    for (const auto& s : taggedStations)
    {
      stations.push_back(s);
    }

    for (const SmartMet::Spine::LocationPtr& location : settings.locations)
    {
      string locationCacheKey = getLocationCacheKey(location->geoid,
                                                    settings.numberofstations,
                                                    settings.stationtype,
                                                    boost::numeric_cast<int>(settings.maxdistance),
                                                    stationstarttime,
                                                    stationendtime);
      auto cachedStations = locationCache.find(locationCacheKey);
      if (cachedStations)
      {
        for (const SmartMet::Spine::Station& cachedStation : *cachedStations)
        {
          stations.push_back(cachedStation);
        }
      }
      else
      {
        auto newStations = spatialitedb->findNearestStations(location,
                                                             info->index,
                                                             settings.maxdistance,
                                                             settings.numberofstations,
                                                             settings.stationgroup_codes,
                                                             stationstarttime,
                                                             stationendtime);

        if (!newStations.empty())
        {
          for (const SmartMet::Spine::Station& newStation : newStations)
          {
            stations.push_back(newStation);
          }
          locationCache.insert(locationCacheKey, newStations);
        }
      }
    }

    // Find station data by using fmisid
    for (int fmisid : settings.fmisids)
    {
      // Search the requested station.
      boost::optional<SmartMet::Spine::Station> station = stationCache.find(fmisid);
      if (station)
      {
        tmpIdStations.push_back(station.get());
      }
      else
      {
        SmartMet::Spine::Station s;
        if (not spatialitedb->getStationById(s, fmisid, settings.stationgroup_codes))
          continue;

        tmpIdStations.push_back(s);
        stationCache.insert(s.fmisid, s);
      }
    }

    for (const auto& coordinate : settings.coordinates)
    {
      auto newStations = spatialitedb->findNearestStations(coordinate.at("lat"),
                                                           coordinate.at("lon"),
                                                           info->index,
                                                           settings.maxdistance,
                                                           settings.numberofstations,
                                                           settings.stationgroup_codes,
                                                           stationstarttime,
                                                           stationendtime);

      if (!newStations.empty())
      {
        for (const SmartMet::Spine::Station& newStation : newStations)
        {
          stations.push_back(newStation);
        }
      }
    }

    if (!settings.wmos.empty())
    {
      SmartMet::Spine::Stations tmpStations =
          spatialitedb->findStationsByWMO(settings, info->index);
      for (const SmartMet::Spine::Station& s : tmpStations)
      {
        tmpIdStations.push_back(s);
      }
    }

    if (!settings.lpnns.empty())
    {
      SmartMet::Spine::Stations tmpStations =
          spatialitedb->findStationsByLPNN(settings, info->index);
      for (const SmartMet::Spine::Station& s : tmpStations)
      {
        tmpIdStations.push_back(s);
      }
    }

    if (!settings.boundingBox.empty())
    {
      getStationsByBoundingBox(stations, settings);
    }

    for (const SmartMet::Spine::Station& s : tmpIdStations)
    {
      stations.push_back(s);
      if (settings.numberofstations > 1)
      {
        auto newStations = spatialitedb->findNearestStations(s.latitude_out,
                                                             s.longitude_out,
                                                             info->index,
                                                             settings.maxdistance,
                                                             settings.numberofstations,
                                                             settings.stationgroup_codes,
                                                             stationstarttime,
                                                             stationendtime);

        for (const SmartMet::Spine::Station& nstation : newStations)
          stations.push_back(nstation);
      }
    }

    return stations;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

ts::TimeSeriesVectorPtr Engine::values(Settings& settings,
                                       const SmartMet::Spine::ValueFormatter& /* valueFormatter */)
{
  try
  {
    if (itsShutdownRequested)
      return NULL;

    // Do sanity check for the parameters
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      if (not_special(p))
      {
        string name = parseParameterName(p.name());
        if (!isParameter(name, settings.stationtype) && !isParameterVariant(name))
        {
          throw SmartMet::Spine::Exception(BCP, "No parameter name " + name + " configured.");
        }
      }
    }

    if (itsStationtypeConfig.getUseCommonQueryMethod(settings.stationtype) and
        settings.producer_ids.empty())
      settings.producer_ids =
          *itsStationtypeConfig.getProducerIdSetByStationtype(settings.stationtype);

    auto stationgroupCodeSet =
        itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
    settings.stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    // Try first from cache and on failure (SmartMet::Spine::Exception::) get from database.
    try
    {
      // Get all data from SpatiaLite database if all requirements below apply:
      // 1) stationtype is cached
      // 2) we have the requested time interval in cache
      // 3) stations are available in SpatiaLite
      // However, if Oracle connection pool is full, use SpatiaLite even if we have no recent data
      // in
      // there
      if (settings.useDataCache && dataAvailableInSpatiaLite(settings) && itsSpatiaLiteHasStations)
      {
        return valuesFromSpatiaLite(settings);
      }
    }
    catch (...)
    {
      throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
    }

    /*  FROM THIS POINT ONWARDS DATA IS REQUESTED FROM ORACLE */

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    if (!connectionsOK)
    {
      errorLog("[Observation] values(): No connections to Oracle database!");
      return ret;
    }

    boost::shared_ptr<Oracle> db = itsPool->getConnection();
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    if (settings.stationtype == "flash")
    {
      FlashUtils flashUtils;

      try
      {
        ret = flashUtils.values(*db, settings, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        throw exception;
      }

      return ret;
    }

    setSettings(settings, *db);

    // This contains all stations relevant to each request type.
    SmartMet::Spine::Stations stations;

    try
    {
      getStations(settings, stations, *db, spatialitedb);
      stations = removeDuplicateStations(stations);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    // Return empty data if there are no stations close enough
    if (stations.empty())
    {
      return ret;
    }

    if (itsStationtypeConfig.getUseCommonQueryMethod(settings.stationtype))
    {
      QueryOpenData opendata;

      db->setDatabaseTableName(
          *itsStationtypeConfig.getDatabaseTableNameByStationtype(settings.stationtype));
      try
      {
        ret = opendata.values(*db, stations, settings, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        throw exception;
      }

      db->setDatabaseTableName("");

      return ret;
    }

    try
    {
      // Road, foreign and mareograph stations use FMISID numbers, so LPNN translation is not
      // needed.
      // They also use same cldb table, so all observation types can be fetched with the same
      // method.
      if (settings.stationtype == "road" || settings.stationtype == "foreign" ||
          settings.stationtype == "elering" || settings.stationtype == "mareograph" ||
          settings.stationtype == "buoy")
      {
        ret = db->values(settings, stations, itsTimeZones);
      }

      // Stations maintained by FMI
      else if (settings.stationtype == "fmi")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Stations which measure solar radiation settings.parameters
      else if (settings.stationtype == "solar")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "minute_rad")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }

      // Hourly data
      else if (settings.stationtype == "hourly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Sounding data
      else if (settings.stationtype == "sounding")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Daily data
      else if (settings.stationtype == "daily")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "lammitystarve")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "monthly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else
      {
        throw SmartMet::Spine::Exception(BCP,
                                         "Engine: invalid stationtype: " + settings.stationtype);
      }
    }

    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      db->resetTimeSeries();
      throw exception;
    }

#ifdef MYDEBUG
    cout << "observation query end" << endl;
#endif

    db->resetTimeSeries();

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::makeQuery(QueryBase* qb)
{
  try
  {
    if (itsShutdownRequested)
      return;

    if (qb == NULL)
    {
      std::ostringstream msg;
      msg << "Engine::makeQuery : Implementation of '" << typeid(qb).name()
          << "' class is missing.\n";

      SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    const std::string sqlStatement = qb->getSQLStatement();

    if (sqlStatement.empty())
    {
      std::ostringstream msg;
      msg << "Engine::makeQuery : SQL statement of '" << typeid(*qb).name()
          << "' class is empty.\n";

      SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    std::shared_ptr<QueryResultBase> result = qb->getQueryResultContainer();

    // Try cache first
    boost::optional<std::shared_ptr<QueryResultBase> > cacheResult =
        itsQueryResultBaseCache.find(sqlStatement);
    if (cacheResult)
    {
      if (result->set(cacheResult.get()))
        return;
    }

    if (result == NULL)
    {
      std::ostringstream msg;
      msg << "Engine::makeQuery : Result container of '" << typeid(*qb).name()
          << "' class not found.\n";

      SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    boost::shared_ptr<Oracle> db;

    // Select an active connection in a very rude way.
    // If connection is not connected, reconnec it.
    // If a connection is not reconnected in here the ConnectionPool
    // will return the same faulty connection again and again.
    for (int counter = 1; counter <= itsPoolSize; ++counter)
    {
      db = itsPool->getConnection();

      if (db and db->isConnected())
        break;

      // ConnectionPool should do this
      db->reConnect();

      if (db->isConnected())
        break;

      if (counter == itsPoolSize)
      {
        SmartMet::Spine::Exception exception(BCP, "Missing database connection!");
        // exception.setExceptionCode(Obs_EngineException::MISSING_DATABASE_CONNECTION);
        exception.addDetail("Can not get a database connection.");
        throw exception;
      }
    }

    try
    {
      db->get(sqlStatement, result, itsTimeZones);

      if (not cacheResult)
      {
        itsQueryResultBaseCache.insert(sqlStatement, std::move(result));
      }
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Database query failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * For some reason, database returns duplicate stations in some cases.
 * Remove them.
 */

SmartMet::Spine::Stations Engine::removeDuplicateStations(SmartMet::Spine::Stations& stations)
{
  try
  {
    std::vector<int> ids;
    SmartMet::Spine::Stations noDuplicates;
    for (const SmartMet::Spine::Station& s : stations)
    {
      if (std::find(ids.begin(), ids.end(), s.station_id) == ids.end())
      {
        noDuplicates.push_back(s);
        // BUG? Why is station_id double?
        ids.push_back(boost::numeric_cast<int>(s.station_id));
      }
    }
    return noDuplicates;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Station searches from database can result stations which do not have lpnn number.
 * Take them away in queries which involve observations searches with lpnn identifier.
 */

SmartMet::Spine::Stations Engine::pruneEmptyLPNNStations(SmartMet::Spine::Stations& stations)
{
  try
  {
    SmartMet::Spine::Stations pruned;

    for (const SmartMet::Spine::Station& s : stations)
    {
      if (s.lpnn > 0)
      {
        pruned.push_back(s);
      }
    }
    return pruned;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::readConfigFile(const std::string& configfile)
{
  try
  {
    SmartMet::Spine::ConfigBase cfg(configfile);

    this->service = cfg.get_mandatory_config_param<std::string>("database.service");
    this->username = cfg.get_mandatory_config_param<std::string>("database.username");
    this->password = cfg.get_mandatory_config_param<std::string>("database.password");
    this->nls_lang =
        cfg.get_optional_config_param<std::string>("database.nls_lang", "NLS_LANG=.UTF8");

    this->quiet = cfg.get_optional_config_param<bool>("quiet", true);
    this->timer = cfg.get_optional_config_param<bool>("timer", false);

    this->maxInsertSize = cfg.get_optional_config_param<std::size_t>(
        "maxInsertSize", 9999999999);  // default = all at once

    this->threading_mode =
        cfg.get_optional_config_param<std::string>("sqlite.threading_mode", "SERIALIZED");
    this->cache_timeout = cfg.get_optional_config_param<size_t>("sqlite.timeout", 30000);
    this->shared_cache = cfg.get_optional_config_param<bool>("sqlite.shared_cache", false);
    this->memstatus = cfg.get_optional_config_param<bool>("sqlite.memstatus", false);
    this->synchronous = cfg.get_optional_config_param<std::string>("sqlite.synchronous", "NORMAL");
    this->journal_mode = cfg.get_optional_config_param<std::string>("sqlite.journal_mode", "WAL");

    this->finUpdateInterval = cfg.get_optional_config_param<std::size_t>("finUpdateInterval", 60);
    this->extUpdateInterval = cfg.get_optional_config_param<std::size_t>("extUpdateInterval", 60);
    this->flashUpdateInterval =
        cfg.get_optional_config_param<std::size_t>("flashUpdateInterval", 60);

    this->disableUpdates = cfg.get_optional_config_param<bool>("cache.disableUpdates", false);
    this->boundingBoxCacheSize = cfg.get_mandatory_config_param<int>("cache.boundingBoxCacheSize");
    this->stationCacheSize = cfg.get_mandatory_config_param<int>("cache.stationCacheSize");
#ifdef ENABLE_TABLE_CACHE
    this->resultCacheSize = cfg.get_mandatory_config_param<int>("cache.resultCacheSize");
#endif
    this->locationCacheSize = cfg.get_mandatory_config_param<int>("cache.locationCacheSize");

    this->spatialiteCacheDuration =
        cfg.get_mandatory_config_param<int>("cache.spatialiteCacheDuration");

    this->spatialiteFlashCacheDuration =
        cfg.get_mandatory_config_param<int>("cache.spatialiteFlashCacheDuration");

    this->itsQueryResultBaseCacheSize =
        cfg.get_optional_config_param<size_t>("cache.queryResultBaseCacheSize", 1000);

    this->itsPoolSize = cfg.get_mandatory_config_param<int>("poolsize");
    this->itsSpatiaLitePoolSize = cfg.get_mandatory_config_param<int>("spatialitePoolSize");

    this->itsOracleConnectionPoolGetConnectionTimeOutSeconds =
        cfg.get_optional_config_param<size_t>("oracleConnectionPoolGetConnectionTimeOutSeconds",
                                              30);

    this->itsSerializedStationsFile =
        cfg.get_mandatory_config_param<std::string>("serializedStationsFile");
    this->itsSpatiaLiteFile = cfg.get_mandatory_config_param<std::string>("spatialiteFile");

    this->itsDBRegistryFolderPath =
        cfg.get_mandatory_config_param<std::string>("dbRegistryFolderPath");

    readStationTypeConfig(configfile);

    this->parameterMap = createParameterMapping(configfile);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::readStationTypeConfig(const std::string& configfile)
{
  try
  {
    SmartMet::Spine::ConfigBase cfg(configfile);

    std::vector<std::string> stationtypes =
        cfg.get_mandatory_config_array<std::string>("stationtypes");
    libconfig::Setting& stationtypelistGroup =
        cfg.get_mandatory_config_param<libconfig::Setting&>("oracle_stationtypelist");
    cfg.assert_is_group(stationtypelistGroup);

    for (const string& type : stationtypes)
    {
      if (type.empty())
        continue;

      string stationtype_param = "oracle_stationtype." + type;
      stationTypeMap[type] = cfg.get_mandatory_config_param<std::string>(stationtype_param);

      libconfig::Setting& stationtypeGroup =
          cfg.get_mandatory_config_param<libconfig::Setting&>("oracle_stationtypelist." + type);
      cfg.assert_is_group(stationtypeGroup);

      bool useCommonQueryMethod =
          cfg.get_optional_config_param<bool>(stationtypeGroup, "useCommonQueryMethod", false);
      bool stationTypeIsCached =
          cfg.get_optional_config_param<bool>(stationtypeGroup, "cached", false);

      // Ignore empty vectors
      std::vector<std::string> stationgroupCodeVector =
          cfg.get_mandatory_config_array<std::string>(stationtypeGroup, "stationGroups");
      if (stationgroupCodeVector.size())
        itsStationtypeConfig.addStationtype(type, stationgroupCodeVector);

      if (useCommonQueryMethod || stationTypeIsCached)
      {
        std::vector<uint> producerIdVector =
            cfg.get_mandatory_config_array<uint>(stationtypeGroup, "producerIds");
        if (producerIdVector.empty())
        {
          std::ostringstream msg;
          msg << "At least one producer id must be defined into producerIds array for the "
                 "stationtype '"
              << type << "' if the useCommonQueryMethod value is true.";

          SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
          // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
          exception.addDetail(msg.str());
          throw exception;
        }
        itsStationtypeConfig.setProducerIds(type, producerIdVector);
      }

      std::string databaseTableName =
          cfg.get_optional_config_param<std::string>(stationtypeGroup, "databaseTableName", "");
      if (not databaseTableName.empty())
      {
        itsStationtypeConfig.setDatabaseTableName(type, databaseTableName);
      }
      else if (useCommonQueryMethod)
      {
        std::ostringstream msg;
        msg << "databaseTableName parameter definition is required for the stationtype '" << type
            << "' if the useCommonQueryMethod value is true.";

        SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
        // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
        exception.addDetail(msg.str());
        throw exception;
      }

      itsStationtypeConfig.setUseCommonQueryMethod(type, useCommonQueryMethod);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Parameter Engine::makeParameter(const string& name) const
{
  try
  {
    if (name.empty())
      throw SmartMet::Spine::Exception(BCP, "Empty parameters are not allowed");

    std::string paramname = name;
    SmartMet::Spine::Parameter::Type type = SmartMet::Spine::Parameter::Type::Data;

    if (paramname == "level" || paramname == "latitude" || paramname == "longitude" ||
        paramname == "latlon" || paramname == "lonlat" || paramname == "geoid" ||
        paramname == "place" || paramname == "stationname" || paramname == "name" ||
        paramname == "iso2" || paramname == "region" || paramname == "country" ||
        paramname == "elevation" || paramname == "sunelevation" || paramname == "sundeclination" ||
        paramname == "sunazimuth" || paramname == "dark" || paramname == "sunrise" ||
        paramname == "sunset" || paramname == "noon" || paramname == "sunrisetoday" ||
        paramname == "sunsettoday" || paramname == "moonphase" || paramname == "model" ||
        paramname == "time" || paramname == "localtime" || paramname == "utctime" ||
        paramname == "epochtime" || paramname == "isotime" || paramname == "xmltime" ||
        paramname == "localtz" || paramname == "tz" || paramname == "origintime" ||
        paramname == "wday" || paramname == "weekday" || paramname == "mon" ||
        paramname == "month" || paramname == "hour" || paramname == "timestring" ||
        paramname == "station_name" || paramname == "distance" || paramname == "direction" ||
        paramname == "stationary" || paramname == "lon" || paramname == "lat" ||
        paramname == "stationlon" || paramname == "stationlat" || paramname == "stationlongitude" ||
        paramname == "stationlatitude" || paramname == "station_elevation" || paramname == "wmo" ||
        paramname == "lpnn" || paramname == "fmisid" || paramname == "rwsid" ||
        paramname == "sensor_no")
    {
      type = SmartMet::Spine::Parameter::Type::DataIndependent;
    }

    else if (paramname == "WindCompass8" || paramname == "WindCompass16" ||
             paramname == "WindCompass32" || paramname == "Cloudiness8th" ||
             paramname == "WindChill" || paramname == "Weather")
    {
      type = SmartMet::Spine::Parameter::Type::DataDerived;
    }

    return SmartMet::Spine::Parameter(paramname, type);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::isParameter(const std::string& alias, const std::string& stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    SmartMet::Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterAliasName);

    if (namePtr == parameterMap.end())
      return false;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::isParameterVariant(const std::string& name) const
{
  try
  {
    std::string parameterLowerCase = Fmi::ascii_tolower_copy(name);
    SmartMet::Engine::Observation::removePrefix(parameterLowerCase, "qc_");
    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterLowerCase);

    if (namePtr == parameterMap.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

uint64_t Engine::getParameterId(const std::string& alias, const std::string& stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    SmartMet::Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterAliasName);

    if (namePtr == parameterMap.end())
      return 0;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return 0;

    // Conversion from string to unsigned int.
    // There is possibility that the configured value is not an integer.
    try
    {
      return Fmi::stoul(stationTypeMapPtr->second);
    }
    catch (...)
    {
      return 0;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::set<std::string> Engine::getValidStationTypes() const
{
  try
  {
    std::set<std::string> stationtypes;

    stationtypes.insert("fmi");
    stationtypes.insert("opendata");
    stationtypes.insert("opendata_minute");
    stationtypes.insert("opendata_daily");
    stationtypes.insert("daily");
    stationtypes.insert("hourly");
    stationtypes.insert("monthly");
    stationtypes.insert("lammitystarve");
    stationtypes.insert("solar");
    stationtypes.insert("minute_rad");
    stationtypes.insert("road");
    stationtypes.insert("mareograph");
    stationtypes.insert("opendata_mareograph");
    stationtypes.insert("buoy");
    stationtypes.insert("opendata_buoy");
    stationtypes.insert("syke");
    stationtypes.insert("flash");
    stationtypes.insert("foreign");
    stationtypes.insert("elering");
    stationtypes.insert("sounding");
    stationtypes.insert("research");

    return stationtypes;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * Construct otl_datetime from boost::posix_time.
*/

otl_datetime Engine::makeOTLTime(const ptime& time) const
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

std::string Engine::makeStringTime(const otl_datetime& time) const
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

/*
 * \brief Read values for given times only.
 */

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr Engine::values(
    Settings& settings, const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions)
{
  try
  {
    // Do sanity check for the parameters
    for (const SmartMet::Spine::Parameter& p : settings.parameters)
    {
      if (not_special(p))
      {
        string name = parseParameterName(p.name());
        if (!isParameter(name, settings.stationtype) && !isParameterVariant(name))
        {
          throw SmartMet::Spine::Exception(BCP, "No parameter name " + name + " configured.");
        }
      }
    }

    if (itsStationtypeConfig.getUseCommonQueryMethod(settings.stationtype) and
        settings.producer_ids.empty())
      settings.producer_ids =
          *itsStationtypeConfig.getProducerIdSetByStationtype(settings.stationtype);

    auto stationgroupCodeSet =
        itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
    settings.stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    // Try first from cache and on failure (Obs_EngineException::) get from database.
    try
    {
      // Get all data from SpatiaLite database if all requirements below apply:
      // 1) stationtype is cached
      // 2) we have the requested time interval in cache
      // 3) stations are available in SpatiaLite
      // However, if Oracle connection pool is full, use SpatiaLite even if we have no recent data
      // in
      // there
      if (settings.useDataCache && dataAvailableInSpatiaLite(settings) && itsSpatiaLiteHasStations)
      {
        return valuesFromSpatiaLite(settings, timeSeriesOptions);
      }
    }
    catch (...)
    {
      throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
    }

    /*  FROM THIS POINT ONWARDS DATA IS REQUESTED FROM ORACLE */

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    if (!connectionsOK)
    {
      errorLog("[Observation] values(): No connections to Oracle database!");
      return ret;
    }

    boost::shared_ptr<Oracle> db = itsPool->getConnection();
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();

    if (settings.stationtype == "flash")
    {
      FlashUtils flashUtils;

      try
      {
        ret = flashUtils.values(*db, settings, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        throw exception;
      }

      return ret;
    }

    setSettings(settings, *db);

    // This contains all stations relevant to each request type.
    SmartMet::Spine::Stations stations;

    try
    {
      getStations(settings, stations, *db, spatialitedb);
      stations = removeDuplicateStations(stations);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    // Return empty data if there are no stations close enough
    if (stations.empty())
    {
      return ret;
    }

    if (itsStationtypeConfig.getUseCommonQueryMethod(settings.stationtype))
    {
      QueryOpenData opendata;

      db->setDatabaseTableName(
          *itsStationtypeConfig.getDatabaseTableNameByStationtype(settings.stationtype));
      try
      {
        ret = opendata.values(*db, stations, settings, timeSeriesOptions, itsTimeZones);
      }
      catch (...)
      {
        SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
        errorLog(exception.what());
        throw exception;
      }

      db->setDatabaseTableName("");

      return ret;
    }

    try
    {
      // Road, foreign and mareograph stations use FMISID numbers, so LPNN translation is not
      // needed.
      // They also use same similar table as in open data, so we can get the data by using the same
      // method.
      if (settings.stationtype == "road" || settings.stationtype == "foreign" ||
          settings.stationtype == "elering" || settings.stationtype == "mareograph" ||
          settings.stationtype == "buoy")
      {
        QueryOpenData opendata;
        db->setDatabaseTableName(
            *itsStationtypeConfig.getDatabaseTableNameByStationtype(settings.stationtype));

        ret = opendata.values(*db, stations, settings, timeSeriesOptions, itsTimeZones);
      }

      // Stations maintained by FMI
      else if (settings.stationtype == "fmi")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Stations which measure solar radiation settings.parameters
      else if (settings.stationtype == "solar")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "minute_rad")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }

      // Hourly data
      else if (settings.stationtype == "hourly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Sounding data
      else if (settings.stationtype == "sounding")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      // Daily data
      else if (settings.stationtype == "daily")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "lammitystarve")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else if (settings.stationtype == "monthly")
      {
        db->translateToLPNN(stations);
        stations = pruneEmptyLPNNStations(stations);
        ret = db->values(settings, stations, itsTimeZones);
      }
      else
      {
        throw SmartMet::Spine::Exception(BCP,
                                         "Engine: invalid stationtype: " + settings.stationtype);
      }
    }

    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      db->resetTimeSeries();
      throw exception;
    }

#ifdef MYDEBUG
    cout << "observation query end" << endl;
#endif

    db->resetTimeSeries();

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Get locations and data from SpatiaLite, do not use Oracle at all
// FMISID is used as main id in SpatiaLite tables
ts::TimeSeriesVectorPtr Engine::valuesFromSpatiaLite(
    Settings& settings, const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<SpatiaLite> spatialitedb = itsSpatiaLitePool->getConnection();
    SmartMet::Spine::Stations stations = getStationsFromSpatiaLite(settings, spatialitedb);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, parameterMap, timeSeriesOptions, itsTimeZones);
        return ret;
      }

      ret = spatialitedb->getCachedData(
          stations, settings, parameterMap, timeSeriesOptions, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void* engine_class_creator(const char* configfile, void* /* user_data */)
{
  return new SmartMet::Engine::Observation::Engine(configfile);
}

extern "C" const char* engine_name()
{
  return "Observation";
}
