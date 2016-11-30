#pragma once

#include "Interface.h"
#include "Oracle.h"
#include "Settings.h"
#include "ObservableProperty.h"
#include "OracleConnectionPool.h"
#include "SpatiaLiteConnectionPool.h"
#include "DataItem.h"
#include "WeatherDataQCItem.h"
#include "LocationItem.h"
#include "FlashDataItem.h"
#include "StationtypeConfig.h"
#include "Utils.h"

#include <spine/Station.h>
#include <spine/Parameter.h>
#include <spine/Location.h>
#include <spine/Table.h>
#include <spine/TimeSeries.h>

#include <engines/geonames/Engine.h>

#include <jssatomic/atomic_shared_ptr.hpp>

#include <macgyver/Cache.h>
#include <macgyver/ObjectPool.h>
#include <macgyver/TimeZones.h>

#include <libconfig.h++>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Engine : public Interface
{
  typedef std::map<std::string, std::map<std::string, std::string> > ParameterMap;

 private:
  Engine();

  ~Engine() { delete itsPool; }
  OracleConnectionPool* itsPool = nullptr;
  SpatiaLiteConnectionPool* itsSpatiaLitePool = nullptr;

  size_t itsOracleConnectionPoolGetConnectionTimeOutSeconds = 0;

  void readConfigFile(const std::string& configfile);

  void readStationTypeConfig(const std::string& configfile);

  SmartMet::Spine::Stations getStationsByTaggedLocations(
      const SmartMet::Spine::TaggedLocationList& taggedLocations,
      boost::shared_ptr<SpatiaLite> spatialitedb,
      const int numberofstations,
      const std::string& stationtype,
      const int maxdistance,
      const std::set<std::string>& stationgroup_codes,
      const boost::posix_time::ptime& starttime,
      const boost::posix_time::ptime& endtime);

  void getStations(Settings& settings,
                   SmartMet::Spine::Stations& stations,
                   Oracle& db,
                   boost::shared_ptr<SpatiaLite> spatialitedb);

  ParameterMap parameterMap;

  libconfig::Config config;

  const std::string configFile;

  bool quiet;
  bool timer;

  std::map<std::string, std::string> stationTypeMap;

  // Database specific variables
  std::string service;
  std::string username;
  std::string password;
  std::string nls_lang;

  // Cache size settings
  int boundingBoxCacheSize;
  int stationCacheSize;
#ifdef ENABLE_TABLE_CACHE
  int resultCacheSize;
#endif
  int locationCacheSize;

  // Cache updates

  bool disableUpdates = false;
  std::size_t finUpdateInterval;
  std::size_t extUpdateInterval;
  std::size_t flashUpdateInterval;

  // sqlite settings
  std::string threading_mode;
  int cache_timeout;
  bool shared_cache;
  bool memstatus;
  std::string synchronous;
  std::string journal_mode;

  // How many hours to keep observations in SpatiaLite database
  int spatialiteCacheDuration;

  // The time interval which is cached in the observation_data table (Finnish observations)
  jss::atomic_shared_ptr<boost::posix_time::time_period> spatialite_period;

  // The time interval which is cached in the weather_data_qc table (Foreign & road observations)
  jss::atomic_shared_ptr<boost::posix_time::time_period> qcdata_period;

  // How many hours to keep flash data in SpatiaLite
  int spatialiteFlashCacheDuration;

  // The time interval for flash observations which is cached in the SpatiaLite
  jss::atomic_shared_ptr<boost::posix_time::time_period> flash_period;

  // Max inserts in one commit
  std::size_t maxInsertSize;

  bool timeIntervalIsCached(const boost::posix_time::ptime& starttime,
                            const boost::posix_time::ptime& endtime);
  bool timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime& starttime,
                                         const boost::posix_time::ptime& endtime);
  bool flashIntervalIsCached(const boost::posix_time::ptime& starttime,
                             const boost::posix_time::ptime& endtime);

  bool dataAvailableInSpatiaLite(const Settings& settings);
  SmartMet::Spine::Stations getStationsFromSpatiaLite(Settings& settings,
                                                      boost::shared_ptr<SpatiaLite> spatialitedb);
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr valuesFromSpatiaLite(Settings& settings);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr valuesFromSpatiaLite(
      Settings& settings, const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions);

  size_t itsQueryResultBaseCacheSize = 100;

  int itsPoolSize;
  int itsSpatiaLitePoolSize;

  std::string itsSerializedStationsFile;

  std::string itsDBRegistryFolderPath;

  std::string itsSpatiaLiteFile;

  void updateObservationCacheFromOracle();
  void updateFlashCacheFromOracle();
  void cacheFromOracle();
  void updateCacheLoop();
  void updateWeatherDataQCCacheFromOracle();
  void updateWeatherDataQCCacheLoop();
  void updateFlashCacheLoop();

  void initializeCache();

  void initializePool(int poolSize);

  void preloadStations();
  void reloadStations();

  bool itsPreloaded = false;
  bool forceReload = false;

  SmartMet::Engine::Geonames::Engine* geonames = nullptr;

  bool itsReady = false;
  bool itsSpatiaLiteHasStations = false;

  std::string getCacheKey(const Settings& settings);

  std::string getBoundingBoxCacheKey(const Settings& settings);

  std::string getLocationCacheKey(int geoID,
                                  int numberOfStations,
                                  std::string stationType,
                                  int maxDistance,
                                  const boost::posix_time::ptime& starttime,
                                  const boost::posix_time::ptime& endtime);

  bool stationExistsInTimeRange(const SmartMet::Spine::Station& station,
                                const boost::posix_time::ptime& starttime,
                                const boost::posix_time::ptime& endtime);

  bool stationHasRightType(const SmartMet::Spine::Station& station, const Settings& settings);

  bool stationIsInBoundingBox(const SmartMet::Spine::Station& station,
                              const std::map<std::string, double> boundingBox);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr flashValuesFromSpatiaLite(Settings& settings);

  void logMessage(const std::string& message);
  void errorLog(const std::string& message);

  // We need to update two data structures atomically - this way we avoid using a mutex
  struct StationInfo
  {
    SmartMet::Spine::Stations stations;
    std::map<int, SmartMet::Spine::Station> index;
  };
  jss::atomic_shared_ptr<StationInfo> itsStationInfo;

  // jss::atomic_shared_ptr<SmartMet::Spine::Stations> itsPreloadedStations;
  // std::map<int, SmartMet::Spine::Station> itsStationIndex;

  void createSerializedStationsDirectory();
  void serializeStations(SmartMet::Spine::Stations& stations);
  void unserializeStations();

  Fmi::Cache::Cache<int, SmartMet::Spine::Station> stationCache;

  Fmi::Cache::Cache<std::string, std::vector<SmartMet::Spine::Station> > boundingBoxCache;

#ifdef ENABLE_TABLE_CACHE
  Fmi::Cache::Cache<std::string, boost::shared_ptr<SmartMet::Spine::Table> > resultCache;
#endif

  Fmi::Cache::Cache<std::string, std::vector<SmartMet::Spine::Station> > locationCache;

  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase> > itsQueryResultBaseCache;

  SmartMet::Spine::Stations removeDuplicateStations(SmartMet::Spine::Stations& stations);

  SmartMet::Spine::Stations pruneEmptyLPNNStations(SmartMet::Spine::Stations& stations);

  otl_datetime makeOTLTime(const boost::posix_time::ptime& time) const;

  std::string makeStringTime(const otl_datetime& time) const;

  int maxConnectionId;

  bool connectionsOK = false;

  boost::mutex ge_mutex;

  std::shared_ptr<DBRegistry> itsDatabaseRegistry;

  StationtypeConfig itsStationtypeConfig;

  volatile int itsActiveThreadCount = 0;
  volatile bool itsShutdownRequested = false;
  std::unique_ptr<boost::thread> itsUpdateCacheLoopThread;
  std::unique_ptr<boost::thread> itsUpdateWeatherDataQCCacheLoopThread;
  std::unique_ptr<boost::thread> itsUpdateFlashCacheLoopThread;
  std::unique_ptr<boost::thread> itsPreloadStationThread;

  Fmi::TimeZones itsTimeZones;

 public:
  Engine(const std::string& configfile);

  // many locations, many timesteps, many parameters
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings& settings, const SmartMet::Spine::ValueFormatter& valueFormatter);

  // many locations, many times, many parameters
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings& settings, const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions);

  virtual boost::shared_ptr<SmartMet::Spine::Table> makeQuery(
      Settings& settings, boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter);

  /**
   *  @brief Make database query.
   *  @param[in,out] qb A query object with an implementation.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the \c qb object is a NULL pointer or
   *             if the \c qb object has an empty SQL statement or
   *             if the \c qb object result container is not implemented or
   *             if the \c qb object has container of empty size or
*             if the \c qb object has container size differ from the number of parameters
* requested.
   *  @exception Obs_EngineException::MISSING_DATABASE_CONNECTION
   *             If database connection is not available.
   *  @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *             If database coperation fail (e.g. unsupported data type)
   */
  void makeQuery(QueryBase* qb);

  FlashCounts getFlashCount(const boost::posix_time::ptime& starttime,
                            const boost::posix_time::ptime& endtime,
                            const SmartMet::Spine::TaggedLocationList& locations);

  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string>& parameters, const std::string language);

  virtual SmartMet::Spine::Parameter makeParameter(const std::string& name) const;

  virtual bool ready() const;

  virtual void setGeonames(SmartMet::Engine::Geonames::Engine* geonames);

  void setSettings(Settings& settings, Oracle& db);

  const std::shared_ptr<DBRegistry> dbRegistry() const { return itsDatabaseRegistry; }
  void getStations(SmartMet::Spine::Stations& stations, Settings& settings);

  SmartMet::Spine::Stations getStationsByArea(const Settings& settings, const std::string& areaWkt);

  virtual void getStationsByBoundingBox(SmartMet::Spine::Stations& stations,
                                        const Settings& settings);

  virtual void getStationsByRadius(SmartMet::Spine::Stations& stations,
                                   const Settings& settings,
                                   double longitude,
                                   double latitude);

  void setBoundingBoxIsGiven(bool value, Oracle& db, Settings& settings);

  /* \brief Test if the given alias name is configured and it has a field for the stationType.
   * \param[in] alias Alias name of a meteorologal parameter.
   * \param[in] stationType Station type to use for the alias.
   * \retval true The alias exist and it has configuration for the stationType.
* \retval false The alias is not configured or there isn't a field for the stationType inside of
* the alias configuration block.
   */

  bool isParameter(const std::string& alias, const std::string& stationType = "unknown") const;

  /* \brief Test if the given alias name is configured
   * \param[in] name Alias name of a meteorologal parameter.
   * \retval true The alias exist and it has configuration for the stationType.
* \retval false The alias is not configured or there isn't a field for the stationType inside of
* the alias configuration block.
   */

  bool isParameterVariant(const std::string& name) const;

  /* \brief Get a numerical identity for an given alias name.
   * \param[in] alias Alias name of a meteorologal parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case insensitive).
   * \return Positive integer in success and zero if there is no a match.
   */
  uint64_t getParameterId(const std::string& alias,
                          const std::string& stationType = "unknown") const;

  std::set<std::string> getValidStationTypes() const;

  const Fmi::TimeZones& getTimeZones() const { return itsTimeZones; }
 protected:
  virtual void init();
  void shutdown();
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
