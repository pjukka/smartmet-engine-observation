#include <string>

namespace sqlite_api
{
#include <sqlite3.h>
#include <spatialite.h>
}

#define DATABASE_VERSION "2"

#define SOCI_USE_BOOST

#include <soci/soci.h>
#include <soci/sqlite3/soci-sqlite3.h>
#include <soci/boost-optional.h>

#include <macgyver/TimeFormatter.h>
#include <macgyver/TimeZones.h>

#include <spine/Location.h>
#include <spine/Station.h>
#include <spine/Thread.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <spine/Value.h>

#include "Settings.h"
#include "LocationItem.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "WeatherDataQCItem.h"
#include "Utils.h"

//#include <boost/utility.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLite : private boost::noncopyable
{
  typedef std::map<std::string, std::map<std::string, std::string> > ParameterMap;

 private:
  // Private members
  soci::session itsSession;
  std::string srid;
  bool itsShutdownRequested;
  int itsConnectionId;
  int itsMaxInsertSize;
  std::map<std::string, std::string> stationTypeMap;

  // Private methods

  std::string stationType(const std::string& type);
  std::string stationType(SmartMet::Spine::Station& station);

  void addSpecialParameterToTimeSeries(
      const std::string& paramname,
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr& timeSeriesColumns,
      const SmartMet::Spine::Station& station,
      const int pos,
      const std::string stationtype,
      const boost::local_time::local_date_time& obstime);

  void addParameterToTimeSeries(
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr& timeSeriesColumns,
      const std::pair<boost::local_time::local_date_time,
                      std::map<std::string, SmartMet::Spine::TimeSeries::Value> >& dataItem,
      const std::map<std::string, int>& specialPositions,
      const std::map<std::string, std::string>& parameterNameMap,
      const std::map<std::string, int>& timeseriesPositions,
      const ParameterMap& parameterMap,
      const std::string& stationtype,
      const SmartMet::Spine::Station& station);

  void addEmptyValuesToTimeSeries(
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr& timeSeriesColumns,
      const boost::local_time::local_date_time& obstime,
      const std::map<std::string, int>& specialPositions,
      const std::map<std::string, std::string>& parameterNameMap,
      const std::map<std::string, int>& timeseriesPositions,
      const std::string& stationtype,
      const SmartMet::Spine::Station& station);

  void updateStations(const SmartMet::Spine::Stations& stations);
  void updateStationGroups(const SmartMet::Spine::Stations& stations);

  boost::posix_time::ptime getLatestTimeFromTable(std::string tablename, std::string time_field);

  void initSpatialMetaData();
  void createStationTable();
  void createStationGroupsTable();
  void createGroupMembersTable();
  void createLocationsTable();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();

 public:
  SpatiaLite(const std::string& spatialiteFile,
             std::size_t max_insert_size,
             const std::string& synchronous,
             const std::string& journal_mode,
             bool shared_cache,
             int timeout);

  ~SpatiaLite();

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @retval boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestObservationTime();
  boost::posix_time::ptime getLatestFlashTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @retval boost::posix_time::ptime The time of the newest observation
   */
  boost::posix_time::ptime getLatestWeatherDataQCTime();

  /**
   * @brief Create the SpatiaLite tables from scratch
   */

  void createTables();

  /**
   * @brief Return the number of rows in the stations table
   */

  size_t getStationCount();

  void updateStationsAndGroups(SmartMet::Spine::Stations& stations);

  SmartMet::Spine::Stations findAllStationsFromGroups(
      const std::set<std::string> stationgroup_codes,
      std::map<int, SmartMet::Spine::Station>& stationIndex,
      const boost::posix_time::ptime& starttime,
      const boost::posix_time::ptime& endtime);

  SmartMet::Spine::Stations findNearestStations(
      double latitude,
      double longitude,
      const std::map<int, SmartMet::Spine::Station>& stationIndex,
      int maxdistance,
      int numberofstations,
      const std::set<std::string>& stationgroup_codes,
      const boost::posix_time::ptime& starttime,
      const boost::posix_time::ptime& endtime);

  SmartMet::Spine::Stations findNearestStations(
      const SmartMet::Spine::LocationPtr& location,
      const std::map<int, SmartMet::Spine::Station>& stationIndex,
      const int maxdistance,
      const int numberofstations,
      const std::set<std::string>& stationgroup_codes,
      const boost::posix_time::ptime& starttime,
      const boost::posix_time::ptime& endtime);

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return itsConnectionId; }
  /**
   * @brief Insert new stations or update old ones in locations table.
   * @param[in] Vector of locations
   */
  void fillLocationCache(const std::vector<LocationItem>& locations);

  /**
   * @brief Update observation_data with data from Oracle's
   *        observation_data table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from observation_data.
  */
  void fillDataCache(const std::vector<DataItem>& cacheData);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  void fillWeatherDataQCCache(const std::vector<WeatherDataQCItem>& cacheData);

  /**
   * @brief Insert cached observations into observation_data table
   * @param cacheData Observation data to be inserted into the table
   */
  void fillFlashDataCache(const std::vector<FlashDataItem>& flashCacheData);

  /**
   * @brief Delete old observation data from tablename table using time_column time field
   * @param tablename The name of the table from which the data will be deleted
   * @param time_column Indicates the time field which is used in WHERE statement
   * @param last_time Delete everything from tablename which is older than last_time
   */
  void cleanCacheTable(const std::string tablename,
                       const std::string time_column,
                       const boost::posix_time::ptime last_time);

  /**
   * @brief Delete everything from observation_data table which is
   *        older than last_time
   * @param[in] last_time
   */
  void cleanDataCache(const boost::posix_time::ptime& last_time);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than last_time
   * @param[in] last_time
   */
  void cleanWeatherDataQCCache(const boost::posix_time::ptime& last_time);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedWeatherDataQCData(
      const SmartMet::Spine::Stations& stations,
      const Settings& settings,
      const ParameterMap& parameterMap,
      const Fmi::TimeZones& timezones);

  /**
   * @brief Delete old flash observation data from flash_data table
   * @param timetokeep Delete everything from flash_data which is older than last_time
   */
  void cleanFlashDataCache(const boost::posix_time::ptime& timetokeep);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedData(
      const SmartMet::Spine::Stations& stations,
      const Settings& settings,
      const ParameterMap& parameterMap,
      const Fmi::TimeZones& timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedFlashData(
      const Settings& settings, const ParameterMap& parameterMap, const Fmi::TimeZones& timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedWeatherDataQCData(
      const SmartMet::Spine::Stations& stations,
      const Settings& settings,
      const ParameterMap& parameterMap,
      const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
      const Fmi::TimeZones& timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedData(
      SmartMet::Spine::Stations& stations,
      Settings& settings,
      ParameterMap& parameterMap,
      const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
      const Fmi::TimeZones& timezones);

  SmartMet::Spine::Stations findStationsInsideArea(
      const Settings& settings,
      const std::string& areaWkt,
      std::map<int, SmartMet::Spine::Station>& stationIndex);
  SmartMet::Spine::Stations findStationsInsideBox(
      const Settings& settings, const std::map<int, SmartMet::Spine::Station>& stationIndex);

  SmartMet::Spine::Stations findStationsByWMO(
      const Settings& settings, const std::map<int, SmartMet::Spine::Station>& stationIndex);
  SmartMet::Spine::Stations findStationsByLPNN(
      const Settings& settings, const std::map<int, SmartMet::Spine::Station>& stationIndex);

  /**
   * @brief Fill station_id, fmisid, wmo, geoid, lpnn, longitude_out and latitude_out into the
   *        station object if value is missing.
   *        Some id  (station_id, fmisid, wmo, lpnn or geoid) must be defined in the Station object.
   * @param[in,out] s Data is filled to this object if some id is present.
   * @param[in] stationgroup_codes Station match requires a correct station group
   *        If the stationgroup_codes list is empty the station group is not used.
   * @retval true If the data is filled successfully.
   * @retval false If the data is not filled at all.
   */
  bool fillMissing(SmartMet::Spine::Station& s, const std::set<std::string>& stationgroup_codes);

  /**
   * @brief Get the station odered by \c station_id.
   * @param station_id Primary identity of the requested station.
   *        \c station_id is the same value as the station fmisid value.
   * @param[in] stationgroup_codes Station match requires a correct station group
   *        If the stationgroup_codes list is empty the station group is not used.
   * @retval true If the station is found and data stored into the given object.
   * @retval false If the station is no found.
   */
  bool getStationById(SmartMet::Spine::Station& station,
                      int station_id,
                      const std::set<std::string>& stationgroup_codes);

  void shutdown();

  /**
 * @brief Get count of flashes are in the time interval
 * @param starttime Start of the time interval
 * @param endtime End of the time interval
 * @param boundingBox The bounding box. Must have crs EPSG:4326.
 * @retval FlashCounts The number of flashes in the interval
 */
  FlashCounts getFlashCount(const boost::posix_time::ptime& starttime,
                            const boost::posix_time::ptime& endtime,
                            const SmartMet::Spine::TaggedLocationList& locations);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
