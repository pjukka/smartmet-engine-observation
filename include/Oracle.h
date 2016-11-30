#pragma once

#define PI 3.14159265358979323846

#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "LocationItem.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "WeatherDataQCItem.h"
#include "Utils.h"

#include <spine/Location.h>
#include <spine/Table.h>
#include <spine/Station.h>
#include <spine/Parameter.h>
#include <spine/TimeSeries.h>
#include <spine/Value.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <spine/ValueFormatter.h>

#include <macgyver/TimeFormatter.h>
#include <macgyver/TimeZones.h>

#include <newbase/NFmiMetMath.h>  //For FeelsLike

#include <engines/geonames/Engine.h>

#include <libconfig.h++>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/local_time_adjustor.hpp>
#include <boost/utility.hpp>

#include <string>

#define OTL_ORA11G_R2
#define OTL_STL
#define OTL_STREAM_READ_ITERATOR_ON
#define OTL_EXTENDED_EXCEPTION
#define OTL_STREAM_THROWS_NOT_CONNECTED_TO_DATABASE_EXCEPTION
//#define OTL_ORA_TIMESTAMP
#include "otlv4.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Oracle : private boost::noncopyable
{
 public:
  /**
   *  @brief Test that the connection is alive.
   *  @retval true Connection is alive
   *  @retval false Connection is not alive and should reconnect.
   */
  bool isConnected();
  void shutdown();

  /**
   *  @brief Execute SQL statement and get the result.
   *  @param[in] sqlStatement SQL statement to execute.
   *  @param[out] qrb Pointer to the container to store the result data.
   *  @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   */
  void get(const std::string& sqlStatement,
           std::shared_ptr<QueryResultBase> qrb,
           const Fmi::TimeZones& timezones);

  /**
   * @brief Get count of flashes in a time interval and bounding boxB
   * @param starttime Start of the time interval
   * @param endtime End of the time interval
   * @param locations The list of area limitations (SmartMet::Spine::BoundingBox or CoordinatePoint)
   * @retval int The number of flashes
   */
  FlashCounts getFlashCount(const boost::posix_time::ptime& starttime,
                            const boost::posix_time::ptime& endtime,
                            const SmartMet::Spine::TaggedLocationList& locations);

  void readLocationsFromOracle(std::vector<LocationItem>& locations,
                               const Fmi::TimeZones& timezones);
  void readCacheDataFromOracle(std::vector<DataItem>& locations,
                               boost::posix_time::ptime lastTime,
                               const Fmi::TimeZones& timezones);
  void readFlashCacheDataFromOracle(std::vector<FlashDataItem>& flashCacheData,
                                    boost::posix_time::ptime lastTime,
                                    const Fmi::TimeZones& timezones);
  void readWeatherDataQCFromOracle(std::vector<WeatherDataQCItem>& locations,
                                   boost::posix_time::ptime lastTime,
                                   const Fmi::TimeZones& timezones);

  void getAllStations(SmartMet::Spine::Stations& stations, const Fmi::TimeZones& timezones);
  void getStationByGeoid(SmartMet::Spine::Stations& stations,
                         int geoid,
                         const Fmi::TimeZones& timezones);
  void getStationById(int id,
                      SmartMet::Spine::Stations& stations,
                      int wmo,
                      int lpnn,
                      const Fmi::TimeZones& timezones);
  void getStation(const std::string& searchkey,
                  SmartMet::Spine::Stations& stations,
                  const Fmi::TimeZones& timezones);
  SmartMet::Spine::Stations getStationsByBoundingBox(
      const std::map<std::string, double>& boundingBox, const Fmi::TimeZones& timezones);

  SmartMet::Spine::Stations getStationsByLatLon(const double lat,
                                                const double lon,
                                                const int numberofstations,
                                                const Fmi::TimeZones& timezones);
  void getNearestStationsForPoint(const SmartMet::Spine::LocationPtr& location,
                                  SmartMet::Spine::Stations& stations,
                                  int numberofstations,
                                  const Fmi::TimeZones& timezones);

  Oracle(SmartMet::Engine::Geonames::Engine* geonames,
         const std::string& service,
         const std::string& username,
         const std::string& password,
         const std::string& nls_lang,
         const boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter);
  ~Oracle();
  boost::shared_ptr<SmartMet::Spine::Table> getWeatherDataQCObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);

  boost::shared_ptr<SmartMet::Spine::Table> getHourlyFMIObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);

  boost::shared_ptr<SmartMet::Spine::Table> getFMIObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);
  boost::shared_ptr<SmartMet::Spine::Table> getSoundings(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);
  boost::shared_ptr<SmartMet::Spine::Table> getSolarObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);
  boost::shared_ptr<SmartMet::Spine::Table> getMinuteRadiationObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const Fmi::TimeZones& timezones);
  boost::shared_ptr<SmartMet::Spine::Table> getDailyAndMonthlyObservations(
      const std::vector<SmartMet::Spine::Parameter>& params,
      const SmartMet::Spine::Stations& stations,
      const std::string& type,
      const Fmi::TimeZones& timezones);

  std::string translateParameter(const std::string& paramname);
  void setTimeInterval(const boost::posix_time::ptime& starttime,
                       const boost::posix_time::ptime& endtime,
                       const int timestep);
  otl_datetime makeOTLTimeNow() const;
  otl_datetime makeOTLTime(const boost::posix_time::ptime& time) const;
  otl_datetime makeRoundedOTLTime(const boost::posix_time::ptime& time) const;
  std::string makeStringTime(const otl_datetime& time) const;
  std::string makeStringTimeWithSeconds(const otl_datetime& time) const;
  boost::posix_time::ptime makePosixTime(const otl_datetime& time,
                                         const Fmi::TimeZones& timezones,
                                         const std::string& timezone = "") const;
  boost::posix_time::ptime makePrecisionTime(const otl_datetime& time);
  std::string makeEpochTime(const boost::posix_time::ptime& time) const;
  std::string formatDate(const boost::local_time::local_date_time& ltime, std::string format);

  void makeParamIndexes(const std::vector<SmartMet::Spine::Parameter>& params,
                        std::map<std::string, int>& paramindex,
                        std::map<int, std::string>& specialsindex,
                        std::string& queryparameters);
  void makeParamIndexesForWeatherDataQC(const std::vector<SmartMet::Spine::Parameter>& params,
                                        std::map<std::string, int>& paramindex,
                                        std::map<int, std::string>& specialsindex,
                                        std::string& queryparameters,
                                        std::map<std::string, int>& queryparamindex);

  std::string makeSpecialParameter(const SmartMet::Spine::Station& station,
                                   const std::string& parameter,
                                   double value = 0);

  SmartMet::Spine::TimeSeries::Value makeSpecialParameterVariant(
      const SmartMet::Spine::Station& station, const std::string& parameter, double value = 0);

  // Station id manipulators
  void translateWMOToLPNN(const std::vector<int>& wmos, SmartMet::Spine::Stations& stations);
  void translateToLPNN(SmartMet::Spine::Stations& stations);
  void translateToWMO(SmartMet::Spine::Stations& stations);
  void translateToRWSID(SmartMet::Spine::Stations& stations);
  std::vector<int> translateWMOToFMISID(const std::vector<int>& wmos);
  std::vector<int> translateRWSIDToFMISID(const std::vector<int>& wmos);
  std::vector<int> translateLPNNToFMISID(const std::vector<int>& lpnns);
  std::vector<int> translateWMOToLPNN(const std::vector<int>& wmos);

  void addInfoToStation(SmartMet::Spine::Station& station,
                        const SmartMet::Spine::LocationPtr& location);
  void addInfoToStation(SmartMet::Spine::Station& station,
                        const double latitude,
                        const double longitude);
  void makeRow(SmartMet::Spine::Table& result,
               const int& metacount,
               std::map<std::string, int>& paramindex,
               std::map<int, std::string>& specialsindex,
               std::map<double, SmartMet::Spine::Station>& stationindex,
               otl_datetime& timestamp,
               int sensor_no,
               double winddirection,
               int level,
               int id,
               int& rownumber,
               otl_stream_read_iterator<otl_stream, otl_exception, otl_lob_stream>& rs,
               otl_stream& query,
               const SmartMet::Spine::Station& station,
               const Fmi::TimeZones& timezones,
               double windspeed = kFloatMissing,        // For FeelsLike
               double temperature = kFloatMissing,      // For FeelsLike
               double relativehumidity = kFloatMissing  // For FeelsLike
               );

  void calculateStationDirection(SmartMet::Spine::Station& station);
  double deg2rad(double deg);
  double rad2deg(double rad);

  std::string windCompass8(double direction);
  std::string windCompass16(double direction);
  std::string windCompass32(double direction);

  void replaceRainParameters(std::string& queryParameters);
  std::string getDistanceSql(const SmartMet::Spine::Stations& stations);
  std::string getIntervalSql(const std::string& qstations, const Fmi::TimeZones& timezones);

  int solveStationtype();
  std::string solveStationtypeList();
  std::map<std::string, double> getStationCoordinates(int fmisid);

  otl_connect& getConnection();

  boost::shared_ptr<SmartMet::Spine::ValueFormatter> valueFormatter;

  boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;

  otl_datetime startTime;
  otl_datetime endTime;
  otl_datetime exactStartTime;
  std::string timeFormat;
  int timeStep;
  double maxDistance;
  std::string stationType;
  std::string timeZone;
  bool allPlaces;
  bool latest;
  typedef std::map<std::string, std::map<std::string, std::string> > ParameterMap;
  ParameterMap parameterMap;
  std::vector<int> hours;
  std::vector<int> weekdays;
  std::string language;
  std::string missingText;
  std::locale locale;
  int numberOfStations;

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return itsConnectionId; }
  void setBoundingBoxIsGiven(bool value) { itsBoundingBoxIsGiven = value; }
  void setDatabaseTableName(const std::string& name);
  const std::string getDatabaseTableName() const;

  bool isFatalError(int code);
  void reConnect();
  void beginSession();
  void endSession();
  void attach();
  void detach();

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(Settings& settings,
                                                          const SmartMet::Spine::Stations& stations,
                                                          const Fmi::TimeZones& timezones);
  void resetTimeSeries() { itsTimeSeriesColumns.reset(); }
 private:
  otl_connect thedb;
  SmartMet::Engine::Geonames::Engine* geonames;
  int itsConnectionId;

  bool itsConnected;
  bool itsShutdownRequested;

  std::string itsService;
  std::string itsUsername;
  std::string itsPassword;

  std::string itsDatabaseTableName;

  bool itsBoundingBoxIsGiven;

  // for time series
  // fmisid -> TimeSeriesVectorPtr
  std::map<int, SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr> itsTimeSeriesStationColumns;
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr itsTimeSeriesColumns;
  void setTimeSeriesValue(unsigned int column, const SmartMet::Spine::TimeSeries::TimedValue& tv);
  void setTimeSeriesStationValue(int fmisid,
                                 unsigned int column,
                                 const SmartMet::Spine::TimeSeries::TimedValue& tv);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
