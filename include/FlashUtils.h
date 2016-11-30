#include "Oracle.h"

#include <macgyver/TimeZones.h>

#include <spine/TimeSeries.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class FlashUtils : private boost::noncopyable
{
 public:
  FlashUtils();

  boost::shared_ptr<SmartMet::Spine::Table> getData(
      Oracle& oracle,
      Settings& settings,
      boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter,
      const Fmi::TimeZones& timezones);
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(Oracle& oracle,
                                                          Settings& settings,
                                                          const Fmi::TimeZones& timezones);

 private:
  void printParams(std::map<std::string, int>& params);

  std::map<std::string, int> createParameterMap(
      std::vector<SmartMet::Spine::Parameter>& parameters);

  bool isWantedParameter(const std::string& key, const std::map<std::string, int>& parameterMap);

  void formatTimeParameters(const Settings& settings,
                            boost::shared_ptr<SmartMet::Spine::Table>& result,
                            const otl_datetime& stroke_time,
                            const Oracle& oracle,
                            std::map<std::string, int>& parameterMap,
                            const int row,
                            const Fmi::TimeZones& timezones);

  boost::posix_time::ptime makeFlashTime(const otl_datetime& time,
                                         const std::string& timezone,
                                         const Fmi::TimeZones& timezones) const;

  // for time series
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr itsTimeSeriesColumns;
  void setTimeSeriesValue(unsigned int column, const SmartMet::Spine::TimeSeries::TimedValue& tv);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
