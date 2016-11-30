#pragma once

#include "QueryBase.h"

#include <spine/Table.h>
#include <spine/TimeSeries.h>

#include <macgyver/Cache.h>
#include <macgyver/TimeFormatter.h>
#include <macgyver/TimeZones.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesGeneratorOptions.h>

#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryOpenData : public QueryBase
{
 public:
  QueryOpenData();

  virtual ~QueryOpenData();

  virtual boost::shared_ptr<SmartMet::Spine::Table> executeQuery(
      Oracle& db,
      SmartMet::Spine::Stations& stations,
      Settings& settings,
      const Fmi::TimeZones& timezones);

  virtual boost::shared_ptr<SmartMet::Spine::Table> executeQuery(
      Oracle& db,
      SmartMet::Spine::Stations& stations,
      Settings& settings,
      const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
      const Fmi::TimeZones& timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(Oracle& oracle,
                                                          SmartMet::Spine::Stations& stations,
                                                          Settings& settings,
                                                          const Fmi::TimeZones& timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      Oracle& oracle,
      SmartMet::Spine::Stations& stations,
      Settings& settings,
      const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
      const Fmi::TimeZones& timezones);

  void selectOne(Oracle& oracle);
  void ping(Oracle& oracle);

 private:
  std::string makeParameterBlock(const SmartMet::Spine::Parameter& p, Oracle& oracle);
  std::string makeIntervalSql(const std::string& stations,
                              const Settings& settings,
                              const Oracle& oracle);

  std::string makeSQLWithNoTimestep(std::string& qstations, Settings& settings, Oracle& oracle);
  std::string makeSQLWithTimestep(std::string& qstations, Settings& settings, Oracle& oracle);

  std::string makeSQLWithTimeSeries(
      std::string& qstations,
      Settings& settings,
      Oracle& oracle,
      const SmartMet::Spine::TimeSeriesGeneratorOptions& timeSeriesOptions,
      const Fmi::TimeZones& timezones);

  void fillSpecials(SmartMet::Spine::Table& result,
                    std::map<std::string, std::string>& values,
                    std::map<std::string, int>& specialsOrder,
                    Settings& settings,
                    int row);

  void fillData(SmartMet::Spine::Table& result,
                std::map<std::string, std::string>& values,
                std::map<std::string, int>& paramindex,
                int& row);

  void fillTimeSeriesData(std::map<std::string, SmartMet::Spine::TimeSeries::Value>& values,
                          std::map<std::string, int>& specialsOrder,
                          std::map<std::string, int>& paramindex,
                          Settings& settings,
                          bool fillMissingValues = false);

  std::map<std::string, SmartMet::Spine::Station> itsStations;

  boost::shared_ptr<SmartMet::Spine::ValueFormatter> itsValueFormatter;

  std::string tableName(const std::string& stationtype);

  // for time series
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr itsTimeSeriesColumns;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
