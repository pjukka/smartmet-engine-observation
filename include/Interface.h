#pragma once

#include <string>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <spine/Station.h>
#include <spine/Table.h>
#include <spine/ValueFormatter.h>
#include <spine/SmartMetEngine.h>
#include <spine/TimeSeries.h>
#include <engines/geonames/Engine.h>
#include "Settings.h"
#include "QueryBase.h"
#include "DBRegistry.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Settings;

class Interface : public SmartMet::Spine::SmartMetEngine
{
 protected:
  Interface() {}
 public:
  virtual ~Interface();

  virtual boost::shared_ptr<SmartMet::Spine::Table> makeQuery(
      Settings& settings, boost::shared_ptr<SmartMet::Spine::ValueFormatter>& valueFormatter) = 0;

  virtual SmartMet::Spine::Parameter makeParameter(const std::string& name) const = 0;

  virtual void makeQuery(QueryBase* qb) = 0;

  virtual bool ready() const = 0;

  virtual void setGeonames(SmartMet::Engine::Geonames::Engine* geonames) = 0;

  virtual const std::shared_ptr<DBRegistry> dbRegistry() const = 0;

  virtual void getStations(SmartMet::Spine::Stations& stations, Settings& settings) = 0;

  virtual void getStationsByBoundingBox(SmartMet::Spine::Stations& stations,
                                        const Settings& settings) = 0;

  virtual void getStationsByRadius(SmartMet::Spine::Stations& stations,
                                   const Settings& settings,
                                   double longitude,
                                   double latitude) = 0;

  virtual bool isParameter(const std::string& alias,
                           const std::string& stationType = "unknown") const = 0;

  virtual uint64_t getParameterId(const std::string& alias,
                                  const std::string& stationType = "unknown") const = 0;

  virtual SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings& settings, const SmartMet::Spine::ValueFormatter& valueFormatter) = 0;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
