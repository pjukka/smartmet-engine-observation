#pragma once

#include "QueryBase.h"

#include <vector>
#include <map>
#include <string>
#include "ObservableProperty.h"
#include "Oracle.h"
#include "Settings.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryObservableProperty : public QueryBase
{
  typedef std::multimap<int, std::string> ParameterIdMapType;  //!< Measurand id / parameter alias
  // name
  typedef std::vector<std::string> ParameterVectorType;
  typedef std::string StationTypeType;

 public:
  QueryObservableProperty();

  virtual ~QueryObservableProperty();

  virtual boost::shared_ptr<std::vector<ObservableProperty> > executeQuery(
      Oracle& db, std::vector<std::string>& parameters, const std::string language) const;

 private:
  void solveMeasurandIds(const ParameterVectorType& parameters,
                         const Oracle::ParameterMap& parameterMap,
                         const StationTypeType& stationType,
                         ParameterIdMapType& parameterIds) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
