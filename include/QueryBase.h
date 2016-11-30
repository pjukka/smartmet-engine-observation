#pragma once

#include "QueryResult.h"

#include "Oracle.h"
#include "ObservableProperty.h"

#include <string>
#include <memory>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryBase
{
 public:
  QueryBase();

  virtual ~QueryBase();

  // virtual  boost::shared_ptr<std::vector<ObservableProperty> > executeQuery(Oracle & db) const =
  // 0;

  /**
   *  @brief Get empty SQL statement.
   *  @return Empty SQL statement.
   */
  virtual std::string getSQLStatement() const { return ""; }
  /**
   *  @brief Get a reference with null value to a container to store data.
   *  @return Null pointer.
   */
  virtual std::shared_ptr<QueryResult> getQueryResultContainer()
  {
    return std::shared_ptr<QueryResult>();
  }

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
