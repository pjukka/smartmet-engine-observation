#ifndef VERIFIABLE_MESSAGE_QUERY_H
#define VERIFIABLE_MESSAGE_QUERY_H

#include <string>
#include <map>
#include <vector>
#include "QueryResult.h"
#include "QueryBase.h"
#include "QueryParamsBase.h"
#include "VerifiableMessageQueryParams.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * @brief The class implements interface to fetch IWXXM messages.
 *
 * All the options needed in database query should be set
 * by using VerifiableMessageQueryParams class object.
 */
class VerifiableMessageQuery : public QueryBase
{
 public:
  explicit VerifiableMessageQuery();

  ~VerifiableMessageQuery();

  /**
   * @brief Get SQL statement constructed in the class.
   * @return SQL statement string of empty string when failure occur.
   */
  std::string getSQLStatement() const;

  /**
   * @brief Get reference to the result container of
   *        the class object to store or read data.
   * @return Reference to the result container or NULL if
   *         SQL statement produce an empty result.
   */
  std::shared_ptr<QueryResult> getQueryResultContainer();

  /**
   * \brief Set query params used in SQL statement formation.
   *        The result lines will be ordered by message_time (ascending order)
   *        and / or station_id respectively if the parameter requested.
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *            If time format conversion to Oracle time format fail.
   * @exception Obs_EngineException::INVALID_SQL_STATEMENT
   *            If SELECT parameter list is empty.
   * @exception Obs_EngineException::MISSING_PARAMETER_VALUE
   *            If there is no any location to look for.
   */
  void setQueryParams(const VerifiableMessageQueryParams* qParams);

 private:
  VerifiableMessageQuery& operator=(const VerifiableMessageQuery& other);
  VerifiableMessageQuery(const VerifiableMessageQuery& other);

  // SQL statement parts contructed in setQueryParams method
  // used lated in getSQLStatement method.
  std::vector<std::string> m_select;
  std::string m_from;
  std::string m_where;
  std::string m_orderBy;

  // Store station IDs in setQueryParams method for later
  // use in getSQLStatement method.
  std::vector<std::string> m_stationIDs;

  std::shared_ptr<QueryResult> m_queryResult;

  // If the param is set to "true", result will be orderd
  // by message time in descending order. Only the first
  // row from the result of each station will be returned.
  bool m_returnOnlyLatest;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // VERIFIABLE_MESSAGE_QUERY_H
