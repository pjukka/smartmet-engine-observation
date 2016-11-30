#ifndef MAST_QUERY_H
#define MAST_QUERY_H

#include <string>
#include <map>
#include <vector>
#include "QueryResult.h"
#include "QueryBase.h"
#include "QueryParamsBase.h"
#include "MastQueryParams.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * @brief The class implements interface to fetch Mast data.
 *
 */
class MastQuery : public QueryBase
{
 public:
  explicit MastQuery();

  ~MastQuery();

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
  void setQueryParams(const MastQueryParams* qParams);

 private:
  MastQuery& operator=(const MastQuery& other);
  MastQuery(const MastQuery& other);

  // SQL statement parts contructed in setQueryParams method
  // used lated in getSQLStatement method.
  int m_selectSize;
  std::string m_select;
  std::string m_from;
  std::string m_where;
  std::string m_orderBy;
  std::string m_distinct;

  std::shared_ptr<QueryResult> m_queryResult;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

/**

@page OBSENGINE_MAST_QUERY Mast data query

@section OBSENGINE_MAST_QUERY_EXAMPLE_CODE Example code

Following code show out how to get all the measurand codes available
in two stations: Kumpula, Helsinki and Vuosaari satama, Helsinki.

NOTE: The code is not tested.

@verbatim

#include <engines/observation/MastQuery.h>
#include <engines/geonames/Engine.h>
#include <engines/observation/Interface.h>

namespace bo = SmartMet::Engine;

SmartMet::GeoEngine::Engine * m_geoEngine;
SmartMet::Obs::Interface * m_obsEngine;

void init()
{
        auto * reactor = get_reactor();
        void* engine;
        engine = reactor->getSingleton("GeoEngine",NULL);
        m_geoEngine = reinterpret_cast<GeoEngine::Engine*>(engine);
        engine = reactor->getSingleton("Engine",NULL);
        m_obsEngine = reinterpret_cast<Obs::Interface*>(engine);
        m_obsEngine->setGeonames(m_geoEngine);
}


const std::shared_ptr<bo::DBRegistryConfig> dbRegistryConfig(const std::string & configName) const
{
        // Get database registry from Engine.
        const std::shared_ptr<bo::DBRegistry> dbRegistry = m_obsEngine->dbRegistry();
        if (dbRegistry)
        {
                const std::shared_ptr<bo::DBRegistryConfig> dbrConfig =
dbRegistry->dbRegistryConfig(configName);
                if (dbrConfig)
                       return dbrConfig;
        }
        return std::shared_ptr<bo::DBRegistryConfig>();
}


void getAndPrint()
{
        const std::shared_ptr<bo::DBRegistryConfig> cnfBase = dbRegistryConfig("OBSERVATIONS_V2");
        const std::shared_ptr<bo::DBRegistryConfig> cnf1 = dbRegistryConfig("MEASURANDS_V1");
        const std::shared_ptr<bo::DBRegistryConfig> cnf2 = dbRegistryConfig("STATIONS_V1");

        if (not cnfBase or not cnf1 or not cnf2)
                return;

        // Set the base configuration. All the other configuration should  be joined to the Base
configuration.
        bo::MastQueryParams stationQueryParams(cnfBase);

        // Join the MEASURANDS_V1 database view by using common field name MEASURAND_ID.
        stationQueryParams.addJoinOnConfig(cnf1, "MEASURAND_ID");

        // Select measurand code data column in to the result.
        stationQueryParams.addField("MEASURAND_CODE");

        // Join the STATIONS_V1 database view by using common field name STATION_ID.
        stationQueryParams.addJoinOnConfig(cnf2, "STATION_ID");

        // Select station identity column in to the result.
        stationQueryParams.addField("STATION_ID");

        // Choose only two stations.
        const int station_id_1 = 101004;
        const int station_id_2 = 100971;
        stationQueryParams.addOperation("OR_GROUP_station_id","STATION_ID","PropertyIsEqualTo",
station_id_1);
        stationQueryParams.addOperation("OR_GROUP_station_id","STATION_ID","PropertyIsEqualTo",
station_id_2);

        // Set station query params into the query object
        bo::MastQuery stationQuery;
        stationQuery.setQueryParams(&stationQueryParams);

        // Make the query
        m_obsEngine->makeQuery(&stationQuery);

        // Get the result container
        std::shared_ptr<QueryResult> dataContainer = stationQuery.getQueryResultContainer();

        // Get the column data iterators
        bo::QueryResult::ValueVectorType::const_iterator dataFmisidIt =
dataContainer->begin("STATION_ID");
        bo::QueryResult::ValueVectorType::const_iterator dataFmisidItEnd =
dataContainer->end("STATION_ID");
        bo::QueryResult::ValueVectorType::const_iterator dataMeasurandCodeIt =
dataContainer->begin("MEASURAND_CODE");

        while (dataFmisidIt != dataFmisidItEnd)
        {
              std::cout << bo::QueryResult::toString(dataFmisidIt,0) << " " <<
bo::QueryResult::toString(dataMeasurandCodeIt) << "\n";
              dataFmisidIt++;
              dataMeasurandCodeIt++;
        }
}

@endverbatim

*/

#endif  // ENVIRONMENTAL_MONITORING_FACILITY_QUERY_H
