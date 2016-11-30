#pragma once

#include "Oracle.h"
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class OracleConnectionPool
{
 public:
  ~OracleConnectionPool() {}  //{ delete itsInstance; }
  bool initializePool(int poolSize);
  boost::shared_ptr<Oracle> getConnection();
  void releaseConnection(int connectionId);

  OracleConnectionPool(SmartMet::Engine::Geonames::Engine* geonames,
                       const std::string& service,
                       const std::string& username,
                       const std::string& password,
                       const std::string& nlslang,
                       int poolSize);

  /**
   * @brief How long we wait an inactive connection if all the connections are active.
   * @param seconds Timeout seconds (default is 30 seconds)
   */
  void setGetConnectionTimeOutSeconds(const size_t seconds);

  void shutdown();

 private:
  // NOT USED:
  // int itsMaxWorkers;
  std::vector<int> itsWorkingList;
  std::vector<boost::shared_ptr<Oracle> > itsWorkerList;

  SmartMet::Spine::MutexType itsGetMutex;

  SmartMet::Engine::Geonames::Engine* itsGeoEngine;
  const std::string itsService;
  const std::string itsUsername;
  const std::string itsPassword;
  const std::string itsNLSLang;
  size_t itsGetConnectionTimeOutSeconds;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
