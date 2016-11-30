#include "OracleConnectionPool.h"
#include <spine/Exception.h>

#include <boost/foreach.hpp>

using namespace std;

namespace
{
template <class T>
struct Releaser
{
  Releaser(SmartMet::Engine::Observation::OracleConnectionPool* pool_handle)
      : poolHandle(pool_handle)
  {
  }
  void operator()(T* t) { poolHandle->releaseConnection(t->connectionId()); }
  SmartMet::Engine::Observation::OracleConnectionPool* poolHandle;
};
}

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
OracleConnectionPool::OracleConnectionPool(SmartMet::Engine::Geonames::Engine* geonames,
                                           const std::string& service,
                                           const std::string& username,
                                           const std::string& password,
                                           const std::string& nlslang,
                                           int poolSize)
    : itsGeoEngine(geonames),
      itsService(service),
      itsUsername(username),
      itsPassword(password),
      itsNLSLang(nlslang),
      itsGetConnectionTimeOutSeconds(30)
{
  try
  {
    itsWorkingList.resize(poolSize, -1);
    itsWorkerList.resize(poolSize);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool OracleConnectionPool::initializePool(int poolSize)
{
  try
  {
    for (unsigned int i = 0; i < itsWorkingList.size(); i++)
    {
      try
      {
        // Logon here
        boost::shared_ptr<SmartMet::Spine::ValueFormatter> valueFormatter(
            new SmartMet::Spine::ValueFormatter(SmartMet::Spine::ValueFormatterParam()));
        itsWorkerList[i] = boost::shared_ptr<Oracle>(new Oracle(
            itsGeoEngine, itsService, itsUsername, itsPassword, itsNLSLang, valueFormatter));
        itsWorkerList[i]->attach();
        itsWorkerList[i]->beginSession();
        // Mark pool item as inactive
        itsWorkingList[i] = 0;
        itsWorkerList[i]->setConnectionId(i);
      }
      catch (otl_exception& p)
      {
        cerr << "[Observation] error: could not get a connection: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        // Something is wrong, return false. The reason is unimportant but failure to create a
        // connection must be taken into account in Engine
        return false;
      }
      catch (std::exception& err)
      {
        cerr << "OracleConnectionPool::initializePool: " << err.what() << endl;
        return false;
      }
      catch (...)
      {
        cerr << "OracleConnectionPool::initializePool: Unknown error." << endl;
        return false;
      }
    }

    // Everything is OK
    return true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<Oracle> OracleConnectionPool::getConnection()
{
  try
  {
    /*
     *  1 --> active
     *  0 --> inactive
     *
     * Logic of returning connections:
     *
     * 1. Check if worker is idle, if so return that worker.
     * 2. Sleep and start over
     */
    size_t countTimeOut = 0;
    while (true)
    {
      SmartMet::Spine::WriteLock lock(itsGetMutex);
      for (unsigned int i = 0; i < itsWorkingList.size(); i++)
      {
        if (itsWorkingList[i] == 0)
        {
          itsWorkingList[i] = 1;
          // itsWorkerList[i]->attach();
          // itsWorkerList[i]->beginSession(tryNumber); // this is very slow
          itsWorkerList[i]->setConnectionId(i);
          return boost::shared_ptr<Oracle>(itsWorkerList[i].get(), Releaser<Oracle>(this));
        }
      }

      // Fail after timeout seconds is reached.
      if (++countTimeOut > itsGetConnectionTimeOutSeconds)
        throw SmartMet::Spine::Exception(
            BCP, "Could not get a database connection. All the database connections are in use!");
      else  // Avoid busy loop. We assume that the for loop above use much less time than a second.
        sleep(1);
    }

    // NEVER EXECUTED:
    // throw SmartMet::Spine::Exception(BCP,"[Observation] Could not get a connection in
    // OracleConnectionPool::getConnection()");
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void OracleConnectionPool::shutdown()
{
  try
  {
    std::cout << "  -- Shutdown requested (OracleConnectionPool)\n";
    for (unsigned int i = 0; i < itsWorkerList.size(); i++)
    {
      auto sl = itsWorkerList[i].get();
      if (sl != NULL)
        sl->shutdown();
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void OracleConnectionPool::releaseConnection(int connectionId)
{
  try
  {
    // This mutex is not needed since writing the int is atomic. In fact, if there is a queue to
    // get connections, releasing a Oracle back to the pool would have to compete against the
    // threads which are trying to get a connection. The more requests are coming, the less
    // chances we have of releasing the connection back to the pool, which may escalate the
    // problem - Mika

    // boost::mutex::scoped_lock lock(itsGetMutex);

    // Do "destructor" stuff here, because Oracle instances are never destructed

    // Release the worker to the pool
    itsWorkingList[connectionId] = 0;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void OracleConnectionPool::setGetConnectionTimeOutSeconds(const size_t seconds)
{
  try
  {
    itsGetConnectionTimeOutSeconds = seconds;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
