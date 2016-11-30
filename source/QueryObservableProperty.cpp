#include "QueryObservableProperty.h"
#include "ObservableProperty.h"
#include <spine/Exception.h>
#include <macgyver/String.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace std;

QueryObservableProperty::QueryObservableProperty()
{
}

QueryObservableProperty::~QueryObservableProperty()
{
}

void QueryObservableProperty::solveMeasurandIds(
    const QueryObservableProperty::ParameterVectorType& parameters,
    const Oracle::ParameterMap& parameterMap,
    const QueryObservableProperty::StationTypeType& stationType,
    QueryObservableProperty::ParameterIdMapType& parameterIDs) const
{
  try
  {
    // Empty list means we want all parameters
    const bool findOnlyGiven = (not parameters.empty());

    for (auto params = parameterMap.begin(); params != parameterMap.end(); ++params)
    {
      if (findOnlyGiven &&
          find(parameters.begin(), parameters.end(), params->first) == parameters.end())
        continue;

      auto gid = params->second.find(stationType);
      if (gid == params->second.end())
        continue;

      int id;
      try
      {
        id = std::stoi(gid->second);
      }
      catch (std::exception&)
      {
        // gid is either too large or not convertible (ie. something is wrong)
        continue;
      }

      parameterIDs.emplace(id, params->first);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<vector<ObservableProperty> > QueryObservableProperty::executeQuery(
    Oracle& oracle, vector<string>& parameters, const string language) const
{
  try
  {
    boost::shared_ptr<vector<ObservableProperty> > observableProperties(
        new vector<ObservableProperty>);

    // Solving measurand id's for valid parameter aliases.
    ParameterIdMapType parameterIDs;
    solveMeasurandIds(parameters, oracle.parameterMap, oracle.stationType, parameterIDs);

    // Return empty list if some parameters are defined and any of those is valid.
    if (parameterIDs.empty())
      return observableProperties;

    int measurandId = -1;
    string measurandCode = "";
    string observablePropertyId = "";
    string observablePropertyLabel = "";
    string basePhenomenon = "";
    string uom = "";
    string statisticalMeasureId = "";
    string statisticalFunction = "";
    string aggregationTimePeriod = "";

    // Input parameters
    boost::posix_time::ptime now(boost::posix_time::second_clock::universal_time());
    otl_datetime in_valid_date = oracle.makeOTLTime(now);
    short int in_station_code = 0;
    short int in_network_id = 0;
    string in_measurand_list = "";

    otl_stream stream;
    otl_refcur_stream refcur;

    try
    {
      stream.open(1,
                  "begin "
                  ":rc<refcur,out> := OPENDATA_QP_PUB.getObservableProperties2_rc("
                  ":in_station_code<short,in>,"
                  ":in_network_id<short,in>,"
                  ":in_begin_time<timestamp,in>,"
                  ":in_measurand_list<char[100],in>,"
                  ":in_language_code<char[100],in>); "
                  "end;",
                  oracle.getConnection());

      stream.set_commit(0);

      stream << in_station_code    // in_station_code
             << in_network_id      // in_network_id
             << in_valid_date      // in_begin_time
             << in_measurand_list  // in_measurand_list
             << language;          // in_language_code

      stream >> refcur;

      // Initialize iterator for the refcur stream and attach the stream to it

      otl_stream_read_iterator<otl_refcur_stream, otl_exception, otl_lob_stream> rs;

      rs.attach(refcur);

      // Iterate the rows
      while (rs.next_row())
      {
        rs.get(1, measurandId);
        rs.get(2, measurandCode);
        rs.get(3, observablePropertyId);
        rs.get(4, observablePropertyLabel);
        rs.get(5, basePhenomenon);
        rs.get(6, uom);
        rs.get(7, statisticalMeasureId);
        rs.get(8, statisticalFunction);
        rs.get(9, aggregationTimePeriod);

        // Multiple parameter name aliases may use a same measurand id (e.g. t2m and temperature)
        std::pair<ParameterIdMapType::iterator, ParameterIdMapType::iterator> r =
            parameterIDs.equal_range(measurandId);
        for (ParameterIdMapType::iterator it = r.first; it != r.second; ++it)
        {
          ObservableProperty property;

          property.measurandId = Fmi::to_string(measurandId);
          property.measurandCode = measurandCode;
          property.observablePropertyId = observablePropertyId;
          property.observablePropertyLabel = observablePropertyLabel;
          property.basePhenomenon = basePhenomenon;
          property.uom = uom;
          property.statisticalMeasureId = statisticalMeasureId;
          property.statisticalFunction = statisticalFunction;
          property.aggregationTimePeriod = aggregationTimePeriod;
          property.gmlId = it->second;

          observableProperties->push_back(property);
        }
      }
    }

    catch (otl_exception& p)  // intercept OTL exceptions
    {
      if (oracle.isFatalError(p.code))  // reconnect if fatal error is encountered
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error

        oracle.reConnect();
        executeQuery(oracle, parameters, language);
      }
      else
      {
        cerr << "ERROR: " << endl;
        cerr << p.msg << endl;       // print out error message
        cerr << p.stm_text << endl;  // print out SQL that caused the error
        cerr << p.var_info << endl;  // print out the variable that caused the error
        throw SmartMet::Spine::Exception(BCP, boost::lexical_cast<std::string>(p.msg));
      }
    }

    return observableProperties;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
