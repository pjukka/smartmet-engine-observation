#include "FlashQuery.h"
#include "Utils.h"
#include <spine/Exception.h>

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
FlashQuery::FlashQuery()
{
}

string FlashQuery::createQuery(Oracle& oracle, Settings& settings)
{
  try
  {
    map<string, int> paramindex;
    map<int, string> specialsindex;
    string queryparams = "";

    oracle.makeParamIndexes(settings.parameters, paramindex, specialsindex, queryparams);

    oracle.setTimeInterval(settings.starttime, settings.endtime, settings.timestep);

    if (!settings.timeformat.empty())
    {
      oracle.timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));
    }
    else
    {
      oracle.timeFormatter.reset(Fmi::TimeFormatter::create(oracle.timeFormat));
    }

    string flashTable = "flashdata";
    std::string query;
    query +=
        "SELECT "
        "CAST(flash.stroke_time AS DATE) AS stroke_time, "
        "flash.flash_id, "
        "flash.stroke_location.sdo_point.x AS longitude, "
        "flash.stroke_location.sdo_point.y AS latitude, ";

    query += addWantedParameters(settings.parameters);

    query += " FROM " + flashTable + " flash ";

    query +=
        "WHERE "
        "flash.stroke_time BETWEEN :in_starttime<timestamp,in> AND :in_endtime<timestamp,in> ";

    if (!settings.taggedLocations.empty())
    {
      for (auto tloc : settings.taggedLocations)
      {
        if (tloc.loc->type == SmartMet::Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          std::string radius = Fmi::to_string(tloc.loc->radius);
          // This might be very slow!
          query +=
              " AND SDO_WITHIN_DISTANCE(flash.stroke_location, SDO_GEOMETRY(2001, 8307, "
              "SDO_POINT_TYPE(" +
              lon + ", " + lat + ", NULL), NULL, NULL), 'distance = " + radius +
              " unit = km') = 'TRUE'";
        }
      }
    }

    if (!settings.boundingBox.empty())
    {
      query +=
          "AND "
          "flash.stroke_location.sdo_point.x"
          " BETWEEN " +
          Fmi::to_string(settings.boundingBox["minx"]) + " AND " +
          Fmi::to_string(settings.boundingBox["maxx"]) + " AND " +
          "flash.stroke_location.sdo_point.y" + +" BETWEEN " +
          Fmi::to_string(settings.boundingBox["miny"]) + " AND " +
          Fmi::to_string(settings.boundingBox["maxy"]);
    }

    query += " ORDER BY flash.stroke_time ";

    return query;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

string FlashQuery::addWantedParameters(const vector<SmartMet::Spine::Parameter>& parameters)
{
  try
  {
    string addedParameters = "";
    for (const SmartMet::Spine::Parameter& parameter : parameters)
    {
      if (parameter.name() != "utctime" && parameter.name() != "flash_id" &&
          parameter.name() != "time" && parameter.name() != "longitude" &&
          parameter.name() != "latitude" && parameter.name() != "localtime" &&
          parameter.type() == SmartMet::Spine::Parameter::Type::Data)
      {
        addedParameters += "flash." + parameter.name() + ",";
      }
    }
    return trimCommasFromEnd(addedParameters);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
