#include "Utils.h"
#include <spine/Exception.h>
#include <boost/filesystem.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/serialization/vector.hpp>
#include <fstream>
#include <macgyver/TypeName.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
bool removePrefix(std::string& parameter, const std::string& prefix)
{
  try
  {
    std::size_t prefixLen = prefix.length();
    if ((parameter.length() > prefixLen) && parameter.compare(0, prefixLen, prefix) == 0)
    {
      parameter.erase(0, prefixLen);
      return true;
    }

    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const SmartMet::Spine::Parameter& theParam)
{
  try
  {
    switch (theParam.type())
    {
      case SmartMet::Spine::Parameter::Type::Data:
      case SmartMet::Spine::Parameter::Type::Landscaped:
        return true;
      case SmartMet::Spine::Parameter::Type::DataDerived:
      case SmartMet::Spine::Parameter::Type::DataIndependent:
        return false;
    }
    // NOT REACHED
    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string trimCommasFromEnd(const std::string& what)
{
  try
  {
    size_t end = what.find_last_not_of(",");
    return what.substr(0, end + 1);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/* Translates parameter names to match the parameter name in the database.
 * If the name is not found in parameter map, return the given name.
 */

std::string translateParameter(const std::string& paramname,
                               const std::string& stationType,
                               ParameterMap& parameterMap)
{
  try
  {
    // All parameters are in lower case in parametermap
    std::string p = Fmi::ascii_tolower_copy(paramname);
    if (!parameterMap[p][stationType].empty())
      return parameterMap[p][stationType];
    else
      return p;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Calculates station direction in degrees from given coordinates
void calculateStationDirection(SmartMet::Spine::Station& station)
{
  try
  {
    double direction;
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude_out);
    double lat2 = deg2rad(station.latitude_out);

    double dlon = lon2 - lon1;

    direction = rad2deg(
        atan2(sin(dlon) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)));

    if (direction < 0)
    {
      direction += 360.0;
    }

    station.stationDirection = std::round(10.0 * direction) / 10.0;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Utility method to convert degrees to radians
double deg2rad(double deg)
{
  return (deg * PI / 180);
}

// Utility method to convert radians to degrees
double rad2deg(double rad)
{
  return (rad * 180 / PI);
}

std::string windCompass8(double direction)
{
  static const std::string names[] = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

  int i = static_cast<int>((direction + 22.5) / 45) % 8;
  return names[i];
}

std::string windCompass16(double direction)
{
  static const std::string names[] = {"N",
                                      "NNE",
                                      "NE",
                                      "ENE",
                                      "E",
                                      "ESE",
                                      "SE",
                                      "SSE",
                                      "S",
                                      "SSW",
                                      "SW",
                                      "WSW",
                                      "W",
                                      "WNW",
                                      "NW",
                                      "NNW"};

  int i = static_cast<int>((direction + 11.25) / 22.5) % 16;
  return names[i];
}

std::string windCompass32(const double direction)
{
  static const std::string names[] = {"N", "NbE", "NNE", "NEbN", "NE", "NEbE", "ENE", "EbN",
                                      "E", "EbS", "ESE", "SEbE", "SE", "SEbS", "SSE", "SbE",
                                      "S", "SbW", "SSW", "SWbS", "SW", "SWbW", "WSW", "WbS",
                                      "W", "WbN", "WNW", "NWbW", "NW", "NWbN", "NNW", "NbW"};

  int i = static_cast<int>((direction + 5.625) / 11.25) % 32;
  return names[i];
}

std::string parseParameterName(const std::string& parameter)
{
  try
  {
    std::string name = Fmi::ascii_tolower_copy(parameter);

    removePrefix(name, "qc_");

    // No understrike
    if (name.find_last_of("_") == std::string::npos)
      return name;

    size_t startpos = name.find_last_of("_");
    size_t endpos = name.length();

    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // Test appearance of the parameter between TRS_10MIN_DIF and TRS_10MIN_DIF_1
    std::string sensorNumber = name.substr(startpos + 1, length);
    try
    {
      Fmi::stoi(sensorNumber);
    }
    catch (...)
    {
      return name;
    }

    return name.substr(0, startpos);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * The sensor number is given after an underscore, for example KELI_1
 */

int parseSensorNumber(const std::string& parameter)
{
  try
  {
    size_t startpos, endpos;
    int defaultSensorNumber = 1;
    std::string sensorNumber;
    startpos = parameter.find_last_of("_");
    endpos = parameter.length();
    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // If sensor number is given, for example KELI_1, return requested number
    if (startpos != std::string::npos && endpos != std::string::npos)
    {
      sensorNumber = parameter.substr(startpos + 1, length);
      try
      {
        return Fmi::stoi(sensorNumber);
      }
      catch (...)
      {
        return defaultSensorNumber;
      }
    }

    return defaultSensorNumber;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::map<int, SmartMet::Spine::Station> unserializeStationFile(const std::string filename)
{
  try
  {
    std::map<int, SmartMet::Spine::Station> index;
    SmartMet::Spine::Stations tmpStations;
    try
    {
      boost::filesystem::path path = boost::filesystem::path(filename);
      if (!boost::filesystem::is_empty(path))
      {
        std::ifstream file(filename);
        boost::archive::xml_iarchive archive(file);
        archive& BOOST_SERIALIZATION_NVP(tmpStations);
        for (const SmartMet::Spine::Station& station : tmpStations)
        {
          index[station.station_id] = station;
        }
      }
    }
    catch (std::exception& e)
    {
      std::string errorMessage = e.what();
      std::cout << "Unserialization failed: " << errorMessage << std::endl;
    }
    return index;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

ParameterMap createParameterMapping(const std::string& configfile)
{
  try
  {
    ParameterMap pm;
    namespace ba = boost::algorithm;

    SmartMet::Spine::ConfigBase cfg(configfile);

    try
    {
      // Use parameter mapping container like this: parameterMap["parameter"]["station_type"]
      // Example: parameterMap["t2m"]["road"]

      // Phase 1: Establish producer setting
      std::vector<std::string> param_names =
          cfg.get_mandatory_config_array<std::string>("parameters");

      // Phase 2: Parse parameter conversions

      for (const std::string& paramname : param_names)
      {
        if (Fmi::ascii_tolower_copy(paramname).compare(0, 3, "qc_") == 0)
          throw SmartMet::Spine::Exception(
              BCP,
              "Observation error: Parameter aliases with 'qc_' prefix are not allowed. Fix the '" +
                  paramname + "' parameter.");

        auto& param = cfg.get_mandatory_config_param<libconfig::Setting&>(paramname);
        cfg.assert_is_group(param);

        std::map<std::string, std::string> p;
        for (int j = 0; j < param.getLength(); ++j)
        {
          std::string name = param[j].getName();
          p.insert(std::make_pair(name, static_cast<const char*>(param[j])));
        }

        const std::string lower_parame_name = Fmi::ascii_tolower_copy(paramname);

        if (pm.find(lower_parame_name) != pm.end())
          throw SmartMet::Spine::Exception(
              BCP, "Observation error: Duplicate parameter alias '" + paramname + "' found.");

        // All internal comparisons between parameter names are done with lower case names
        // to prevent confusion and typos
        pm.insert(make_pair(lower_parame_name, p));
      }
    }
    catch (libconfig::ConfigException&)
    {
      cfg.handle_libconfig_exceptions("createParameterMapping");
    }

    return pm;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
