#pragma once

#define PI 3.14159265358979323846

#ifndef SMARTMET_OBSENGINE_UTILS_H
#define SMARTMET_OBSENGINE_UTILS_H

#include <macgyver/String.h>
#include <spine/Parameter.h>
#include <spine/Station.h>
#include <spine/ConfigBase.h>

#include <boost/utility.hpp>
#include <boost/algorithm/string.hpp>

#include <string>
#include <vector>
#include <map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
typedef struct FlashCounts
{
  int flashcount;
  int strokecount;
  int iccount;
} FlashCounts;

/** \brief Remove given prefix from an input string.
 * @param[in,out] parameter The string from which the prefix is wanted to remove.
 * @param[in] prefix The prefix string we are looking for.
 * @return true if the given prefix is found and removed otherwise false.
 */
bool removePrefix(std::string& parameter, const std::string& prefix);

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const SmartMet::Spine::Parameter& theParam);

typedef std::map<std::string, std::map<std::string, std::string> > ParameterMap;

std::string trimCommasFromEnd(const std::string& what);

std::string translateParameter(const std::string& paramname,
                               const std::string& stationType,
                               ParameterMap& parameterMap);

void calculateStationDirection(SmartMet::Spine::Station& station);
double deg2rad(double deg);
double rad2deg(double rad);

std::string windCompass8(const double direction);
std::string windCompass16(const double direction);
std::string windCompass32(const double direction);

std::string parseParameterName(const std::string& parameter);
int parseSensorNumber(const std::string& parameter);

/** \brief Get station data structure from disk
 * @param[in] filename The filename with path which contains stations in xml format
 * @retval std::map<int, SmartMet::Spine::Station> Data structure which can be used to quickly query
 * station info
 */
std::map<int, SmartMet::Spine::Station> unserializeStationFile(const std::string filename);

ParameterMap createParameterMapping(const std::string& configfile);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // SMARTMET_OBSENGINE_UTILS_H
