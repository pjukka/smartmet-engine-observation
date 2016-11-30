#include "Oracle.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class FlashQuery : private boost::noncopyable
{
 public:
  FlashQuery();

  std::string createQuery(Oracle& oracle, Settings& settings);

 private:
  std::string addWantedParameters(const std::vector<SmartMet::Spine::Parameter>& parameters);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
