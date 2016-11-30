#include "catch.hpp"
#include "../include/Utils.h"
#include "../include/Engine.h"
#include "../include/Settings.h"

#include <macgyver/TimeZones.h>

// Use global database instance and stationIndex - initializing them always is kind of slow

std::string spatialiteFile = "../../../data/stations/stations.sqlite";
std::string stationXMLFile = "../../../data/stations/stations.xml";

boost::posix_time::ptime starttime(boost::posix_time::time_from_string("2010-01-01 00:00:00"));
boost::posix_time::ptime endtime(boost::posix_time::time_from_string("2010-01-02 00:00:00"));

const bool shared_cache = false;
const int timeout = 10000;
const int max_insert_size = 999999;

SmartMet::Engine::Observation::SpatiaLite db(
    spatialiteFile, max_insert_size, "NORMAL", "WAL", shared_cache, timeout);

Fmi::TimeZones timezones;

std::map<int, SmartMet::Spine::Station> stationIndex =
    SmartMet::Engine::Observation::unserializeStationFile(stationXMLFile);

TEST_CASE("SpatiaLite Basic")
{
  SECTION("getStationCount")
  {
    size_t numberOfStations = db.getStationCount();

    REQUIRE(numberOfStations > 1);
  }

  SECTION("getLatestObservationTime")
  {
    boost::posix_time::ptime latest = db.getLatestObservationTime();
    REQUIRE(latest != boost::posix_time::not_a_date_time);
  }
  SECTION("getLatestWeatherDataQCTime")
  {
    boost::posix_time::ptime latest = db.getLatestWeatherDataQCTime();
    REQUIRE(latest != boost::posix_time::not_a_date_time);
  }
}

TEST_CASE("Test station and data searches")
{
  SECTION("Search Stations using AWS group")
  {
    double lat = 60.17522999999999;
    double lon = 24.94459;
    int maxdistance = 50000;

    std::set<std::string> stationgroup_codes;
    int station_id = 100971;
    SmartMet::Spine::Station station;

    stationgroup_codes.insert("AWS");

    SECTION("A station is searched by id")
    {
      bool success = db.getStationById(station, station_id, stationgroup_codes);
      REQUIRE(station.fmisid == station_id);
      REQUIRE(station.station_formal_name == "Helsinki Kaisaniemi");
    }
    SECTION("1 station is searched by coordinates")
    {
      int numberofstations = 1;

      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().fmisid == station_id);
      REQUIRE(stations.back().station_formal_name == "Helsinki Kaisaniemi");
      REQUIRE(Fmi::stod(stations.back().distance) < 0.1);
    }
    SECTION("5 stations are searched by coordinates")
    {
      int numberofstations = 5;

      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);

      REQUIRE(stations.size() == 5);
      REQUIRE(stations[0].fmisid == 100971);
      REQUIRE(stations[1].fmisid == 101007);
      REQUIRE(stations[2].fmisid == 101004);
      REQUIRE(stations[3].fmisid == 100996);
      REQUIRE(stations[4].fmisid == 101005);
    }

    SECTION("All AWS stations are searched")
    {
      SmartMet::Spine::Stations stations =
          db.findAllStationsFromGroups(stationgroup_codes, stationIndex, starttime, endtime);
      REQUIRE(stations.size() == 179);
    }

    SECTION("Data is searched")
    {
      std::string configfile = "cnf/observation.conf";
      SmartMet::Engine::Observation::ParameterMap parameterMap =
          SmartMet::Engine::Observation::createParameterMapping(configfile);
      SmartMet::Engine::Observation::Settings settings;

      int numberofstations = 5;
      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);

      std::vector<SmartMet::Spine::Parameter> params;
      params.push_back(
          SmartMet::Spine::Parameter("fmisid", SmartMet::Spine::Parameter::Type::DataIndependent));
      params.push_back(
          SmartMet::Spine::Parameter("Temperature", SmartMet::Spine::Parameter::Type::Data));
      boost::posix_time::ptime starttime(
          boost::posix_time::time_from_string("2015-10-08 00:00:00"));
      boost::posix_time::ptime endtime(boost::posix_time::time_from_string("2015-10-08 01:00:00"));

      settings.parameters = params;
      settings.starttime = starttime;
      settings.endtime = endtime;

      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr data =
          db.getCachedData(stations, settings, parameterMap, timezones);

      REQUIRE(data->size() == 2);
      SmartMet::Spine::TimeSeries::TimeSeries ts = data->at(0);
      BOOST_FOREACH (SmartMet::Spine::TimeSeries::TimedValue& value, ts)
      {
      }

      SECTION("Using hour:01 and timestep=1")
      {
        boost::posix_time::ptime starttime(
            boost::posix_time::time_from_string("2015-10-07 00:01:00"));
        boost::posix_time::ptime endtime(
            boost::posix_time::time_from_string("2015-10-07 06:01:00"));

        settings.parameters = params;
        settings.starttime = starttime;
        settings.endtime = endtime;
        settings.timestep = 1;
        settings.stationtype = "opendata";

        SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr data =
            db.getCachedData(stations, settings, parameterMap, timezones);

        REQUIRE(data->size() == 2);
        SmartMet::Spine::TimeSeries::TimeSeries ts = data->at(0);
        BOOST_FOREACH (SmartMet::Spine::TimeSeries::TimedValue& value, ts)
        {
        }
      }
    }
  }

  SECTION("Using EXTRWS group")
  {
    double lat = 60.0605900;
    double lon = 24.0758100;
    int maxdistance = 50000;

    std::set<std::string> stationgroup_codes;
    int station_id = 100013;
    SmartMet::Spine::Station station;

    stationgroup_codes.insert("EXTRWS");

    SECTION("A station is searched by id")
    {
      bool success = db.getStationById(station, station_id, stationgroup_codes);
      REQUIRE(station.fmisid == station_id);
      REQUIRE(station.station_formal_name == "kt51 Inkoo R");
    }
    SECTION("1 station is searched by coordinates")
    {
      int numberofstations = 1;

      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().fmisid == station_id);
      REQUIRE(stations.back().station_formal_name == "kt51 Inkoo R");
      REQUIRE(Fmi::stod(stations.back().distance) < 0.1);
    }
    SECTION("5 stations are searched by coordinates")
    {
      int numberofstations = 5;

      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);

      REQUIRE(stations.size() == 5);
      REQUIRE(stations[0].fmisid == 100013);
      REQUIRE(stations[1].fmisid == 100016);
      REQUIRE(stations[2].fmisid == 100039);
      REQUIRE(stations[3].fmisid == 100065);
      REQUIRE(stations[4].fmisid == 100063);
    }

    SECTION("All EXTRWS stations are searched")
    {
      SmartMet::Spine::Stations stations =
          db.findAllStationsFromGroups(stationgroup_codes, stationIndex, starttime, endtime);
      REQUIRE(stations.size() == 1484);
    }

    SECTION("Data is searched")
    {
      std::string configfile = "cnf/observation.conf";
      SmartMet::Engine::Observation::ParameterMap parameterMap =
          SmartMet::Engine::Observation::createParameterMapping(configfile);
      SmartMet::Engine::Observation::Settings settings;

      int numberofstations = 5;
      SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                  lon,
                                                                  stationIndex,
                                                                  maxdistance,
                                                                  numberofstations,
                                                                  stationgroup_codes,
                                                                  starttime,
                                                                  endtime);

      std::vector<SmartMet::Spine::Parameter> params;
      params.push_back(
          SmartMet::Spine::Parameter("fmisid", SmartMet::Spine::Parameter::Type::DataIndependent));
      params.push_back(
          SmartMet::Spine::Parameter("Temperature", SmartMet::Spine::Parameter::Type::Data));
      boost::posix_time::ptime starttime(
          boost::posix_time::time_from_string("2015-10-07 06:00:00"));
      boost::posix_time::ptime endtime(boost::posix_time::time_from_string("2015-10-07 09:00:00"));

      settings.parameters = params;
      settings.starttime = starttime;
      settings.endtime = endtime;
      settings.stationtype = "road";

      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr data =
          db.getCachedWeatherDataQCData(stations, settings, parameterMap, timezones);

      REQUIRE(data->size() == 2);
      SmartMet::Spine::TimeSeries::TimeSeries ts = data->at(0);
      BOOST_FOREACH (SmartMet::Spine::TimeSeries::TimedValue& value, ts)
      {
      }
    }

    SECTION("Using EXTSYNOP group")
    {
      double lat = 63.6522;
      double lon = 18.5506001;
      int maxdistance = 50000;
      int station_id = 114226;
      int geoid = -16011960;

      std::set<std::string> stationgroup_codes;
      SmartMet::Spine::Station station;

      stationgroup_codes.insert("EXTSYNOP");

      SECTION("1 station is searched by coordinates")
      {
        int numberofstations = 1;

        SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                    lon,
                                                                    stationIndex,
                                                                    maxdistance,
                                                                    numberofstations,
                                                                    stationgroup_codes,
                                                                    starttime,
                                                                    endtime);
        REQUIRE(stations.size() == 1);
        REQUIRE(stations.back().fmisid == station_id);
        REQUIRE(stations.back().station_formal_name == "Hemling");
        REQUIRE(stations.back().geoid == geoid);
        REQUIRE(Fmi::stod(stations.back().distance) <= 0.1);
      }

      SECTION("Data is searched")
      {
        std::string configfile = "cnf/observation.conf";
        SmartMet::Engine::Observation::ParameterMap parameterMap =
            SmartMet::Engine::Observation::createParameterMapping(configfile);
        SmartMet::Engine::Observation::Settings settings;

        int numberofstations = 1;
        SmartMet::Spine::Stations stations = db.findNearestStations(lat,
                                                                    lon,
                                                                    stationIndex,
                                                                    maxdistance,
                                                                    numberofstations,
                                                                    stationgroup_codes,
                                                                    starttime,
                                                                    endtime);

        std::vector<SmartMet::Spine::Parameter> params;
        params.push_back(SmartMet::Spine::Parameter(
            "fmisid", SmartMet::Spine::Parameter::Type::DataIndependent));
        params.push_back(
            SmartMet::Spine::Parameter("Temperature", SmartMet::Spine::Parameter::Type::Data));

        SECTION("Using exact hour:00")
        {
          boost::posix_time::ptime starttime(
              boost::posix_time::time_from_string("2015-10-08 00:00:00"));
          boost::posix_time::ptime endtime(
              boost::posix_time::time_from_string("2015-10-08 06:00:00"));

          settings.parameters = params;
          settings.starttime = starttime;
          settings.endtime = endtime;

          SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr data =
              db.getCachedWeatherDataQCData(stations, settings, parameterMap, timezones);

          REQUIRE(data->size() == 2);
          SmartMet::Spine::TimeSeries::TimeSeries ts = data->at(0);
          BOOST_FOREACH (SmartMet::Spine::TimeSeries::TimedValue& value, ts)
          {
          }
        }
        SECTION("Using hour:01 and timestep=20")
        {
          boost::posix_time::ptime starttime(
              boost::posix_time::time_from_string("2015-10-08 00:01:00"));
          boost::posix_time::ptime endtime(
              boost::posix_time::time_from_string("2015-10-08 06:01:00"));

          settings.parameters = params;
          settings.starttime = starttime;
          settings.endtime = endtime;
          settings.timestep = 20;
          settings.stationtype = "foreign";

          SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr data =
              db.getCachedWeatherDataQCData(stations, settings, parameterMap, timezones);

          REQUIRE(data->size() == 2);
          SmartMet::Spine::TimeSeries::TimeSeries ts = data->at(0);
          BOOST_FOREACH (SmartMet::Spine::TimeSeries::TimedValue& value, ts)
          {
          }
        }
      }
    }
  }
}
