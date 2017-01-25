%define DIRNAME observation
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet Observation Engine
Name: %{SPECNAME}
Version: 17.1.4
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-observation
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: libconfig-devel
BuildRequires: oracle-instantclient11.2-devel
BuildRequires: boost-devel
Requires: libconfig
BuildRequires: smartmet-library-spine-devel >= 17.1.4
BuildRequires: smartmet-engine-geonames-devel >= 17.1.4
BuildRequires: mysql++-devel >= 3.1.0
BuildRequires: libspatialite-devel >= 4.1.1
BuildRequires: sqlite-devel >= 3.11.0
BuildRequires: soci-devel >= 3.2.3
BuildRequires: soci-sqlite3-devel >= 3.2.3
BuildRequires: smartmet-library-locus-devel >= 16.12.20
BuildRequires: smartmet-library-macgyver-devel >= 16.12.20
BuildRequires: jssatomic
Requires: smartmet-server >= 17.1.4
Requires: smartmet-engine-geonames >= 17.1.4
Requires: smartmet-library-spine >= 17.1.4
Requires: smartmet-library-locus >= 16.12.20
Requires: smartmet-library-macgyver >= 16.12.20
Requires: oracle-instantclient11.2-basic
Requires: libatomic
Requires: unixODBC
Requires: mysql++
Requires: libspatialite >= 4.1.1
Requires: sqlite >= 3.11.0
Requires: soci >= 3.2.3
Requires: soci-sqlite3 >= 3.2.3
Requires: boost-date-time
Requires: boost-iostreams
Requires: boost-locale
Requires: boost-serialization
Requires: boost-system
Requires: boost-thread
Obsoletes: smartmet-brainstorm-obsengine < 16.11.1
Obsoletes: smartmet-brainstorm-obsengine-debuginfo < 16.11.1

%if 0%{rhel} >= 7
BuildRequires: mariadb-devel
Requires: mariadb-libs
%else
BuildRequires: mysql-devel
%endif
Provides: %{SPECNAME}

%description
SmartMet engine for fetching observations from the climate database (cldb).

%package -n %{SPECNAME}-devel
Summary: SmartMet %{SPECNAME} development headers
Group: SmartMet/Development
Provides: %{SPECNAME}-devel
Obsoletes: smartmet-brainstorm-obsengine-devel < 16.11.1
%description -n %{SPECNAME}-devel
SmartMet %{SPECNAME} development headers.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n engines/%{DIRNAME}

%build -q -n engines/%{DIRNAME}
make %{_smp_mflags}

%install
%makeinstall
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d
mkdir -p $RPM_BUILD_ROOT%{_var}/smartmet/observation
install -m 644 cnf/tnsnames.ora $RPM_BUILD_ROOT%{_sysconfdir}/tnsnames.ora
install -m 644 cnf/oracle-x86_64.conf $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d/oracle-x86_64.conf
install -m 664 cnf/stations.xml $RPM_BUILD_ROOT/var/smartmet/observation/stations.xml
install -m 664 cnf/stations.sqlite.2 $RPM_BUILD_ROOT/var/smartmet/observation/stations.sqlite.2

%post
/sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files -n %{SPECNAME}
%defattr(0775,root,root,0775)
%{_datadir}/smartmet/engines/%{DIRNAME}.so
%defattr(0664,root,root,0775)
%config(noreplace) %{_sysconfdir}/tnsnames.ora
%{_sysconfdir}/ld.so.conf.d/oracle-x86_64.conf
%config(noreplace) %{_var}/smartmet/observation/stations.xml
%config(noreplace) %{_var}/smartmet/observation/stations.sqlite.2

%files -n %{SPECNAME}-devel
%defattr(0664,root,root,0775)
%{_includedir}/smartmet/engines/%{DIRNAME}

%changelog
* Wed Jan  4 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.1.4-1.fmi
- Changed to use renamed SmartMet base libraries

* Wed Nov 30 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.30-2.fmi
- Safer location database initialization

* Wed Nov 30 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.30-1.fmi
- Removed open and hp configurations
- No installation for configuration

* Tue Nov 29 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.29-1.fmi
- Added multiple join fields support to MastQuery class.

* Wed Nov 23 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.23-1.fmi
- Fixes to station caching

* Tue Nov  1 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.1-1.fmi
- Namespace changed
- Fixed the possibility to disable SpatiaLite cache updates
- Fixed SnowDepth06 and snow06 producers to be opendata_daily

* Tue Sep 20 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.9.20-2.fmi
- Add parameters to opendata stationtype to get radiation parameters working

* Tue Sep  6 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.9.6-1.fmi
- New exception handler

* Mon Aug 15 2016 Markku Koskela <markku.koskela@fmi.fi> - 16.8.15-1.fmi
- The engine was fixed so that it does not try cache older information
- than it is actually holding in the cache.
- Added the shutdown of the Oracle connections. This was not needed
- earlier, because the shutdown signal was SIGINT, which terminates
- also the Oracle connections.

* Mon Jun 20 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.20-1.fmi
- Added dummy parameters for PoP and WeatherSymbol3 to be compatible with qengine names.
- Added checks against NULL coordinate values from the database

* Tue Jun 14 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.14-2.fmi
- Added spatialiteFlashCacheDuration setting to obsengine_hp.conf

* Tue Jun 14 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.14-1.fmi
- Full recompile

* Fri Jun  3 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.3-1.fmi
- Fixed merge problems - start update threads only once

* Thu Jun  2 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.2-2.fmi
- Fixed error handling when stations with missing FMISID's are requested

* Thu Jun  2 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.2-1.fmi
- Full recompile
- Made station preload thread safe

* Wed Jun  1 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.1-1.fmi
- Added graceful shutdown
- updates can now be disabled using option disableUpdates (default = false)

* Mon May 23 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.5.23-1.fmi
- sqlite options are now configurable
- multithread-mode is now default instead of serialized-mode

* Wed May 18 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.5.18-1.fmi
- Using atomic_shared_ptr for thread safety

* Mon May 16 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.5.16-1.fmi
- Replaced TimeZoneFactory with TimeZones

* Wed May 11 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.5.11-1.fmi
- Added WW_AWS and RI_10MIN parameters to foreign producer with MISSING values
- Fixed a bug in weather_data_qc windcompass queries

* Tue May  3 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.5.3-1.fmi
- Reduced Oracle pool size to 10

* Wed Apr 27 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.4.27-2.fmi
- Fix a bug in flash query time handling

* Wed Apr 27 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.4.27-1.fmi
- Support point radius queries with metaplugin and timeseries

* Thu Apr 21 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.21-2.fmi
- Amended open data configuration with smaller flash cache

* Thu Apr 21 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.21-1.fmi
- Increased flash cache duration because salamapalvelu

* Wed Apr 20 2016  Santeri Oksman <santeri.oksman@fmi.fi> - 16.4.20-2.fmi
- Fix paramname insert in spatialite

* Wed Apr 20 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.20-1.fmi
- Added flash caching

* Thu Apr 14 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.14-1.fmi
- Fixed SnowDepth to be 1 minute instant value

* Mon Apr  4 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.4-2.fmi
- Start using a newer database view on observation data caching.

* Mon Apr  4 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.4.4-1.fmi
- Start using new measurand id values for daily rain and snow parameters.

* Fri Mar  4 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.3.4-1.fmi
- Added a possibility to disable spatialite caching.

* Mon Feb 22 2016  Santeri Oksman <santeri.oksman@fmi.fi> - 16.2.22-2.fmi
- Fix flash queries

* Mon Feb 22 2016  Santeri Oksman <santeri.oksman@fmi.fi> - 16.2.22-1.fmi
- Various fixes to timeseries generation.

* Tue Feb  9 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.2.9-1.fmi
- Rebuilt against the new TimeSeries::Value definition

* Mon Feb  8 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.2.8-1.fmi
- Fixed missing value handling

* Tue Feb  2 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.2.2-1.fmi
- Pointers to std::shared_ptr replacement of QueryResult container (API change)
- Returning ts::None values if wmo, plnn, or rwsid is invalid.
- Removed stattime rounding to full hours
- Now using Timeseries None - type

* Sat Jan 23 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.23-1.fmi
- Fmi::TimeZoneFactory API changed

* Fri Jan 22 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.22-1.fmi
- Optimized the algorithm for fetching delayed observations into local cache

* Thu Jan 21 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.1.21-2.fmi
- Road weather searches does not require producer ids. (INSPIRE-740)

* Thu Jan 21 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.21-1.fmi
- Report to blog the number of rows updated for each cache update

* Wed Jan 20 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.1.20-2.fmi
- Removed parameter snow from opendata_daily configuration

* Wed Jan 20 2016 Santeri Oksman <santeri.oksman@fmi.fi> - 16.1.20-1.fmi
- Added GroundTemperature06, fixes to model and modtime parameters

* Mon Jan 18 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.18-1.fmi
- newbase API changed, full recompile

* Fri Jan 15 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.15-1.fmi
- Fixed directory permissions for the include files

* Wed Jan 13 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.14-1.fmi
- Removed an extra comma from a generation station id list.
- Improved safety against missing FMISID information in the fminames database

* Wed Jan 13 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.13-1.fmi
- Rewrote spatialite locking to to minimize lock contestion

* Thu Dec 10 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.12.10-2.fmi
- Fixed problem with timestep=0

* Thu Dec 10 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.12.10-1.fmi
- Generate time intervals in C++ code, not in database.

* Tue Dec  8 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.12.8-1.fmi
- Try to fix lock problems in SpatiaLite connection pool

* Mon Dec  7 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.12.7-1.fmi
- Fix a timestep bug in SpatiaLite data searches.

* Thu Dec  3 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.12.2-2.fmi
- Reduced hp Oracle pool size

* Wed Dec  2 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.12.2-1.fmi
- Now using new Oracle service definitions

* Tue Dec  1 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.12.1-2.fmi
- Use ST_EvvIntersects for speed when looking for stations inside a bounding box

* Tue Dec  1 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.12.1-1.fmi
- Fixed station direction calculation in Spatialite

* Thu Nov 19 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.11.19-1.fmi
- Add RoadTemperature parameter

* Wed Nov 18 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.11.18-1.fmi
- More station types

* Wed Nov 11 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.11.11-1.fmi
- Fixed a bug in spatialite timestep when facing missing values

* Tue Nov 10 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.11.10-1.fmi
- Avoid string streams to avoid global std::locale locks

* Mon Nov  9 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.11.9-1.fmi
- Using fast case conversion without locale locks when possible

* Fri Nov  6 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.11.6-1.fmi
- Added some more parameters

* Tue Nov  3 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.11.3-1.fmi
- Stopped using deprecated Cast.h functions

* Mon Oct 26 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.10.26-1.fmi
- Added proper debuginfo packaging

* Thu Oct 22 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.10.22-1.fmi
- Fixed wawa code configuration and a problem with observation cache update

* Tue Oct 20 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.10.20-1.fmi
- Cache also weather_data_qc data (i.e. road and foreign stationtypes)
- Version number the sqlite file,  starting with 1
- Add places=all functionality for spatialite station searches

* Wed Oct 14 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.10.14-1.fmi
- Added producer id 28 to station types which present observations from AVI group

* Tue Oct 13 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.10.13-2.fmi
- Now blocking concurrent writes to sqlite database

* Tue Oct 13 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.10.13-1.fmi
- Added TA_1h_AVH parameter to configuration

* Fri Oct  2 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.10.2-1.fmi
- Added snow06 parameter to configuration

* Tue Sep 29 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.9.29-1.fmi
- Opendata updates
- Converting Oracle data values to integrals (long) if scale is 0 (Oracle::get).

* Wed Sep 16 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.9.16-1.fmi
- Fixed station searches for stations which are not currently operating

* Tue Sep 15 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.9.15-1.fmi
- Fixed feelslike-parameter in open data queries

* Mon Aug 31 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.8.31-1.fmi
- Fixed WindCompass handling

* Tue Aug 25 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.25-2.fmi
- Made the API more const correct

* Tue Aug 25 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.8.25-1.fmi
- Added parameter configurations

* Thu Aug 20 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.20-1.fmi
- Added feelslike-parameter
- Bugfixes

* Wed Aug 19 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.19-1.fmi
- Fixed 'place' - parameter

* Tue Aug 18 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.18-1.fmi
- Use time formatters from macgyver to avoid global locks from sstreams

* Mon Aug 17 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.17-1.fmi
- Use -fno-omit-frame-pointer to improve perf use

* Fri Aug 14 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.14-2.fmi
- Avoid std::ostringstream, boost::lexical_cast and Fmi::number_cast

* Fri Aug 14 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.14-1.fmi
- Fixed connection leaking from Connection Pools

* Tue Aug  4 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.4-1.fmi
- A station can belong to many station groups now
- Now supporting place-parameter

* Tue Jul 21 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.7.21-1.fmi
- Recompiled to get devel and debuginfo packages too

* Tue Jul  7 2015 Roope Tervo <roope.tervo@fmi.fi> - 15.7.7-1.fmi 
- Added tmin06, tmax06 and tmin18

* Mon Jul  6 2015 Roope Tervo <roope.tervo@fmi.fi> - 15.7.6-1.fmi 
- Added tmax18

* Fri Jun 26 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.6.26-1.fmi
- Added tamin12h, tamax12h, tamin24h, tamax24h

* Tue Jun 23 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.6.23-1.fmi
- Location object API changed, recompile forced

* Mon May 18 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.5.18-1.fmi
- 'foreign' station type is now allowed for bounding box searches

* Wed Apr 29 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.4.29-1.fmi
- Recompiled since Location object has a new data member

* Wed Apr 22 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.22-1.fmi
- Added parameter configurations

* Fri Apr 17 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.17-2.fmi
- Do not try to read null values from cldb when updating cache

* Fri Apr 17 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.17-1.fmi
- Spec file changed: do not overwrite sqlite file

* Thu Apr 16 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.16-2.fmi
- Relax a little the parameter configuration existence demand in producer=fmi queries

* Thu Apr 16 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.16-1.fmi
- Fix fmisid searches in timeseries

* Wed Apr 15 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.4.15-1.fmi
- Added fmi-parameters needed for icemap generation

* Tue Apr 14 2015 Santeri Oksman <santeri.oksman@fmi.fi> - 15.4.14-1.fmi
- New release to enable observation cache
- cnf/stations.xml file update.

* Thu Apr  9 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.4.9-1.fmi
- newbase API changed

* Wed Apr  8 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.4.8-1.fmi
- Dynamic linking of smartmet libraries into use

* Mon Feb 23 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.2.23-1.fmi
- Added configuration variable quiet whose default value is true

* Wed Feb 18 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.2.18-1.fmi
- Now using wasabi instead of uramaki for 'avoindata'

* Tue Feb 17 2015 Jukka A. Pakarinen <jukka.pakarinen@fmi.fi> - 15.2.17-1.fmi
- PAK-437 Measurand conversion bug fix. 

* Mon Feb 16 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.2.16-1.fmi
- Added geoid, wmo and lpnn codes into station.sqlite db.
- Into the SpatiaLite stationType method added an empty station type (station type ignored)
- Geoid, wmo ja lpnn station identities stored into SpatiaLite database.
- SpatiaLite class extented with fillMissing and getStationById methods.

* Mon Jan 26 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.1.26-1.fmi
- Added the Height of base of cloud (CLHB) parameter configuration.
- Pecipitation "PREC" stations got stationType.

* Wed Jan  7 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.1.7-1.fmi
- Disable cache of Table objects for not being thread-safe

* Wed Dec 17 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.12.17-1.fmi
- Fixes to variant table generation: prefer doubles instead of strings
- NamesAllowed class created and VerifiableMessageQuery using it.
- VerifiableMessageQuery changed to use DBRegistry.
- DBRegistry to store database table information used in queries.

* Thu Nov 13 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.11.13-1.fmi
- Recompiled due to newbase API changes

* Thu Oct 23 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.10.23-1.fmi
- Added current weather parameter wawa (56) configuration (opendata and commercial).

* Wed Oct 22 2014 Tuomo Lauri <tuomo.lauri@fmi.fi> - 14.10.22-1.fmi
- Fixed rounding issues in latlon-like searches

* Tue Sep 16 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.9.16-1.fmi
- Added missing soci dependency

* Wed Sep 10 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.9.10-2.fmi
- Added install of station cache files

* Wed Sep 10 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.9.10-1.fmi
- Fix to use stations only in valid time range in spatialite searches

* Tue Sep  9 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.9.9-1.fmi
- Locations for stations are now searched from local SpatiaLite/SQLite database.

* Mon Sep  8 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.9.8-1.fmi
- Recompiled due to geoengine API changes

* Tue Jul 1 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.7.1-1.fmi
- IWXXM message query implementation (VerifiableMessageQuery).
- Container class that store boost:any data.
- New way to fetch data from DB.

* Mon Jun 30 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.6.30-1.fmi
- New release to get ObsEngine::getValidStationTypes in

* Thu Jun 19 2014 Jukka A. Pakarinen <jukka.pakarinen@fmi.fi> - 14.6.19-1.fmi
- Hotfix: WMO code cache fix.

* Thu May 15 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.5.15-1.fmi
- Use opendata server until cldb problems have been resolved

* Wed May 14 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.5.14-1.fmi
- Use shared macgyver and locus libraries

* Fri May  9 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.5.9-1.fmi
- Recompiled to get latest locus

* Mon Apr 28 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.4.28-1.fmi
- Full recompile due to large changes in spine etc APIs

* Wed Apr 23 2014 Mika Heiskanen <mika.heiskanen@fmi.fi> - 14.4.23-1.fmi
- Hotfix: Fixed bug in flash reader error handler

* Thu Apr 10 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.4.10-2.fmi
- Hotfix: Duplicate parameter alias remove and startup check.

* Thu Apr 10 2014 Anssi Reponen <anssi.reponen@fmi.fi> 14.4.10-1.fmi
- Support for timeseries plugin added

* Thu Mar 20 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.3.20-1.fmi
- Open data 2014-04-01 release

* Thu Mar  6 2014 Tuomo Lauri <tuomo.lauri@fmi.fi> - 14.3.6-1.fmi
- Hotfix: Removed duplicate RH alias parameter definition from engine configuration files.

* Thu Feb 27 2014 Tuomo Lauri <tuomo.lauri@fmi.fi> - 14.2.27-2.fmi
- Updated config for foreign wfs observations

* Thu Feb 27 2014 Tuomo Lauri <tuomo.lauri@fmi.fi> - 14.2.27-1.fmi
- Added wind parameters
- User foreced to give stationType input parameter for isParameter method.

* Fri Feb 14 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.2.14-1.fmi
- Fixed geoid return values in opendata bbox searches

* Wed Feb 5 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.2.5-1.fmi
- Added a missing livi parameter configuration RH_3 and fmi parameter N_MAN.

* Mon Feb 3 2014 Mikko Visa <mikko.visa@fmi.fi> - 14.2.3-2.fmi
- Open data 2014-02-03 release
- Quality code parameter support implemented by using 'qc_' prefix.

* Mon Jan 13 2014 Santeri Oksman <santeri.oksman@fmi.fi> - 14.1.13-1.fmi
- Added a method that check that a parameter alias name is configured in the engine.

* Thu Dec 12 2013 Tuomo Lauri <tuomo.lauri@fmi.fi> - 13.12.12-2.fmi
- Configured multiple parameters of 30-year normal periods.
- Configured 3 sun radiation parameters.

* Tue Dec  3 2013 Santeri Oksman <santeri.oksman@fmi.fi> - 13.12.3-2.fmi
- Remove duplicate stations

* Tue Dec  3 2013 Santeri Oksman <santeri.oksman@fmi.fi> - 13.12.3-1.fmi
- Prune stations with no lpnn number when doing queries to tables which have lpnn as identifier.

* Mon Dec  2 2013 Santeri Oksman <santeri.oksman@fmi.fi> - 13.12.2-1.fmi
- Fixed missing epochtime from opendata queries, fixed broken cachekey in latlon station searches.

* Fri Nov 29 2013 Santeri Oksman <santeri.oksman@fmi.fi> - 13.11.29-1.fmi
- Fixed issue with latitude and longitude searches

* Thu Nov 28 2013 Tuomo Lauri <tuomo.lauri@fmi.fi> - 13.11.28-1.fmi
- Fixed issue with negative geoids

* Mon Nov 25 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.11.25-1.fmi
- Added soil parameter configurations.

* Thu Nov 14 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.11.14-1.fmi
- Ported to use locus instead of fminames

* Tue Nov  5 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.11.5-1.fmi
- Make possible to query also stations which are no longer in existence.
- Added SYKE water temperature stations.

* Wed Oct 9 2013 Tuomo Lauri     <tuomo.lauri@fmi.fi>    - 13.10.9-1.fmi
- Now conforming with the new Reactor initialization API

* Mon Sep 23 2013 Tuomo Lauri    <tuomo.lauri@fmi.fi>    - 13.9.23-1.fmi
- Various bugfixes
- Fixed a parameter name parsing.

* Tue Sep 17 2013 Santeri Oksman <santeri.oksman@fmi.fi> - 13.9.17-1.fmi
- Fixed a broken cache key in latlon station searches.

* Fri Sep 6  2013 Tuomo Lauri    <tuomo.lauri@fmi.fi>    - 13.9.6-1.fmi
- Recompiled due Spine changes

* Fri Aug 30 2013 Tuomo Lauri    <tuomo.lauri@fmi.fi>    - 13.8.30-1.fmi
- Rebuilt against the non-caching fminames

* Tue Aug 13 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.8.13-1.fmi
- Disabled caching of observations until Oracle data members are included in the cache key

* Mon Aug 12 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.8.12-1.fmi
- Fixed fetching observations when there are connection breaks

* Thu Aug  8 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.8.9-1.fmi
- Added caching of observations for a short period of time

* Thu Aug  8 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.8.8-1.fmi
- Added caching of metadata
- Removed an unnecessary scoped lock when releasing an Oracle connection to the pool

* Tue Jul 23 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.7.23-1.fmi
- Recompiled due to thread safety fixes in newbase & macgyver

* Wed Jul  3 2013 Mika Heiskanen <mika.heiskanen@fmi.fi> - 13.7.3-2.fmi
- Update to boost 1.54

* Wed Jul  3 2013 Tuomo Lauri    <tuomo.lauri@fmi.fi>    - 13.7.3-1.fmi
- OracleConnectionPool uses locks less frequently

* Mon Jun 17 2013 Tuomo Lauri    <tuomo.lauri@fmi.fi>    - 13.6.17-1.fmi
- Added WTP and WHDD parameters to buoy observations.

* Fri Jun  7 2013 Andris Pavenis <andris.pavenis@fmi.fi> - 13.6.7-1.fmi
- Fix crash when observations queried by non-existing fmisid

* Mon Jun  3 2013 Tuomo Lauri <tuomo.lauri@fmi.fi> - 13.6.3-1.fmi
- Built against the new Spine

* Wed May 29 2013 Roope Tervo <roope.tervo@fmi.fi> - 13.5.29-2.fmi
- Repaired windspeed configuration

* Wed May 29 2013 Tuomo Lauri <tuomo.lauri@fmi.fi> - 13.5.29-1.fmi
- Added possibility to query all ObservableProperties
- Added configuration for multiple road weather parameters.
- Added multiple underscore support for parameter names.

* Wed May 22 2013 Andris Pavenis <andris.pavenis@fmi.fi> - 13.5.22-1.fmi
- Add user API for selecting stations by bounding box and maximal distance

* Tue May 21 2013 oksman <santeri.oksman@fmi.fi> - 13.5.21-1.fmi
- Support the case that multiple gml ids use same observable property.

* Fri May 17 2013 oksman <santeri.oksman@fmi.fi> - 13.5.17-1.fmi
- One more fix to meta query reconnection.

* Thu May 16 2013 oksman <santeri.oksman@fmi.fi> - 13.5.16-1.fmi
- Added language support to meta queries.

* Wed May 15 2013 oksman <santeri.oksman@fmi.fi> - 13.5.15-1.fmi
- Release candidate of open data

* Tue May 14 2013 oksman <santeri.oksman@fmi.fi> - 13.5.14-1.fmi
- Serialize stations used in bounding box searches to disk and unserialize them in startup.

* Mon May 13 2013 oksman <santeri.oksman@fmi.fi> - 13.5.13-2.fmi
- Removed forgotten debug messages.

* Mon May 13 2013 oksman <santeri.oksman@fmi.fi> - 13.5.13-1.fmi
- Catch errors in database queries and release connections to pool if needed.

* Sat May 11 2013 oksman <santeri.oksman@fmi.fi> - 13.5.11-3.fmi
- Fixes to metadata and opendata queries.

* Sat May 11 2013 oksman <santeri.oksman@fmi.fi> - 13.5.11-2.fmi
- Fixes to open data parameter configuration.

* Sat May 11 2013 oksman <santeri.oksman@fmi.fi> - 13.5.11-1.fmi
- A lot of fixes to get open data queries working.

* Wed May 08 2013 oksman <santeri.oksman@fmi.fi> - 13.5.8-3.fmi
- Enhanced error handling.

* Wed May 08 2013 oksman <santeri.oksman@fmi.fi> - 13.5.8-2.fmi
- Fixed a bug in location cache handling.

* Wed May 08 2013 oksman <santeri.oksman@fmi.fi> - 13.5.8-1.fmi
- Support more parameters and enhance time handling in open data queries.

* Thu May 2 2013 oksman <santeri.oksman@fmi.fi> - 13.5.2-1.fmi
- Fixes to flash time handling. Try really hard to reconnect if connection to Oracle is lost.

* Tue Apr 30 2013 oksman <santeri.oksman@fmi.fi> - 13.4.30-1.fmi
- Try to reconnect if connection to Oracle is lost. Fixes to meta queries.

* Tue Apr 23 2013 oksman <santeri.oksman@fmi.fi> - 13.4.23-1.fmi
- Fixes to connection pool.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-8.fmi
- Put results into cache.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-7.fmi
- Return empty result if there are no Stations. Use result cache.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-6.fmi
- Trying to fix a bug regarding session_begin.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-5.fmi
- Release the connection even when an error occurs in the query.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-4.fmi
- Even more fixes to configuration.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-3.fmi
- More fixes to configuration.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-2.fmi
- Added missing configuration.

* Mon Apr 22 2013 oksman <santeri.oksman@fmi.fi> - 13.4.22-1.fmi
- New beta release.

* Wed Apr 17 2013 oksman <santeri.oksman@fmi.fi> - 13.4.17-1.fmi
- Test build

* Fri Apr 12 2013 lauri <tuomo.lauri@fmi.fi>    - 13.4.12-1.fmi
- Built against the new Spine

* Tue Apr 9 2013 oksman <santeri.oksman@fmi.fi> - 13.4.9-1.el6.fmi
- Enabled the result cache and removed debug prints.

* Mon Apr 8 2013 oksman <santeri.oksman@fmi.fi> - 13.4.8-1.el6.fmi
- New beta release.

* Wed Mar 20 2013 tervo <roope.terv@fmi.fi> - 13.3.20-1.el6.fmi
- Added Oracle service configuration

* Sat Mar 16 2013 tervo <roope.terv@fmi.fi> - 13.3.16-1.el6.fmi
- Added separate configuration files for open data and commercial use.

* Thu Mar 14 2013 oksman <santeri.oksman@fmi.fi> - 13.3.14-1.el6.fmi
- New build from develop branch.

* Wed Mar 13 2013 oksman <santeri.oksman@fmi.fi> - 13.3.13-1.el6.fmi
- Fixed a bug of missing direction parameter

* Thu Mar  7 2013 mheiskan <mika.heiskanen@fmi.fi> - 13.3.7-1.fmi
- Recompiled with latest fminames library

* Mon Feb 25 2013 oksman <santeri.oksman@fmi.fi> - 13.2.25-1.el6.fmi
- Added monthly observations.

* Wed Feb 13 2013 oksman <santeri.oksman@fmi.fi> - 13.2.13-2.el6.fmi
- Fixed buoy and mareograph searches with geoid.

* Wed Feb 13 2013 oksman <santeri.oksman@fmi.fi> - 13.2.13-1.el6.fmi
- Fixed stationtype number for Elering, was 261(?) is now 142 (EXTSYNOP), same as for all foreign stations.

* Wed Feb  6 2013 lauri  <tuomo.lauri@fmi.fi>    - 13.2.6-1.fmi
- Built against new Spine and Server

* Thu Nov 29 2012 oksman <santeri.oksman@fmi.fi> - 12.11.29-2.el6.fmi
- Use stationtype = -5 when getting stations by coordinates (using weather_qc table). This includes AWS, SYNOP and CLIM, AVI station types.

* Thu Nov 29 2012 oksman <santeri.oksman@fmi.fi> - 12.11.29-1.el6.fmi
- Removed forgotten debug print.

* Wed Nov 28 2012 oksman <santeri.oksman@fmi.fi> - 12.11.28-1.el6.fmi
- Allow only one of each weather parameters per row.

* Mon Nov 26 2012 oksman <santeri.oksman@fmi.fi> - 12.11.26-1.el6.fmi
- Added support for buoy observations
- Support 12 and 24 hour rain sum parameters
- Early version of flash data support.

* Thu Nov 22 2012 oksman <santeri.oksman@fmi.fi> - 12.11.22-1.el6.fmi
- Bug fix for station id conversions.

* Wed Nov 21 2012 oksman <santeri.oksman@fmi.fi> - 12.11.21-1.el6.fmi
- Removed SP_HttpRequest dependency from obsengine.
- Lots of refactoring in the code.

* Wed Nov  7 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.11.7-1.el6.fmi
- Upgrade to boost 1.52
- Upgrade to refactored spine library

* Thu Sep 27 2012 oksman <santeri.oksman@fmi.fi> - 12.9.27-1.el6.fmi
- Enabled bounding box searches.

* Wed Sep 12 2012 oksman <santeri.oksman@fmi.fi> - 12.9.12-1.el6.fmi
 - Added mareograph stationtype.
- Solar stations have no longer own stationtype.
- Code for bounding box station search which is not yet completely implemented.

* Wed Aug 29 2012 oksman <santeri.oksman@fmi.fi> - 12.8.29-1.el6.fmi
- Fixed a bug in station fetching.

* Thu Aug 16 2012 oksman <santeri.oksman@fmi.fi> - 12.8.16-1.el6.fmi
- Sensor number selecting is supported only for foreign and road data.

* Wed Aug  15 2012 oksman <santeri.oksman@fmi.fi> - 12.8.15-1.el6.fmi
- Added support for selecting sensor number in weather_data_qc queries.

* Wed Aug  8 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.8.8-2.el6.fmi
- FmiNames library updated

* Wed Aug  8 2012 lauri    <tuomo.lauri@fmi.fi>    - 12.8.8-1.el6.fmi
- Location API change

* Tue Jul 10 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.7.10-2.el6.fmi
- Do not return an empty shared_ptr<Table>, but one with a zero-size Table instead

* Tue Jul 10 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.7.10-1.el6.fmi
- Do not throw if there are no stations near enough, just return empty data

* Thu Jul  5 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.7.5-1.el6.fmi
- Upgrade to boost 1.50

* Fri Jun 29 2012 oksman <santeri.oksman@fmi.fi> - 12.6.29-1.el6.fmi.fi
- Don't try to get data if no stations are found, throw instead.

* Wed Jun 27 2012 oksman <santeri.oksman@fmi.fi> - 12.6.27-1.el6.fmi.fi
- Fix encoding of station names which come from Oracle database.

* Tue Jun 19 2012 oksman <santeri.oksman@fmi.fi> - 12.6.19-1.el6.fmi.fi
- numberofstations request parameter works now with wmo search too.

* Mon Jun 18 2012 oksman <santeri.oksman@fmi.fi> - 12.6.18-1.el6.fmi.fi
- numberofstations request parameter works now with geoid search too.

* Tue May 29 2012 oksman <santeri.oksman@fmi.fi> - 12.5.29-2.el6.fmi
- Added region output field.

* Tue May 29 2012 oksman <santeri.oksman@fmi.fi> - 12.5.29-1.el6.fmi
- Changes to timestep and starttime logic. New functionality: 1) If timestep is given, fill missing observations with missingvalue. 2) If timestep=all, return all observations within the time interval, nothing more. 3) If no starttime is given, use one with 24 hours backwards, floored to even hour. 4) Use the same logic in timestep/starttime values as in pointforecast.

* Thu May 10 2012 oksman <santeri.oksman@fmi.fi> - 12.5.10-1.el6.fmi
- Fixed a bug in distance parameter when using endtime=now.

* Thu Apr 19 2012 oksman <santeri.oksman@fmi.fi> - 12.4.19-2.el6.fmi
- Added safety against empty stationdistances

* Thu Apr 19 2012 oksman <santeri.oksman@fmi.fi> - 12.4.19-1.el6.fmi
- Fixed distance and rain parameter manipulation in inner sql in stationtype=fmi.

* Wed Apr 18 2012 oksman <santeri.oksman@fmi.fi> - 12.4.18-1.el6.fmi
- Sort the data first by station distance from given point.
- Accept precipitation parameters with different caps.

* Wed Apr  4 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.4.4-1.el6.fmi
- common libs changed

* Mon Apr  2 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.4.2-1.el6.fmi
- macgyver change forced recompile

* Sat Mar 31 2012 mheiskan <mika.heiskanen@fmi.fi> - 12.3.31-1.el5.fmi.fi
- Upgrade to boost 1.49

* Tue Mar  6 2012 oksman <santeri.oksman@fmi.fi> - 12.3.6-1.el5.fmi.fi
- Moved location solving for geoids from obsplugin.
- Reformatted station finding methods.
- If geoid is given in querystring, preserve it in any case.

* Fri Feb 24 2012 oksman <santeri.oksman@fmi.fi> - 12.2.24-1.el5.fmi.fi
- Calculate timestep in sql queries.

* Wed Feb 15 2012 oksman <santeri.oksman@fmi.fi> - 12.2.15-1.el5.fmi.fi
- Added weekday option and parameter.
- Added locale option.

* Thu Feb 9 2012 oksman <santeri.oksman@fmi.fi> - 12.2.9-1.el5.fmi.fi
- Use given tz for hour option, not always utc.

* Wed Jan 18 2012 oksman <santeri.oksman@fmi.fi> - 12.1.18-1.el5.fmi.fi
- Rewrote the query for getting weather_data_qc observations.
- Fixed humidity parameter name in road observation table.

* Thu Jan 12 2012 oksman <santeri.oksman@fmi.fi> - 12.1.12-1.el5.fmi.fi
- Get the original road station id with param=rwsid.

* Thu Jan 5 2012 oksman <santeri.oksman@fmi.fi> - 12.1.5-2.el5.fmi.fi
- A fix to Elering observations.

* Thu Jan 5 2012 oksman <santeri.oksman@fmi.fi> - 12.1.5-1.el5.fmi.fi
- Added possibility to get also rain parameters when using stationtype=fmi.

* Tue Dec 27 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.12.27-3.el5.fmi
- Bug fix to common Table class

* Tue Dec 27 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.12.27-1.el5.fmi
- fminames recompile

* Fri Dec 23 2011 oksman <santeri.oksman@fmi.fi> - 11.12.23-1.el5.fmi
- Fixed a bug regarding the new Table class.

* Thu Dec 22 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.12.22-1.el6.fmi
- Recompiled with latest fminames library

* Wed Dec 21 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.12.21-1.el6.fmi
- RHEL6 release

* Tue Dec 20 2011 oksman <santeri.oksman@fmi.fi> - 11.12.20-1.el5.fmi
- Fixed latest observation fetching.

* Fri Dec 9 2011 oksman <santeri.oksman@fmi.fi> - 11.12.9-4.el5.fmi
- Geoid search fixed to EMHI observations.

* Fri Dec 9 2011 oksman <santeri.oksman@fmi.fi> - 11.12.9-3.el5.fmi
- Fixed a bug with WMO numbers.

* Fri Dec 9 2011 oksman <santeri.oksman@fmi.fi> - 11.12.9-2.el5.fmi
- Few tweaks for EMHI observations.

* Fri Dec 9 2011 oksman <santeri.oksman@fmi.fi> - 11.12.9-1.el5.fmi
- Added EMHI stations with stationtype=elering.
- Added support for RWSID identifiers.

* Tue Nov 29 2011 oksman <santeri.oksman@fmi.fi> - 11.11.29-3.el5.fmi
- Removed debug prints.

* Tue Nov 29 2011 oksman <santeri.oksman@fmi.fi> - 11.11.29-2.el5.fmi
- Fixed a bug in latest observation fetching from road and foreign stations. 

* Tue Nov 29 2011 oksman <santeri.oksman@fmi.fi> - 11.11.29-1.el5.fmi
- Added astronomy parameters.
- Fixed geoid fetching for also other stationtypes than fmi.

* Wed Nov 23 2011 oksman <santeri.oksman@fmi.fi> - 11.11.23-1.el5.fmi
- Fixes to latitude and longitude fetching when using wmo place identifier.
- Time zone respects now correctly the localtime setting.

* Tue Nov 22 2011 oksman <santeri.oksman@fmi.fi> - 11.11.22-1.el5.fmi
- Fixed a bug in elevation handling in weather_data_qc table queries.
- Marked Wconversion flag as difficult in the Makefile.

* Mon Nov 21 2011 oksman <santeri.oksman@fmi.fi> - 11.11.21-1.el5.fmi
- Added elevation parameter to queries from weather_data_qc table.
- Fixed time zone handling.
- Use only SYNOP class when getting locations with geoids.

* Thu Sep 29 2011 oksman <santeri.oksman@fmi.fi> - 11.9.29-1.el5.fmi
- Add elevation to weather_data_qc queries.

* Wed Sep 28 2011 oksman <santeri.oksman@fmi.fi> - 11.9.28-1.el5.fmi
- Bug fix: query with several tables with stationtype=fmi does not work as is, commented it out.

* Mon Sep 26 2011 oksman <santeri.oksman@fmi.fi> - 11.9.26-1.el5.fmi
- Added rain and daily observations when using stationtype=fmi.

* Wed Sep 21 2011 oksman <santeri.oksman@fmi.fi> - 11.9.21-4.el5.fmi
- Fixed release number.

* Wed Sep 21 2011 oksman <santeri.oksman@fmi.fi> - 11.9.21-3.el5.fmi
- Fixes to daily observations.

* Wed Sep 21 2011 oksman <santeri.oksman@fmi.fi> - 11.9.21-2.el5.fmi
- Removed debug output.

* Wed Sep 21 2011 oksman <santeri.oksman@fmi.fi> - 11.9.21-1.el5.fmi
- New RPM for RHEL5

* Tue Sep 20 2011 oksman <santeri.oksman@fmi.fi> - 11.9.20-2.el5.fmi
- Added daily observations.

* Tue Sep 20 2011 oksman <santeri.oksman@fmi.fi> - 11.9.20-1.el5.fmi
- Added sounding observations and param=level.

* Mon Sep 19 2011 oksman <santeri.oksman@fmi.fi> - 11.9.19-1.el5.fmi
- Added option lang and param=country.
- Added MySQL and FmiNames dependency.

* Tue Sep 13 2011 oksman <santeri.oksman@fmi.fi> - 11.9.13-1.el5.fmi
- Several small changes: 1) Added hours option. 2) Added error messages when using debug mode. 3) Fixed time zone handling. 4) Enhanced timestep option. 5) Added defaults to time options. 6) Default stationtype=fmi.

* Tue Aug 16 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.8.16-1.el5.fmi
- Upgrade to boost 1.47

* Tue Jul 26 2011 oksman <santeri.oksman@fmi.fi> - 11.7.26-2.el5.fmi
- Added direction parameter which tells the direction of the station from given coordinates.

* Tue Jul 26 2011 oksman <santeri.oksman@fmi.fi> - 11.7.26-1.el5.fmi
- Coordinate parameters stationlat and stationlon work now also with station_type=fmi.

* Thu Jul 21 2011 oksman <santeri.oksman@fmi.fi> - 11.7.21-1.el5.fmi
- Foreign stations are available with station_type=foreign.

* Wed Jul 20 2011 oksman <santeri.oksman@fmi.fi> - 11.7.20-1.el5.fmi
- Recognize iso2 and region parameters.

* Wed Jul 13 2011 oksman <santeri.oksman@fmi.fi> - 11.7.13-1.el5.fmi
- Fixes to parameter names, issues BRAINSTORM-170 - BRAINSTORM-172.

* Fri Jul 8 2011 oksman <santeri.oksman@fmi.fi> - 11.7.8-3.el5.fmi
- Even more debug messages removed.

* Fri Jul 8 2011 oksman <santeri.oksman@fmi.fi> - 11.7.8-2.el5.fmi
- Removed forgotten debug messages.

* Fri Jul 8 2011 oksman <santeri.oksman@fmi.fi> - 11.7.8-1.el5.fmi
- Added option to choose n nearest observation stations.

* Mon Jun 6 2011 oksman <santeri.oksman@fmi.fi> - 11.6.6-1.el5.fmi
- Added few parameter name alternatives to configuration file

* Tue May 31 2011 oksman <santeri.oksman@fmi.fi> - 11.5.31-2.el5.fmi
- Fixed a bug in station search for hourly observations

* Tue May 31 2011 oksman <santeri.oksman@fmi.fi> - 11.5.31-1.el5.fmi
- Added hourly observations and rudimentary timestep handling

* Thu Mar 24 2011 mheiskan <mika.heiskanen@fmi.fi> - 11.3.24-4.el5.fmi
- Upgrade to boost 1.46

* Thu Mar 24 2011 oksman <santeri.oksman@fmi.fi> - 11.3.24-3.el5.fmi
- More fixes to WXML format. One can also ask same parameter many times.

* Thu Mar 24 2011 oksman <santeri.oksman@fmi.fi> - 11.3.24-2.el5.fmi
- Added param=geoid option.

* Thu Mar 24 2011 oksman <santeri.oksman@fmi.fi> - 11.3.24-1.el5.fmi
- Fixes to timezone handling. Now also param=time,epochtime,localtime works.

* Wed Mar 23 2011 oksman <santeri.oksman@fmi.fi> - 11.3.23-1.el5.fmi
- Fixed WXML functionality and added time zone support.

* Mon Mar 21 2011 oksman <santeri.oksman@fmi.fi> - 11.3.21-1.el5.fmi
- Enhanched support for different place selectors. LPNN numbers can be used also for place identification.

* Wed Mar 16 2011 oksman <santeri.oksman@fmi.fi> - 11.3.16-2.el5.fmi
- Do not limit station group in getStation method.

* Wed Mar 16 2011 oksman <santeri.oksman@fmi.fi> - 11.3.16-1.el5.fmi
- Added stationtype=lammitystarve switch. Also a few fixes for station id gathering logic.

* Mon Feb 21 2011 oksman <santeri.oksman@fmi.fi> - 11.2.21-1.el5.fmi
- Added ld configuration path for Oracle libraries and ldconfig to post script.

* Fri Feb 18 2011 oksman <santeri.oksman@fmi.fi> - 11.2.18-1.el5.fmi
- Lat and lon available parameters for also option places=all. Also few parameter name fixes to be more compatible with pointforecast.

* Wed Feb 16 2011 oksman <santeri.oksman@fmi.fi> - 11.2.16-1.el5.fmi
- Added support for getting latest road observations.

* Fri Feb 11 2011 oksman <santeri.oksman@fmi.fi> - 11.2.11-1.el5.fmi
- Support to solar radiation measures and usage of wmo numbers as place identifiers with them.

* Wed Feb 9 2011 oksman <santeri.oksman@fmi.fi> - 11.2.9-1.el5.fmi
- Added places=all option which gets all stations for given station type.

* Tue Feb 8 2011 oksman <santeri.oksman@fmi.fi> - 11.2.8-1.el5.fmi
- Initial version
