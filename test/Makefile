PROG = $(patsubst %.cpp,%,$(wildcard *SpatiaLiteTest.cpp))

MAINFLAGS = -std=c++11 -Wall -W -Wno-unused-parameter -Wno-unknown-pragmas

EXTRAFLAGS = -Werror -pedantic -Wpointer-arith -Wcast-qual \
	-Wcast-align -Wwrite-strings -Wconversion -Winline \
	-Wctor-dtor-privacy -Wnon-virtual-dtor -Wno-pmf-conversions \
	-Wsign-promo -Wchar-subscripts -Wold-style-cast \
	-Wshadow -Woverloaded-virtual

DIFFICULTFLAGS = -Weffc++ -Wunreachable-code -Wredundant-decls

CC = g++
CFLAGS = -DUNIX -O0 -g $(MAINFLAGS)

# SOURCES = MainTest.cpp \
# 	../source/SpatiaLite.cpp \
# 	../source/ObservationUtils.cpp \
# 	../source/Settings.cpp

SOURCES = MainTest.o /usr/share/smartmet/engines/geonames.so ../observation.so

INCLUDES = \
	-I ../include \
	-I /usr/include/smartmet \
	-I/usr/include/oracle/11.2/client64 \
	-I /usr/include/soci \
	`pkg-config --cflags libconfig++`

LIBS = \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lsmartmet-locus \
	-lsmartmet-newbase \
	`pkg-config --libs libconfig++` \
	-lpqxx \
	-lboost_date_time \
	-lboost_thread \
	-lboost_filesystem \
	-lboost_serialization \
	-lboost_regex \
	-lboost_iostreams \
	-lboost_system \
	-lsqlite3 \
	-lsoci_core \
	-lsoci_sqlite3 \
	`pkg-config --libs spatialite` \
	-lbz2 -lz -lpthread

all: 	$(PROG)
clean:
	rm -f $(PROG) *~

test: $(PROG)
	@echo Running tests:
	@for prog in $(PROG); do \
	./$$prog -s; \
	done

$(PROG) : % : %.cpp ../observation.so /usr/share/smartmet/engines/geonames.so
	$(CC) -c MainTest.cpp
	$(CC) $(CFLAGS) $(SOURCES) -o $@ $@.cpp $(INCLUDES) $(LIBS)
