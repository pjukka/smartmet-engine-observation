SUBNAME = observation
SPEC = smartmet-engine-$(SUBNAME)
INCDIR = smartmet//engines/$(SUBNAME)

# Installation directories

processor := $(shell uname -p)

ifeq ($(origin PREFIX), undefined)
  PREFIX = /usr
else
  PREFIX = $(PREFIX)
endif

ifeq ($(processor), x86_64)
  libdir = $(PREFIX)/lib64
else
  libdir = $(PREFIX)/lib
endif

bindir = $(PREFIX)/bin
includedir = $(PREFIX)/include
datadir = $(PREFIX)/share
enginedir = $(datadir)/smartmet/engines
objdir = obj

# Compiler options

DEFINES = -DUNIX -D_REENTRANT

ifeq ($(CXX), clang++)

 # TODO: Should remove -Wno-sign-conversion, FlashTools.cpp warns a lot

 FLAGS = \
	-std=c++11 -fPIC -MD \
	-Weverything \
	-Wno-c++98-compat \
	-Wno-float-equal \
	-Wno-padded \
	-Wno-missing-prototypes \
	-Wno-global-constructors \
	-Wno-exit-time-destructors \
	-Wno-sign-conversion

 INCLUDES = \
	-isystem $(includedir) \
	-isystem $(includedir)/smartmet \
	-isystem $(includedir)/mysql \
	-isystem $(includedir)/oracle/11.2/client64 \
	-isystem $(includedir)/soci 

else

 FLAGS = -std=c++11 -fPIC -MD -Wall -W -Wno-unused-parameter -fno-omit-frame-pointer -Wno-unknown-pragmas -fdiagnostics-color=always

 FLAGS_DEBUG = \
	-Wcast-align \
	-Winline \
	-Wno-multichar \
	-Wno-pmf-conversions \
	-Woverloaded-virtual  \
	-Wpointer-arith \
	-Wcast-qual \
	-Wredundant-decls \
	-Wwrite-strings \
	-Wsign-promo \

 INCLUDES = \
	-I$(includedir) \
	-I$(includedir)/smartmet \
	-I$(includedir)/mysql \
	-I$(includedir)/oracle/11.2/client64 \
	-I$(includedir)/soci

endif

# Compile options in detault, debug and profile modes

CFLAGS_RELEASE = $(DEFINES) $(FLAGS) $(FLAGS_RELEASE) -DNDEBUG -O2 -g
CFLAGS_DEBUG   = $(DEFINES) $(FLAGS) $(FLAGS_DEBUG)   -Werror  -O0 -g

ifneq (,$(findstring debug,$(MAKECMDGOALS)))
  override CFLAGS += $(CFLAGS_DEBUG)
else
  override CFLAGS += $(CFLAGS_RELEASE)
endif

LIBS = -L$(libdir) \
        -lmysqlpp \
        -L$(libdir)/mysql -lmysqlclient_r -lz \
        -lsmartmet-spine \
        -lsmartmet_macgyver \
        -lsmartmet_locus \
        -lboost_thread \
        -lboost_iostreams \
        -lboost_date_time \
        -lboost_locale \
        -lboost_system \
        -lboost_serialization \
	-lsqlite3 \
	-lsoci_core \
	-lsoci_sqlite3 \
	`pkg-config --libs spatialite` \
        -lbz2 -lz \
	-latomic -lpthread \
	-L/usr/lib/oracle/11.2/client64/lib/ \
	-Wl,-rpath,/usr/lib/oracle/11.2/client64/lib/ \
        -lnnz11 \
        -lclntsh

# rpm variables

rpmsourcedir = /tmp/$(shell whoami)/rpmbuild

rpmerr = "There's no spec file ($(LIB).spec). RPM wasn't created. Please make a spec file or copy and rename it into $(LIB).spec"

# What to install

LIBFILE = $(SUBNAME).so

# How to install

INSTALL_PROG = install -p -m 775
INSTALL_DATA = install -p -m 664

# Compilation directories

vpath %.cpp source
vpath %.h include

# The files to be compiled

SRCS = $(wildcard source/*.cpp)
HDRS = $(wildcard include/*.h)
OBJS = $(patsubst %.cpp, obj/%.o, $(notdir $(SRCS)))

INCLUDES := -Iinclude $(INCLUDES)

.PHONY: test rpm

# The rules

all: objdir $(LIBFILE)
debug: all
release: all
profile: all

$(LIBFILE): $(OBJS)
	$(CXX) $(CFLAGS) -shared -rdynamic -o $(LIBFILE) $(OBJS) $(LIBS)

clean:
	rm -f $(LIBFILE) *~ source/*~ include/*~
	rm -rf obj

format:
	clang-format -i -style=file include/*.h source/*.cpp test/*.cpp

install:
	@mkdir -p $(includedir)/$(INCDIR)
	@list='$(HDRS)'; \
	for hdr in $$list; do \
	  HDR=$$(basename $$hdr); \
	  echo $(INSTALL_DATA) $$hdr $(includedir)/$(INCDIR)/$$HDR; \
	  $(INSTALL_DATA) $$hdr $(includedir)/$(INCDIR)/$$HDR; \
	done
	@mkdir -p $(enginedir)
	$(INSTALL_DATA) $(LIBFILE) $(enginedir)/$(LIBFILE)

test:
	cd test && make test

objdir:
	@mkdir -p $(objdir)

rpm: clean
	@if [ -e $(SPEC).spec ]; \
	then \
	  smartspecupdate $(SPEC).spec ; \
	  mkdir -p $(rpmsourcedir) ; \
	  tar -C ../../ -cf $(rpmsourcedir)/$(SPEC).tar engines/$(SUBNAME) ; \
	  gzip -f $(rpmsourcedir)/$(SPEC).tar ; \
	  TAR_OPTIONS=--wildcards rpmbuild -v -ta $(rpmsourcedir)/$(SPEC).tar.gz ; \
	  rm -f $(rpmsourcedir)/$(SPEC).tar.gz ; \
	else \
	  echo $(rpmerr); \
	fi;

.SUFFIXES: $(SUFFIXES) .cpp

obj/%.o: %.cpp
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o $@ $<

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif
