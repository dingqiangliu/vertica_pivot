############################
#
# Makefile to build pivot user defined functions
#
# To run under valgrind:
#   make RUN_VALGRIND=1 run
#
# Copyright DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2017
############################

SDK?=/opt/vertica/sdk
VSQL?=/opt/vertica/bin/vsql

# Define the .so name here (and update the references in install.sql and uninstall.sql)
PACKAGE_LIBNAME=lib/pivot.so

CXX=g++
#CXXFLAGS=-I ${SDK}/include -g -Wall -Wno-unused-value -shared -fPIC 
CXXFLAGS=-I ${SDK}/include -Wall -Wno-unused-value -shared -fPIC 

ifdef RUN_VALGRIND
VALGRIND=valgrind --leak-check=full
endif

.PHONEY: simulator run

all: ${PACKAGE_LIBNAME}

${PACKAGE_LIBNAME}: src/*.cpp ${SDK}/include/Vertica.cpp ${SDK}/include/BuildInfo.h
	mkdir -p lib
	$(CXX) $(CXXFLAGS) -o $@ src/*.cpp ${SDK}/include/Vertica.cpp 

# Targets to install and uninstall the library and functions
install: $(PACKAGE_LIBNAME) ddl/install.sql
	$(VSQL) -f ddl/install.sql
uninstall: ddl/uninstall.sql
	$(VSQL) -f ddl/uninstall.sql

# run examples
run: $(PACKAGE_LIBNAME) install test/test.sql
	$(VSQL) -f test/test.sql | tee testresult.txt

clean:
	[ -d lib ] && rm -rf lib || true
	[ -f testresult.txt ] && rm -f testresult.txt || true
