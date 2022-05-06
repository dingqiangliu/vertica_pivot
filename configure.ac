#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([pivot], [1.0.0], [dingqiangliu@gmail.com])
AC_CONFIG_SRCDIR([src])
AC_CONFIG_FILES([makefile])

AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([subdir-objects -Wall])

AC_ENABLE_SHARED
AC_DISABLE_STATIC
LT_INIT


# Checks for programs.
AC_PROG_CXX

# Checks for libraries.

# Checks for header files.

AC_CHECK_HEADER_STDBOOL

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.


# option: enable-debug
AC_ARG_ENABLE(debug,
AS_HELP_STRING([--enable-debug],
               [enable debugging, default: no]),
[case "${enableval}" in
             yes) debug=yes ;;
             no)  debug=no ;;
             *)   AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;;
esac],
[debug=false])

# Remove all optimization flags from CFLAGS & CXXFLAGS
changequote({,})
CFLAGS=`echo "$CFLAGS" | $SED -e 's/-O[0-9s]*//g'`
CXXFLAGS=`echo "$CXXFLAGS" | $SED -e 's/-O[0-9s]*//g'`

CFLAGS=`echo "$CFLAGS" | $SED -e 's/-g[0-9]*//g'`
CXXFLAGS=`echo "$CXXFLAGS" | $SED -e 's/-g[0-9]*//g'`
changequote([,])

echo "enable-debug=$debug"
if test x"$debug" = x"yes" ; then
  CFLAGS="$CFLAGS -g -O0"
  CXXFLAGS="$CXXFLAGS -g -O0"
else
  CFLAGS="$CFLAGS -O2"
  CXXFLAGS="$CXXFLAGS -O2"
fi

echo "set compile tag for Vertica 9+"
test -f /opt/vertica/sdk/include/BuildInfo.h \
  && test 9 -le "$(grep -E '#define\sVERTICA_BUILD_ID_SDK_Version_Major' /opt/vertica/sdk/include/BuildInfo.h | awk '{print $3}')" \
  && CXXFLAGS="$CXXFLAGS -std=c++11"

echo "set compile tag for Vertica 10+"
test -f /opt/vertica/sdk/include/BuildInfo.h \
  && test 10 -le "$(grep -E '#define\sVERTICA_BUILD_ID_SDK_Version_Major' /opt/vertica/sdk/include/BuildInfo.h | awk '{print $3}')" \
  && CXXFLAGS="$CXXFLAGS -D_GLIBCXX_USE_CXX11_ABI=0"

echo "CFLAGS=$CFLAGS"
echo "CXXFLAGS=$CXXFLAGS"


# set variables for makefile

test -z "$SDK" && SDK=/opt/vertica/sdk
AC_SUBST([SDK], $SDK)
echo "SDK=$SDK"

test -z "$VSQL" && VSQL=/opt/vertica/bin/vsql
AC_SUBST([VSQL], $VSQL)
echo "VSQL=$VSQL"


AC_OUTPUT
