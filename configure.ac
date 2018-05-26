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

AC_SUBST([SDK], [/opt/vertica/sdk])
AC_SUBST([VSQL], [/opt/vertica/bin/vsql])

# Checks for programs.
AC_PROG_CXX

# Checks for libraries.

# Checks for header files.

AC_CHECK_HEADER_STDBOOL

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_OUTPUT
