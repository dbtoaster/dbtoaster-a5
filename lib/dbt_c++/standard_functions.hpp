#ifndef DBTOASTER_STANDARD_FUNCTIONS_H
#define DBTOASTER_STANDARD_FUNCTIONS_H

#include <string>
using namespace std;

#include "event.hpp"

namespace dbtoaster {

  // Date extraction functions
  // ImperativeCompiler synthesizes calls to the following from calls to 
  // date_part
  long year_part(date d);
  long month_part(date d);
  long day_part(date d);
  
  // String functions
  string substring(string &s, long start, long len);  
  int regexp_match(const char *regex, string &s);
  
  // Type conversion functions
  inline long cast_int_from_float(double           d) { return (long)d; };
  inline long cast_int_from_string(const char     *c) { return atoi(c); };
  inline long cast_int_from_string(string         &s) { 
    return cast_int_from_string(s.c_str()); 
  };
  inline double cast_float_from_int(long           i) { return (double)i; };
  inline double cast_float_from_string(const char *c) { return atof(c); };
  inline double cast_float_from_string(string     &s) { 
    return cast_float_from_string(s.c_str()); 
  };
  
  template <class T> 
  string cast_string(const T &t);
  
  inline string cast_string_from_int(long      i) { return cast_string(i); }
  inline string cast_string_from_double(double d) { return cast_string(d); }
  string cast_string_from_date(date ymd);
  date cast_date_from_string(const char *c);
  inline date cast_date_from_string(string &s) { 
    return cast_date_from_string(s.c_str()); 
  }
}

#endif //DBTOASTER_STANDARD_FUNCTIONS_H