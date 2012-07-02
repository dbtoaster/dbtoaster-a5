#ifndef DBTOASTER_STANDARD_FUNCTIONS_H
#define DBTOASTER_STANDARD_FUNCTIONS_H

#include <string>
#include <sstream>
#include <regex.h>

namespace dbtoaster {

  // Date extraction functions
  // ImperativeCompiler synthesizes calls to the following from calls to 
  // date_part
  long year_part(date d) { 
    return (d / 10000) % 10000;
  }
  long month_part(date d) { 
    return (d / 100) % 100;
  }
  long day_part(date d) { 
    return d % 100;
  }
  
  // String functions
  inline string substring(string &s, long start, long len){
    return s.substr(start, len);
  }
  
  inline int regexp_match(const char *regex, string &s){
    //TODO: Caching regexes, or possibly inlining regex construction
    regex_t preg;
    int ret;
    
    if(regcomp(&preg, regex, REG_EXTENDED | REG_NOSUB)){
      cerr << "Error compiling regular expression: /" << 
              regex << "/" << endl;
      exit(-1);
    }
    ret = regexec(&preg, s.c_str(), 0, NULL, 0);
    regfree(&preg);
    
    switch(ret){
      case 0: return 1;
      case REG_NOMATCH: return 0;
      default:
      cerr << "Error evaluating regular expression: /" << 
              regex << "/" << endl;
      exit(-1);
    }
    
    regfree(&preg);
  }
  
  // Type conversion functions
  inline long cast_int_from_float(double           d) { return (long)d; };
  inline long cast_int_from_string(const char     *c) { return atoi(c); };
  inline long cast_int_from_string(string         &s) { return cast_int_from_string(s.c_str()); };
  inline double cast_float_from_int(long           i) { return (double)i; };
  inline double cast_float_from_string(const char *c) { return atof(c); };
  inline double cast_float_from_string(string     &s) { return cast_float_from_string(s.c_str()); };
  template <class T> 
    inline string cast_string(const T &t) {
      std::stringstream ss;
      ss << t;
      return ss.str();
    }
  inline string cast_string_from_int(long      i) { return cast_string(i); }
  inline string cast_string_from_double(double d) { return cast_string(d); }
  inline string cast_string_from_date(date ymd)   { 
    std::stringstream ss;
    ss << ((ymd / 10000) % 10000)
       << ((ymd / 100  ) % 100)
       << ((ymd        ) % 100);
    return ss.str();
  }
  inline date cast_date_from_string(const char *c) { 
    unsigned int y, m, d;
    if(sscanf(c, "%u-%u-%u", &y, &m, &d) < 3){
      cerr << "Invalid date string: "<< c << endl;
    }
    if((m > 12) || (d > 31)){ 
      cerr << "Invalid date string: "<< c << endl;
    }
    return (y%10000) * 10000 + (m%100) * 100 + (d%100);
  }
  inline date cast_date_from_string(string &s) { return cast_date_from_string(s.c_str()); }
}

#endif //DBTOASTER_STANDARD_FUNCTIONS_H