#ifndef DBTOASTER_UTIL_H
#define DBTOASTER_UTIL_H

namespace dbtoaster {
  namespace util {

    // Misc function object helpers.
    struct fold_hash {
      typedef std::size_t result_type;
      template<class T>
      std::size_t operator()(const T& arg, std::size_t current) {
        boost::hash_combine(current, arg);
        return(current);
      }
    };

  }
}

#endif
