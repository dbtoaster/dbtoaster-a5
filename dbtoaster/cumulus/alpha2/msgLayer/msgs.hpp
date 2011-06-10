//
// msgs.hpp
// ~~~~~~~~~
//
// Copyright (c) 2011 Aleksandar Vitorovic
//

#ifndef SERIALIZATION_STOCK_HPP
#define SERIALIZATION_STOCK_HPP

#include <string>

namespace msg_layer {

/// Structure to hold information about a single stock.
struct put_msg
{

  int trigger_id;
  int timestamp;

  template <typename Archive>
  void serialize(Archive& ar, const unsigned int version)
  {
    ar & trigger_id;
    ar & timestamp;
  }
};

} // namespace msg_layer

#endif // SERIALIZATION_STOCK_HPP
