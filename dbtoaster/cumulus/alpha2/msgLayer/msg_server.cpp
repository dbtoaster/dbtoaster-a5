//
// server.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2011 Aleksandar Vitorovic
//

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <vector>
#include "connection.hpp" // Must come before boost/serialization headers.
#include <boost/serialization/vector.hpp>
#include "msgs.hpp"

namespace msg_layer {

/// Serves stock quote information to any client that connects to it.
class server
{
public:
  /// Constructor opens the acceptor and starts waiting for the first incoming
  /// connection.
  server(boost::asio::io_service& io_service, unsigned short port)
    : acceptor_(io_service,
        boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
  {
    // Create the data to be sent to each client.
    put_msg pm;
    pm.trigger_id = 10;
    pm.timestamp = 1;
    put_msgs_.push_back(pm);
    pm.trigger_id = 20;
    pm.timestamp = 2;
    put_msgs_.push_back(pm);

    // Start an accept operation for a new connection.
    connection_ptr new_conn(new connection(acceptor_.io_service()));
    acceptor_.async_accept(new_conn->socket(),
        boost::bind(&server::handle_accept, this,
          boost::asio::placeholders::error, new_conn));
  }

  /// Handle completion of a accept operation.
  void handle_accept(const boost::system::error_code& e, connection_ptr conn)
  {
    if (!e)
    {
      // Successfully accepted a new connection. Send the list of stocks to the
      // client. The connection::async_write() function will automatically
      // serialize the data structure for us.
      conn->async_write(put_msgs_,
          boost::bind(&server::handle_write, this,
            boost::asio::placeholders::error, conn));

      // Start an accept operation for a new connection.
      connection_ptr new_conn(new connection(acceptor_.io_service()));
      acceptor_.async_accept(new_conn->socket(),
          boost::bind(&server::handle_accept, this,
            boost::asio::placeholders::error, new_conn));
    }
    else
    {
      // An error occurred. Log it and return. Since we are not starting a new
      // accept operation the io_service will run out of work to do and the
      // server will exit.
      std::cerr << e.message() << std::endl;
    }
  }

  /// Handle completion of a write operation.
  void handle_write(const boost::system::error_code& e, connection_ptr conn)
  {
    // Nothing to do. The socket will be closed automatically when the last
    // reference to the connection object goes away.
  }

private:
  /// The acceptor object used to accept incoming socket connections.
  boost::asio::ip::tcp::acceptor acceptor_;

  /// The data to be sent to each client.
  std::vector<put_msg> put_msgs_;
};

} // namespace msg_layer

int main(int argc, char* argv[])
{
  try
  {
    // Check command line arguments.
    if (argc != 2)
    {
      std::cerr << "Usage: server <port>" << std::endl;
      return 1;
    }
    unsigned short port = boost::lexical_cast<unsigned short>(argv[1]);

    boost::asio::io_service io_service;
    msg_layer::server server(io_service, port);
    io_service.run();
  }
  catch (std::exception& e)
  {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}
