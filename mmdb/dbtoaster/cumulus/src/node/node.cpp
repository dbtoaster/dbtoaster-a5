#include <ctime>
#include <iostream>
#include <string>
#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>

#include "cumulus.hpp"

using boost::asio::ip::tcp;

class node_connection 
  : public boost::enable_shared_from_this<tcp_connection>
{
  public:
    typedef boost::shared_ptr<tcp_connection> pointer;
    
    static pointer create(boost::asio::io_service &io_service, boost::shared_ptr<node_handler> &handler)
    {
      return pointer(new node_connection(io_service, handler));
    }
    
    tcp::socket &socket()
    {
      return socket;
    }
    
    void start()
    {
      boost::asio::async_read(
        socket,
        GET_PUT_HEAD_BUFF(read_buffer),
        boost::bind(
          &node_connection::handle_read_stage1, shared_from_this,
          boost::asio::placeholders::error
        )
      );
    }
    
  private:
    node_connection(boost::asio::io_service &io_service, boost::shared_ptr<node_handler> &handler)
      : socket(io_service), handler(handler)
    {}
    
    void handle_read_stage1(const boost::system::error_code &error)
    {
      if(error) {
        handle_error(error);
        return;
      }
      if(! VALIDATE_PUT(read_buffer) ){
        handle_error(boost::error::basic_errors::fault);
        return;
      }
      
      boost::asio::async_read(
        socket,
        GET_PUT_PARAM_BUFF(read_buffer),
        boost::bind(
          &node_connection::handle_read_stage2, shared_from_this,
          boost::asio::placeholders::error
        )
      );
    }
    
    void handle_read_stage2(const boost::system::error_code &error)
    {
      if(error) {
        handle_error(error);
        return;
      }
      
      handler->handle(GET_PUT_REQUEST(b));
      
      start();
    }
    
    void handle_error(const boost::system::error_code &error)
    {
      
    }
    
    boost::shared_ptr<node_handler> handler;
    unsigned char read_buffer[PUT_REQUEST_MAX_SIZE]
    tcp::socket socket;
}

class node_server 
{
  public:
  
    node_server(boost::asio::ioservice &io_service, boost::shared_ptr<node_handler> &handler) 
      : self(io_service, handler CUMULUS_DEFAULT_NODE_PORT) {};
  
    node_server(boost::asio::ioservice &io_service, boost::shared_ptr<node_handler> &handler, int port) 
      : acceptor(io_service, tcp::endpoint(tcp::v4(), port)), handler(handler);
    {
      start_accept();
    }
  
  private:
    
    void start_accept(void)
    {
      node_connection::pointer new_connection = 
        node_connection::create(acceptor.io_service(), handler);
      
      acceptor.async_accept(
        new_connection->socket(),
        boost::bind(
          &node_server::handle_accept, this, 
          new_connection, boost::asio::placeholders::error
        )
      );
    }
    
    void handle_accept(node_connection::pointer new_connection, const boost::system::error_code &error)
    {
      if (!error){
        new_connection->start();
        start_accept();
      }
    }
    
    boost::shared_ptr<node_handler> handler;
    tcp::acceptor acceptor;
}

class node_handler
{
  public:
  
    static pointer create()
    {
      return pointer(new node_handler());
    }
    
    void handle(put_request *request){
      ++request_count;
    }
  
  private:
    
    void print_stats()
    {
      cerr << "Requests handled: " << request_count << std::endl;
    }
    
    node_handler()
      : request_count(0)
    { }
  
  unsigned int request_count;
}

int main()
{
  std::cout << "Node Server Starting" << std::endl;
  
  try {
    boost::asio::io_service io_service;
    node_server server(io_service);
    io_service.run();
  } catch (std::exception &e) {
    std::cerr << e.what << std::endl;
  }
  
  std::cout << "Node Server Complete" << std::endl;
  
  return 0;
}

