
#ifndef CUMULUS_H_SHIELD
#define CUMULUS_H_SHIELD

#define CUMULUS_DEFAULT_NODE_PORT   52982
#define CUMULUS_DEFAULT_SWITCH_PORT 52981
#define CUMULUS_DEFAULT_SLICER_PORT 52980

class node_accessor {
  public:
    node_accessor(boost::asio::io_service &io_service, std::string &address)
      : self(io_service, address, CUMULUS_DEFAULT_NODE_PORT); 
    
    node_accessor(boost::asio::io_service &io_service, std::string &address, int port)
      socket(io_service)
    {
      tcp::resolver resolver(io_service);
      tcp::resolver::query query(address, port);
      tcpresolver::iterator endpoint_iterator = resolver.resolve(query), end;
      boost::system::error_code error = boost::asio::error::host_not_found;
      while (error && endpoint_iterator != end) {
        socket.close();
        socket.connect(*endpoint_iterator++, error);
      }
      if (error) {
        throw boost::system::system_error(error);
      }
    }
    
  private:
    tcp::socket socket;
}

////////////////////////////////////////////////////////////////////////////////

#define PUT_REQUEST_MAGIC      "cuP"
#define PUT_REQUEST_MAX_PARAMS 20
#define PUT_REQUEST_PARAM_SIZE
#define PUT_REQUEST_MAX_SIZE   (sizeof(put_request) + (sizeof(double) * PUT_REQUEST_MAX_PARAMS))
#define PUT_REQUEST_MIN_SIZE   (sizeof(put_request))

#define GET_PUT_REQUEST(b)     ((put_request *)b)
#define GET_PUT_MAGIC(b)       (GET_PUT_REQUEST(b)->magic)
#define GET_PUT_TID(b)         (GET_PUT_REQUEST(b)->template_id)
#define GET_PUT_PARAM_CNT(b)   (GET_PUT_REQUEST(b)->param_count)
#define GET_PUT_PARAM(b,n)     ((GET_PUT_PARAM_CNT(b) <= n) ? NaN : (GET_PUT_REQUEST(b)->params[n]))
#define GET_PUT_HEAD_BUFF(b)   (boost::asio::buffer(b, PUT_REQUEST_MIN_SIZE)
#define GET_PUT_PARAM_BUFF(b)  (boost::asio::buffer((char *)GET_PUT_REQUEST(b)->params, GET_PUT_PARAM_CNT(b) * sizeof(double))

#define VALIDATE_PUT(b)        (strcmp(GET_PUT_MAGIC(b), PUT_REQUEST_MAGIC) == 0)

struct put_request {
  char         magic[4];
  unsigned int template_id;
  unsigned int param_count;
  double       params[0];
}


#endif
