

#ifndef READER_CONNECTION_DEMO
#define READER_CONNECTION_DEMO

#include <string>

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>


#include "DataTuple.h"

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT "5501"

namespace DBToaster
{
    namespace DemoAlgEngine
    {

        using boost::asio::ip::tcp;
        using namespace std;
//        using namespace tr1;
        using namespace boost;
        using namespace DBToaster::DemoAlgEngine;

        class ReaderConnection
        {
        public:
            ReaderConnection(boost::asio::io_service& io_service)
                : localIOService(io_service),
                resolver(io_service),
                query(SERVER_IP, SERVER_PORT),
                localSocket(io_service)
            {

                tcp::resolver::iterator endpointIterator = resolver.resolve(query);

                type=1;
                tcp::endpoint endpoint = *endpointIterator;
                localSocket.async_connect(endpoint,
                    boost::bind(&ReaderConnection::handleConnection, this,
                    boost::asio::placeholders::error, ++endpointIterator));

            }

            void read( boost::function<void (DataTuple& a)>  handler)
            {
                localIOService.post(boost::bind(&ReaderConnection::doRead, this, handler));
            }

        private:

            void doRead( boost::function<void (DataTuple& a)>  handler)
            {
                    //reading message from a server
                char tail=10;
                boost::asio::async_read_until(localSocket,
                    readResponse,
                    tail,
                    boost::bind(&ReaderConnection::handleRead, this,
                    boost::asio::placeholders::error, handler));
            }

            void handleRead( const boost::system::error_code& error, boost::function<void (DataTuple& a)>  handler)
            {

                std::istream ist(&readResponse);

                string line;
                getline(ist, line);

                DataTuple r;
                parseLine(line, r);

                handler(r);			
            }

            inline bool parseLine(string line, DataTuple& r)
            {
                std::istringstream ist(line);
                string param;
                ist>>param;
                r.t=string_conversion(param);

                ist>>r.id;

                ist>>r.action;

                ist>>r.volume;

                ist>>r.price;

                return true;
            }

            void handleConnection(const boost::system::error_code& error,
                tcp::resolver::iterator endpointIterator)
            {
                if (!error)
                {

                    boost::asio::async_write(localSocket,
                        boost::asio::buffer((void*) &type, sizeof(int)),
                        boost::bind(&ReaderConnection::doNothing, this,
                        boost::asio::placeholders::error));
                }
                else if (endpointIterator != tcp::resolver::iterator())
                {
                        //in case the connection is not established try to reconnect with different protocol

                    localSocket.close();
                    tcp::endpoint endpoint = *endpointIterator;
                    localSocket.async_connect(endpoint,
                        boost::bind(&ReaderConnection::handleConnection, this,
                        boost::asio::placeholders::error, ++endpointIterator));
                }
            }

            void doNothing(const boost::system::error_code& error)
            {
                    //cout<<"inside doing nothing\n";
            }

            long long string_conversion(string s){
                long long value=0;

                for (int i=0; i<s.length(); i++){
                    value=value*10+atoi((s.substr(i,1)).c_str());
                }
                return value;
            }

            boost::asio::io_service& localIOService;
            tcp::resolver resolver;
            tcp::socket localSocket;
            tcp::resolver::query query;
            boost::asio::streambuf readResponse;

            int type;            

        };        
    };
};


#endif