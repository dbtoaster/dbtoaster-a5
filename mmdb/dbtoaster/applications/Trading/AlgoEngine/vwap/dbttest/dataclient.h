
#ifndef DEMO_DATACLIENT_H
#define DEMO_DATACLIENT_H

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/any.hpp>


#include <tr1/memory>
#include <tr1/tuple>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "datasets/datasets.h"

//#define DEBUG 1
#define SERVER_IP "127.0.0.1"
#define SERVER_PORT "5500"

namespace DBToaster
{
    namespace DemoDatasets
    {
        using boost::asio::ip::tcp;
        using namespace std;
        using namespace tr1;
        using namespace boost;
        using namespace DBToaster::DemoDatasets;
        
        template<typename tuple>
        struct ClientStream
        {
//            typedef list<tuple> StreamBuffer;

            typedef boost::function<void (boost::any)> Handler;
            Handler dispatcher;
            
            virtual void read(boost::function<void ()> handler) {};
        };

        class VwapDataClient : public ClientStream<VwapTuple>
        {

        public:

            VwapDataClient(boost::asio::io_service& io_service, boost::function<void (boost::any)> fun)
                : 
                localIOService(io_service),
                resolver(io_service),
                query(SERVER_IP, SERVER_PORT),
                localSocket(io_service)
                
            {
                dispatcher = fun;
                tcp::resolver::iterator endpointIterator = resolver.resolve(query);
                
                type=1;
                tcp::endpoint endpoint = *endpointIterator;
                localSocket.async_connect(endpoint,
                    boost::bind(&DBToaster::DemoDatasets::VwapDataClient::handleConnection, this,
                    boost::asio::placeholders::error, ++endpointIterator));

            }

            void read( boost::function<void ()> handler )
            {
                localIOService.post(boost::bind(&VwapDataClient::doRead, this, handler));
            }

        private:

            void doRead(boost::function<void ()> handler )
            {
                //reading message from a server
                char tail=10;
                boost::asio::async_read_until(localSocket,
                    readResponse,
                    tail,
                    boost::bind(&VwapDataClient::handleRead, this,
                    boost::asio::placeholders::error, handler));
            }

            void handleRead( const boost::system::error_code& error, boost::function<void ()> handler)
            {

                std::istream ist(&readResponse);

                string line;
                getline(ist, line);

                VwapTuple r;
                parseLine(line, r);
               
                dispatcher(r);
                handler();			
            }

            inline bool parseLine(string line, VwapTuple& r)
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
                        boost::bind(&VwapDataClient::doNothing, this,
                        boost::asio::placeholders::error));
                }
                else if (endpointIterator != tcp::resolver::iterator())
                {
                    //in case the connection is not established try to reconnect with different protocol

                    localSocket.close();
                    tcp::endpoint endpoint = *endpointIterator;
                    localSocket.async_connect(endpoint,
                        boost::bind(&VwapDataClient::handleConnection, this,
                        boost::asio::placeholders::error, ++endpointIterator));
                }
            }

            void doNothing(const boost::system::error_code& error)
            {
                cout<<"inside doing nothing\n";
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
            int type;      

            boost::asio::streambuf readResponse;

        };
    };
};

#endif