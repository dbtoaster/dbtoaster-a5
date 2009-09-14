

#ifndef WRITER_CONNECTION_DEMO
#define WRITER_CONNECTION_DEMO

#include <string>
#include <stdio.h>
#include <iostream>

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


/*
 * Handles connection to the Exchange Server Simulator. 
 * supports both reading and writing to/from server
 */

namespace DBToaster
{
    namespace DemoAlgEngine
    {

        using boost::asio::ip::tcp;
        using namespace std;
        using namespace boost;
        using namespace DBToaster::DemoAlgEngine;

        class WriterConnection
        {
        public:
            //constractor sets the io_service 
            //opens connection to server
            WriterConnection(boost::asio::io_service& io_service, int t)
                : localIOService(io_service),
                resolver(io_service),
                query(SERVER_IP, SERVER_PORT),
                localSocket(io_service)
            {

                tcp::resolver::iterator endpointIterator = resolver.resolve(query);

                type=t;
                tcp::endpoint endpoint = *endpointIterator;
                localSocket.async_connect(endpoint,
                    boost::bind(&WriterConnection::handleConnection, this,
                    boost::asio::placeholders::error, ++endpointIterator));

            }

            //reads data from server
            void read( boost::function<void (DataTuple& a)>  handler)
            {
                localIOService.post(boost::bind(&WriterConnection::doRead, this, handler));
            }

            //writes a tuple to a server
            void write(boost::function<void (DataTuple& a)>  handler, DataTuple& tuple, int& info)
            {
                localIOService.post(boost::bind(&WriterConnection::doSyncWrite, this, handler, tuple, info));
            }

        private:

            void doWrite(boost::function<void (DataTuple& a)>  handler, DataTuple & tuple, int & info)
            {	
                //writes a tuple to server
                std::stringstream ss (std::stringstream::in | std::stringstream::out);
                ss<<tuple.t<<" "<<tuple.id<<" "<<tuple.b_id<<" "<<tuple.action<<" "<<tuple.volume<<" "<<tuple.price;
                std::string strMessage=ss.str();
                char tail=10;
                strMessage=strMessage+tail;

                //if info is equals 1 this is an order which needs to read info from server 
                if (info == 0)
                {

                    boost::asio::async_write(localSocket,
                        boost::asio::buffer(strMessage.data() ,strMessage.size()),
                        boost::bind(&WriterConnection::doNothing, this,
                        boost::asio::placeholders::error));
                }
                else
                {
                    boost::asio::async_write(localSocket,
                        boost::asio::buffer(strMessage.data() ,strMessage.size()),
                        boost::bind(&WriterConnection::doRead, this, handler));
                }

            }

            //writes data and recives a data syncronously 
            void doSyncWrite(boost::function<void (DataTuple& a)>  handler, DataTuple & tuple, int & info)
            {	
                
                std::stringstream ss (std::stringstream::in | std::stringstream::out);
                ss<<tuple.t<<" "<<tuple.id<<" "<<tuple.b_id<<" "<<tuple.action<<" "<<tuple.volume<<" "<<tuple.price;
                std::string strMessage=ss.str();
                char tail=10;
                strMessage=strMessage+tail;

                if (info == 0)
                {

                    boost::asio::async_write(localSocket,
                        boost::asio::buffer(strMessage.data() ,strMessage.size()),
                        boost::bind(&WriterConnection::doNothing, this,
                        boost::asio::placeholders::error));

                }
                else
                {
                    try{
                        //syncronous read
                        boost::asio::write(localSocket, 
                            boost::asio::buffer(strMessage.data() ,strMessage.size()));

                        doSyncRead(handler);

                    }
                    catch (std::exception& e)
                    {
                        cout <<"doSyncWrite"<< e.what() << endl;
                        exit(1);
                    }
                }

            }

            //does syncronous read of data from server
            void doSyncRead(boost::function<void (DataTuple& a)>  handler)
            {
                boost::asio::streambuf my_readResponse;
                char tail=10;
                boost::asio::read_until(localSocket,
                    my_readResponse,
                    tail);

                std::istream ist(&my_readResponse);

                string line;
                getline(ist, line);

                DataTuple r;
                parseLine(line, r);
                int num=5;

                if (!ist)
                {
                    cout<<"handleRead: error istream is corrupted"<<endl;
                    exit(1);
                }
                
                //handles the results of the read on the same execution loop 
                handler(r);

            }

            //does async read from server
            void doRead( boost::function<void (DataTuple& a)>  handler)
            {
                    //reading message from a server
                char tail=10;
                boost::asio::async_read_until(localSocket,
                    readResponse,
                    tail,
                    boost::bind(&WriterConnection::handleRead, this,
                    boost::asio::placeholders::error, _2, handler));
            }

            //handles the results of a read
            void handleRead(const boost::system::error_code& error,std::size_t transferred, boost::function<void (DataTuple& a)>  handler)
            {
                if (!error)
                {

                    std::istream ist(&readResponse);

                    string line;
                    getline(ist, line);


                    DataTuple r;
                    parseLine(line, r);
                    int num=5;

                    if (!ist)
                    {
                        cout<<"handleRead: error istream is corrupted"<<endl;
                        exit(1);
                    }

                    //sends data to be handled 
                    handler(r);		
                }
                else
                {
                    cout<<"handleRead: error"<<endl;
                    exit(1);
                }
            }

            //parses order into order tuple
            inline bool parseLine(string line, DataTuple& r)
            {
                std::istringstream ist(line);
                string param;
                ist>>param;
                r.t=string_conversion(param);

                ist>>r.id;

                ist>>r.b_id;

                ist>>r.action;

                ist>>r.volume;

                ist>>r.price;

                return true;
            }

            //time id
            long long string_conversion(string s)
            {
                long long value=0;

                for (int i=0; i<s.length(); i++){
                    value=value*10+atoi((s.substr(i,1)).c_str());
                }
                return value;
            }

            //connection handleing
            void handleConnection(const boost::system::error_code& error,
                tcp::resolver::iterator endpointIterator)
            {
                if (!error)
                {

                    boost::asio::async_write(localSocket,
                        boost::asio::buffer((void*) &type, sizeof(int)),
                        boost::bind(&WriterConnection::doNothing, this,
                        boost::asio::placeholders::error));
                }
                else if (endpointIterator != tcp::resolver::iterator())
                {
                    //in case the connection is not established try to reconnect with different protocol

                    localSocket.close();
                    tcp::endpoint endpoint = *endpointIterator;
                    localSocket.async_connect(endpoint,
                        boost::bind(&WriterConnection::handleConnection, this,
                        boost::asio::placeholders::error, ++endpointIterator));
                }
            }

            //dumb funciton to do nothing 
            void doNothing(const boost::system::error_code& error)
            {
                //cout<<"inside doing nothing\n";
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
