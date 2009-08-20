

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

            void read( boost::function<void (DataTuple& a)>  handler)
            {
                localIOService.post(boost::bind(&WriterConnection::doRead, this, handler));
            }

            void write(boost::function<void (DataTuple& a)>  handler, DataTuple& tuple, int& info)
            {
//                cout<<"in write "<<tuple.action<<" "<<tuple.price<<endl;
                localIOService.post(boost::bind(&WriterConnection::doSyncWrite, this, handler, tuple, info));
            }

        private:

            void doWrite(boost::function<void (DataTuple& a)>  handler, DataTuple & tuple, int & info)
            {	
//                cout<<"in doWrite"<<endl;
//                flush(cout);
//                cout<<tuple.t<<" "<<tuple.id<<" "<<tuple.b_id<<" "<<tuple.action<<" "<<tuple.volume<<" "<<tuple.price<<endl;
//                cout<<"in doWrite "<<info<<endl;
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
                    boost::asio::async_write(localSocket,
                        boost::asio::buffer(strMessage.data() ,strMessage.size()),
                        boost::bind(&WriterConnection::doRead, this, handler));
                }

            }

            void doSyncWrite(boost::function<void (DataTuple& a)>  handler, DataTuple & tuple, int & info)
            {	
//                cout<<"in doWrite"<<endl;
//                flush(cout);
//                cout<<tuple.t<<" "<<tuple.id<<" "<<tuple.b_id<<" "<<tuple.action<<" "<<tuple.volume<<" "<<tuple.price<<endl;
//                cout<<"in doWrite "<<info<<endl;
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

                        boost::asio::write(localSocket, 
                            boost::asio::buffer(strMessage.data() ,strMessage.size()));
//                            boost::asio::transfer_all(), ignored_error);

                        doSyncRead(handler);

                    }
                    catch (std::exception& e)
                    {
                        cout <<"doSyncWrite"<< e.what() << endl;
                        exit(1);
                    }
                }

            }

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


//                cout << "handleRead size: " << readResponse.size() << endl;

                DataTuple r;
                parseLine(line, r);
                int num=5;

                if (!ist)
                {
                    cout<<"handleRead: error istream is corrupted"<<endl;
                    exit(1);
                }

//               cout<<r.t<<" "<<r.id<<" "<<r.b_id<<" "<<r.action<<" "<<r.volume<<" "<<r.price<<endl;
//                cout<<"handleRead: about to handle order r: "<<r.price<<endl;
                handler(r);

            }

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

            void handleRead(const boost::system::error_code& error,std::size_t transferred, boost::function<void (DataTuple& a)>  handler)
            {
                if (!error)
                {
//                    cout<<"handleRead: in "<<transferred<< " " << readResponse.size() << endl;
//                    flush(cout);
//                boost::asio::streambuf local_readResponsereadResponse);
                    std::istream ist(&readResponse);

                    string line;
                    getline(ist, line);


//                    cout << "handleRead size: " << readResponse.size() << endl;

                    DataTuple r;
                    parseLine(line, r);
                    int num=5;

                    if (!ist)
                    {
                        cout<<"handleRead: error istream is corrupted"<<endl;
                        exit(1);
                    }

//                    cout<<r.t<<" "<<r.id<<" "<<r.b_id<<" "<<r.action<<" "<<r.volume<<" "<<r.price<<endl;
//                    cout<<"handleRead: about to handle order r: "<<r.price<<endl;
                    handler(r);	
//                    cout<<"handleRead: done handleOrder"<<endl;		
                }
                else
                {
                    cout<<"handleRead: error"<<endl;
                    exit(1);
                }
            }

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

            long long string_conversion(string s)
            {
                long long value=0;

                for (int i=0; i<s.length(); i++){
                    value=value*10+atoi((s.substr(i,1)).c_str());
                }
                return value;
            }

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
