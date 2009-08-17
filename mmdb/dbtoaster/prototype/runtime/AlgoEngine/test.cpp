

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

#include "GuiData.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

#include "GUICommunication_types.h"
#include "GuiDataServer.h"

#include "DataCollection.h"
#include "OrderManager.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

#include "DataTuple.h"
#include "ReaderConnection.h"
#include "WriterConnection.h"
#include "OrderManager.h"
#include "AlgorithmsEngine.h"
#include "SimpleSOBI.h"
#include "VolatileSOBI.h"
#include "TimedSOBI.h"
#include "MarketPlayers.h"


using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

ReaderConnection * clientPtr;

void outputMessage(DataTuple & msg)
{

    cout<<msg.t<< " "<<msg.id<<" "<<msg.action<<" "<<msg.price<<" "<<msg.volume<<endl;
    clientPtr->read(boost::bind(&outputMessage, _1));
}

int main()
{
    boost::asio::io_service io_service;
    boost::asio::io_service io_service_reader;

/*    tcp::resolver resolver(io_service);
    tcp::resolver::query query("127.0.0.1", "5500");
    tcp::resolver::iterator my_iterator = resolver.resolve(query);
    tcp::socket socket_(io_service);
*/
//    ReaderConnection my_client(io_service);
//    WriterConnection writer_client(io_service, 0);
    
//    clientPtr=&my_client;

//    my_client.read(boost::bind(&outputMessage, _1));

    AlgorithmsEngine man(io_service,io_service_reader, 0, 2500000);
    
    boost::thread t(boost::bind(&boost::asio::io_service::run, &io_service_reader));
     
 
    
    SimpleSOBI alg1;
    VolatileSOBI alg2(400);
    TimedSOBI  alg3;
    MarketPlayers alg4(0.5, 0.7);
    
    man.addAlgo(&alg1);
    man.addAlgo(&alg2);
    man.addAlgo(&alg3);
    man.addAlgo(&alg4);
    
    io_service.post(boost::bind(&DBToaster::DemoAlgEngine::AlgorithmsEngine::runAlgos, &man));
    
    
    int server_port(5502);
    shared_ptr<GuiDataHandler> handler(new GuiDataHandler(man.getDataCollection(), man.getOrderManager()));
    shared_ptr<TProcessor> processor(new GuiDataProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(server_port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    
//    boost::thread t2(boost::bind(&apache::thrift::server::TSimpleServer::serve, &server));
//    server.serve();
    
    io_service.run();

//    boost::thread t(boost::bind(&boost::asio::io_service::run, &io_service));

    t.join();
//    t2.join();


    return 0;
}