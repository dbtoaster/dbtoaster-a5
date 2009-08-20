


#include "datasets.h"
#include "dataclient.h"

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>


using namespace std;
using namespace tr1;
using namespace boost;
using namespace DBToaster;
using namespace DemoDatasets;

VwapDataClient * clientPtr;

void outputMessage(VwapTuple & msg)
{

    cout<<msg.t<< " "<<msg.id<<" "<<msg.action<<" "<<msg.price<<" "<<msg.volume<<endl;
    clientPtr->read(boost::bind(&outputMessage, _1));
}

int main()
{
    boost::asio::io_service io_service;

/*    tcp::resolver resolver(io_service);
    tcp::resolver::query query("127.0.0.1", "5500");
    tcp::resolver::iterator my_iterator = resolver.resolve(query);
    tcp::socket socket_(io_service);
*/
    VwapDataClient my_client(io_service);

    clientPtr=&my_client;

    my_client.read(boost::bind(&outputMessage, _1));

    boost::thread t(boost::bind(&boost::asio::io_service::run, &io_service));

    t.join();


    return 0;
}