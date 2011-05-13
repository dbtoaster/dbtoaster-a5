

#include <cstdlib>
#include <deque>
#include <iostream>
#include <sstream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <fstream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <vector>
#include <tr1/memory>

#include <tr1/tuple>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include <sys/time.h>
#include <unistd.h>



using boost::asio::ip::tcp;
using namespace std;
using namespace tr1;

#define DBT_HASH_MAP unordered_map
#define DBT_HASH_SET unordered_set

#define OUTPUT 1

#define VWAP_K 0.25

#define DEBUG 1

// Timestamp, order id, action, volume, price
typedef tuple<double, int, string, double, double> stream_tuple;

// Input stream
typedef list<stream_tuple> stream_buffer;

// storage of bid/ask orders 
typedef DBT_HASH_MAP<int, tuple<double, double> > order_ids;

///////////////////////////////////////////////////
//
// DBToaster data structures for monotonic version

typedef map<double, double> select_sv1_index;

typedef map<double, double> spv_index;

select_sv1_index m_bids;
spv_index q_bids;
tuple<double, double> sv1_at_pmin_bids;
tuple<double, double> spv_at_pmin_bids;

select_sv1_index m_asks;
spv_index q_asks;
tuple<double, double> sv1_at_pmin_asks;
tuple<double, double> spv_at_pmin_asks;

struct positive_m_fn :
	public unary_function<pair<double, double>, bool>
{
	positive_m_fn() {}

	bool operator()(const pair<double, double> v) const {
		return v.second > 0;
	}
};

positive_m_fn positive_m;

/////////////////////////////////////
//
// DBToaster exploiting monotonicity

void print_select_sv1_index(select_sv1_index& m)
{
	select_sv1_index::iterator m_it = m.begin();
	select_sv1_index::iterator m_end = m.end();

	for (; m_it != m_end; ++m_it) {
		cout << m_it->first << ", " << m_it->second << endl;
	}
}

void print_spv_index(spv_index& q)
{
	spv_index::iterator q_it = q.begin();
	spv_index::iterator q_end = q.end();

	for (; q_it != q_end; ++q_it) {
		cout << q_it->first << ", " << q_it->second << endl;
	}
}

class exchange_message
{
public:

	long long timestamp;
	int order_id;
	char action;
	double volume;
	double price;


	//empty exchange message will look like a buy order at time 10000 of 0 volume at 0 price
	exchange_message():
	timestamp(0), order_id(0), action('B'), volume(0), price (0)
		{};
	exchange_message(long t, int id, char a, double v, double p) :
	timestamp(t), order_id(id), action(a), volume(v), price (p)
		{};
	void getParameters(std::istream &ist)
	{	
		string parameters;
		ist >> parameters;	
		char * char_ptr;

		timestamp=string_conversion(parameters);
		ist >> parameters;
		order_id=atoi(parameters.c_str());
		ist >> parameters;
		action=parameters.c_str()[0];
		ist >> parameters;
		volume=atof(parameters.c_str());
		ist >> parameters;
		price=atof(parameters.c_str());
	}
	
	void getParameters(string inParams)
	{
		std::istringstream ist(inParams);
		string param;
		ist>>param;
		timestamp=string_conversion(param);

		ist>>order_id;

		ist>>action;

		ist>>volume;

		ist>>price;

	}
	

private:
	friend class boost::serialization::access;
	// When the class Archive corresponds to an output archive, the
	// & operator is defined similar to <<.  Likewise, when the class Archive
	// is a type of input archive the & operator is defined similar to >>.
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{ //Timestamp, order id, action, volume, price
		ar & timestamp;
		ar & order_id;
		ar & action;
		ar & volume;
		ar & price;

	}
	friend ostream & operator<<( ostream & left, const exchange_message & message);
	
	//helper function to convert time from string into a long long int
	long long string_conversion(string s){
		long long value=0;
		
		for (int i=0; i<s.length(); i++){
			value=value*10+atoi((s.substr(i,1)).c_str());
		}
		return value;
	}


};

//printing the message
ostream & operator<<( ostream & left, const exchange_message & message){

	left<<message.timestamp<<" "<<message.order_id<<" "<<message.action<<" "<<message.volume<<" "<<message.price;
	return left;
}
/////////////////////////////////////////////////////////////////////////////////////////////
//has to be here after the definition of the class
//queue for storing exchange messages as they are coming in to the 
typedef std::deque<exchange_message> exchange_message_queue;

//boost::function<void (exchange_message &a)> handleFunction;


inline double update_vwap_monotonic(double price, double volume,
	select_sv1_index& m, spv_index& q,
	tuple<double, double>& sv1_at_pmin,
	tuple<double, double>& spv_at_pmin,
	bool insert = true)
{
	bool bids = m.begin() == m_bids.begin();

	select_sv1_index::iterator m_p_found = m.find(price);
	
	// foreach p2 in dom_p2 ...
	select_sv1_index::iterator p2_it = m.begin();
	select_sv1_index::iterator p2_end = m.end();

	for (; p2_it != p2_end; ++p2_it)
	{
		if ( insert )  {
			m[p2_it->first] = p2_it->second +
				(VWAP_K*volume - (price > p2_it->first ? volume : 0));
		}
		else {
			m[p2_it->first] = p2_it->second -
				(VWAP_K*volume - (price > p2_it->first ? volume : 0));
		}
	}
	// p not in dom_p2
	if ( insert && m_p_found == m.end() ) {
		select_sv1_index::iterator p_ub = m.upper_bound(price);

		while ( p_ub != m.end() && p_ub->first <= price )
			++p_ub;

		if ( p_ub == m.end() ) {
			// No upper bound, price is largest seen so far.
			m[price] = VWAP_K * volume;
		}
		else {
			double p_upper = p_ub->first;
			double p_upper_sv1 = p_ub->second;

			double sv1_at_p_upper = 0.0;

			if ( p_ub == m.begin() )
			{
				sv1_at_p_upper = p_ub->second + get<1>(sv1_at_pmin);

				// update sv1_at_pmin
				sv1_at_pmin = make_tuple(price, volume);
			}
			else {
				--p_ub;
				sv1_at_p_upper = p_ub->second - (p_upper_sv1 + (VWAP_K*volume - volume));
			}

			m[price] = p_upper_sv1 + sv1_at_p_upper;
		}
	}
	/*
	 * TODO: garbage collect, but how do you deal with p_max, which always has sv1 = 0
	 else if ( !insert && m_p_found != m.end() ) {
		 if ( m_p_found->second == 0.0 )
			 m.erase(price);
	 }
	 */

	// foreach pmin in dom_p2
	spv_index::iterator q_p_found = q.find(price);

	spv_index::iterator pmin_it = q.begin();
	spv_index::iterator pmin_end = q.end();

	for (; pmin_it != pmin_end; ++pmin_it)
	{
		if ( insert ) {
			q[pmin_it->first] = pmin_it->second +
				(price >= pmin_it->first? price*volume : 0);
		}
		else {
			q[pmin_it->first] = pmin_it->second -
				(price >= pmin_it->first? price*volume : 0);
		}
	}
	if ( insert && q_p_found == q.end() )
	{
		spv_index::iterator p_ub = q.upper_bound(price);

		while ( p_ub != q.end() && p_ub->first <= price )
			++p_ub;

		if ( p_ub == q.end() ) {
			// No upper bound, price is largest seen so far.
			q[price] = price * volume;
		}
		else {
			double p_upper = p_ub->first;
			double p_upper_spv = p_ub->second;

			double spv_at_p_upper = 0.0;

			if ( p_ub == q.begin() )
			{
				spv_at_p_upper = p_ub->second + get<0>(spv_at_pmin);

				// update spv_at_pmin
				spv_at_pmin = make_tuple(price, price*volume);
			}
			else {
				--p_ub;
				spv_at_p_upper = p_ub->second - (p_upper_spv + price*volume);
			}

			q[price] = p_ub->second + spv_at_p_upper;
		}
	}
	/*
	 * TODO: garbage collect, but how do you deal with p_max, which always has spv = 0
	 else if ( !insert && q_p_found != q.end() ) {
		 if ( q_p_found->second == 0.0 )
			 q.erase(price);
	 }
	 */
	
	// p_min = min {p' | m[p'] > 0}
	select_sv1_index::iterator p_min_found =
		find_if(m.begin(), m.end(), positive_m);

	double result = 0.0;

	if ( p_min_found != m.end() )
	{
		spv_index::iterator r_found = q.find(p_min_found->first);
		if ( r_found != q.end() )
			result = r_found->second;
		else {
			cerr << (bids? "Bids " : "Asks ")
				<< "no q[p_min] found for p_min = "
				<< p_min_found->first << endl;
			print_select_sv1_index(m);
		}
	}
	else {
		cerr << (bids? "Bids " : "Asks ") << "no m[p] > 0 found!" << endl;
		cerr << "New (p,v) " << price << ", " << volume << endl;
		print_select_sv1_index(m);
	}

	return result;
}


void vwap_stream_monotonic(exchange_message new_tuple, 
							order_ids& bid_orders, order_ids& ask_orders,
							boost::function<void (exchange_message& a, bool& v)>  getOrder)
{
	
	

//  check if we really need all of these vars
//	struct timeval tvs, tve;
	
	double bidResult = 0.0;
	double askResult = 0.0;
//	unsigned long tuple_counter = 0;

//	double tup_sum = 0.0;
//	double tup_sum_usec = 0.0;
	
	struct timeval tups, tupe;
	
	gettimeofday(&tups, NULL);
	
	int order_id = new_tuple.order_id;
	char action = new_tuple.action;
	
	#ifdef LOG_INPUT
	cout << "Tuple: order " << order_id << ", " << action
		<< " t=" << new_tuple.timestamp << " p=" << new_tuple.price
		<< " v=" << new_tuple.volume << endl;
	#endif
	
	if (action == 'B') {
		// Insert bids
		double price = new_tuple.price;
		double volume = new_tuple.volume;

		tuple<double, double> pv = make_tuple(price, volume);
		bid_orders[order_id] = pv;

		bidResult = update_vwap_monotonic(
			price, volume, m_bids, q_bids,
			sv1_at_pmin_bids, spv_at_pmin_bids);
	}
	else if (action == 'S')
	{
		// Insert asks
		double price = new_tuple.price;
		double volume = new_tuple.volume;

		ask_orders[order_id] = make_tuple(price, volume);
		askResult = update_vwap_monotonic(
			price, volume, m_asks, q_asks,
			sv1_at_pmin_asks, spv_at_pmin_asks);
	}
	else if (action == 'E')
	{
		// Partial execution based from order book according to order id.
		double delta_volume = new_tuple.volume;

		order_ids::iterator bid_found = bid_orders.find(order_id);
		if ( bid_found != bid_orders.end() )
		{
			double price =  get<0>(bid_found->second);
			double volume =  get<1>(bid_found->second);
			double new_volume = volume - delta_volume;

			// For now, we handle updates as delete, insert
			tuple<double, double> old_pv = make_tuple(price, volume);
			tuple<double, double> new_pv = make_tuple(price, new_volume);

			bid_orders.erase(bid_found);

			bidResult = update_vwap_monotonic(
				price, volume, m_bids, q_bids,
				sv1_at_pmin_bids, spv_at_pmin_bids, false);

			if ( new_volume > 0 )
			{
				bid_orders[order_id] = new_pv;
				bidResult = update_vwap_monotonic(
					price, new_volume, m_bids, q_bids,
					sv1_at_pmin_bids, spv_at_pmin_bids);
			}
		}
		else
		{
			order_ids::iterator ask_found = ask_orders.find(order_id);
			if ( ask_found != ask_orders.end() )
			{
				double price =  get<0>(ask_found->second);
				double volume =  get<1>(ask_found->second);
				double new_volume = volume - delta_volume;

				// For now, we handle updates as delete, insert
				ask_orders.erase(ask_found);

				askResult = update_vwap_monotonic(
					price, volume, m_asks, q_asks,
					sv1_at_pmin_asks, spv_at_pmin_asks, false);

				if ( new_volume > 0 ) {
					askResult = update_vwap_monotonic(
						price, new_volume, m_asks, q_asks,
						sv1_at_pmin_asks, spv_at_pmin_asks);
				}
			}
		}
	}
	else if (action == 'F')
	{
		// Order executed in full
		order_ids::iterator bid_found = bid_orders.find(order_id);
		if ( bid_found != bid_orders.end() )
		{
			double price =  get<0>(bid_found->second);
			double volume =  get<1>(bid_found->second);

			bid_orders.erase(bid_found);

			bidResult = update_vwap_monotonic(
				price, volume, m_bids, q_bids,
				sv1_at_pmin_bids, spv_at_pmin_bids, false);
		}
		else
		{
			order_ids::iterator ask_found = ask_orders.find(order_id);
			if ( ask_found != ask_orders.end() )
			{
				double price =  get<0>(ask_found->second);
				double volume =  get<1>(ask_found->second);
				ask_orders.erase(ask_found);
				askResult = update_vwap_monotonic(
					price, volume, m_asks, q_asks,
					sv1_at_pmin_asks, spv_at_pmin_asks, false);
			}
		}
	}
	else if (action == 'D')
	{
		// Delete from relevant order book.
		order_ids::iterator bid_found = bid_orders.find(order_id);
		if ( bid_found != bid_orders.end() )
		{
			double price =  get<0>(bid_found->second);
			double volume =  get<1>(bid_found->second);

			bid_orders.erase(bid_found);

			bidResult = update_vwap_monotonic(
				price, volume, m_bids, q_bids,
				sv1_at_pmin_bids, spv_at_pmin_bids, false);
		}
		else {
			order_ids::iterator ask_found = ask_orders.find(order_id);
			if ( ask_found != ask_orders.end() )
			{
				double price =  get<0>(ask_found->second);
				double volume =  get<1>(ask_found->second);

				ask_orders.erase(ask_found);
				askResult = update_vwap_monotonic(
					price, volume, m_asks, q_asks,
					sv1_at_pmin_asks, spv_at_pmin_asks, false);
			}
		}
	}
	
	cout<<"buys ="<<bidResult<<" sells ="<<askResult<<endl;
	
	if (bidResult-askResult<10000)
	{
		exchange_message msg(0, 0, 'B', 1000, 10);
		bool t=true;
		getOrder(msg, t);
	}
	if (bidResult-askResult<-10000)
	{
		exchange_message msg(0, 0, 'S', 1000, 10);
			bool t=true;
		getOrder(msg, t);
	}
	
	
}



/////////////////////////////////////////////////////////////////////////
//   Template client for writing a message to a socket



template <class message> class exchangeConnection
{
	//Template requirement that operator << is overloaded for the class message
public:
	exchangeConnection(boost::asio::io_service& io_service,
		tcp::resolver::iterator endpointIterator, int t):
		io_service_(io_service),
		socket_(io_service),
		type(t)
	{
//		cout<<"entering constructor (type t- "<<type<<endl;
		tcp::endpoint endpoint = *endpointIterator;
		socket_.async_connect(endpoint,
			boost::bind(&exchangeConnection<message>::handleConnection, this,
			boost::asio::placeholders::error, ++endpointIterator));	
			
//		cout<<"done connecting"<<endl;	
	}
	//send the message to server
	void write(const message& msg)
	{
		io_service_.post(boost::bind(&exchangeConnection::doWrite, this, msg));
	}
	//reading message from a server
	void read( boost::function<void (message& a)>  handler){
		cout<<"Entering read"<<endl;
		io_service_.post(boost::bind(&exchangeConnection::doRead, this, handler));
		cout<<"Exiting read"<<endl;
	}
	//close connection
	void close()
	{
		io_service_.post(boost::bind(&exchangeConnection::closeConnection, this));
	}
	
	
private:
	
	void handleConnection(const boost::system::error_code& error,
		tcp::resolver::iterator endpointIterator)
	{
//		cout<<"entering handle connect"<<endl;
		if (!error)
		{
			#ifdef DEBUG
				cout << "Establishing connnection to the server" << endl;
			#endif
			
			boost::asio::async_write(socket_,
				boost::asio::buffer((void*) &type, sizeof(int)),
				boost::bind(&exchangeConnection::doNothing, this,
				boost::asio::placeholders::error));

			#ifdef DEBUG
				cout<<"Finished connecting to a server"<<endl;
			#endif 
		}
		else if (endpointIterator != tcp::resolver::iterator())
		{
			//in case the connection is not established try to reconnect with different protocol
			#ifdef DEBUG
				cout << "Reconnecting to server, handle_connect" << endl;
			#endif
			socket_.close();
			tcp::endpoint endpoint = *endpointIterator;
			socket_.async_connect(endpoint,
				boost::bind(&exchangeConnection::handleConnection, this,
				boost::asio::placeholders::error, ++endpointIterator));
		}
	}
	void doWrite(message& msg)
	{	
		//writes a message to a server 	
		#ifdef DEBUG
		cout<<"Entered doWrite"<<endl;
		cout<<"Sending: "<<msg<<endl;
		#endif
		
		std::stringstream ss (std::stringstream::in | std::stringstream::out);
		ss<<msg;
		std::string strMessage=ss.str();
		char tail=10;
		strMessage=strMessage+tail;
		
		boost::asio::async_write(socket_,
			boost::asio::buffer(strMessage.data() ,strMessage.size()),
			boost::bind(&exchangeConnection::doNothing, this,
			boost::asio::placeholders::error));
		
//		messageQueue.push_back(msg);
//		handleWrite(boost::system::errc::make_error_code(boost::system::errc::success));

	}
	void doRead( boost::function<void (message& a)>  handler){
		//reading message from a server
		cout<<"Entering doRead"<<endl;
		char tail=10;
		boost::asio::async_read_until(socket_,
			readResponse,
			tail,
			boost::bind(&exchangeConnection::handleRead, this,
			boost::asio::placeholders::error, handler));
		cout<<"Exiting doRead"<<endl;
	}
	void handleRead( const boost::system::error_code& error, boost::function<void (message& a)>  handler)
	{
		#ifdef DEBUG
			cout<<"Converting buffer into a message"<<endl;
			cout<<"The number of chars in a buffer is -- "<<readResponse.size()<<endl;
		#endif
		

		
//		cout<<"entering handleRead"<<endl;
//		cout<<"read responce\n"<<boost::asio::buffer_cast<const char*> (readResponse.data())<<"\nbuffer done"<<endl;
		std::istream ist(&readResponse);
		
		string line;
		getline(ist, line);

		message msg;
		msg.getParameters(line);

//		msg.getParameters(ist);	
		cout<<"handleRead: "<<msg<<endl;
		handler(msg);			
	}
	void doNothing(const boost::system::error_code& error)
	{
		//TODO: see if there is a way to remove a handle from the async_write or put an empty handle
	}
	void closeConnection()
	{
		//closing connection to a server
		socket_.close();
	}
	
	boost::asio::io_service& io_service_;
	tcp::socket socket_;
	boost::asio::streambuf readResponse;
	deque<message> messageQueue;
	
	int type;
};

//////////////////////////////////////////////////////////////////////////
//   Class to deal with communication with the server.

class serverCommunication
{
public:
	serverCommunication( exchangeConnection<exchange_message> *p,  exchangeConnection<exchange_message> *t)
	: 
	proxy(p),
	toaster(t)
	{
//		&toaster=&t;
		//Anything else?
	}
	void readToaster(){
		toaster->read(boost::bind(&serverCommunication::handleToasterRead, this, _1));
	}
	void handleToasterRead(exchange_message& msg){
		cout<<"got message: "<< msg<<endl;
		vwap_stream_monotonic(msg, bid_orders, ask_orders, 
						boost::bind(&serverCommunication::writeProxy, this, _1, _2));
		readToaster();
		
//		exchange_message msg1(0, 0, 'B', 1000, 10.0);
//		writeProxy(msg1, true);
		
	}
	void writeProxy(exchange_message& msg, bool& needRead)
	{
		proxy->write(msg);
		if (needRead)
		{
			proxy->read(boost::bind(&serverCommunication::handleProxyRead, this, _1));
		}
	}
	void handleProxyRead(exchange_message& msg)
	{
		//keep some statistics about the message just recived
		cout<<"proxy recived from server: "<<msg<<endl;
	}

private:
	exchangeConnection<exchange_message> *proxy;
	exchangeConnection<exchange_message> *toaster;
	
	order_ids bid_orders;
	order_ids ask_orders;
};


int main(int argc, char* argv[])
{
	try
	{
		if (argc != 3)
		{
			std::cerr << "Usage: name <host> <port>\n";
			return 1;
		}



		boost::asio::io_service io_service;

		tcp::resolver resolver(io_service);
		tcp::resolver::query query("127.0.0.1", argv[2]);
		tcp::resolver::iterator iterator = resolver.resolve(query);


		exchangeConnection<exchange_message> proxy(io_service, iterator, 0);
		exchangeConnection<exchange_message> toaster(io_service, iterator, 1);
		
		
		
		serverCommunication ptr(&proxy, &toaster);
		ptr.readToaster();
	
//		exchange_message msg(0, 0, 'B', 1000, 10.0);
//		ptr.writeProxy(msg, true);

		boost::thread ioEvensLoop(boost::bind(&boost::asio::io_service::run, &io_service));
		
		//seen something want to write
		
//		io_service.run();
		

		
//		ptr.proxyWrite(msg);
		
		ioEvensLoop.join();
		
//		exchange_message msg;
//		toaster.read(msg);
		
//		cout<<msg<<endl;
		
/*
		serverCommunication ptr(proxy, toaster);

		ioEvensLoop.join();

*/

	}
	catch (std::exception& e)
	{
		std::cerr << "Exception: " << e.what() << "\n";
	}

	return 0;
}