

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

// Timestamp, order id, action, volume, price
typedef tuple<double, int, string, double, double> stream_tuple;

// Input stream
typedef list<stream_tuple> stream_buffer;

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
public:

	long long timestamp;
	int order_id;
	char action;
	double volume;
	double price;



	exchange_message():
	timestamp(10000), order_id(0), action('B'), volume(0), price (0)
		{};
	exchange_message(long t, int id, char a, double v, double p) :
	timestamp(t), order_id(id), action(a), volume(v), price (p)
		{}
};

ostream & operator<<( ostream & left, const exchange_message & message){

	left<<message.timestamp<<" "<<message.order_id<<" "<<message.action<<" "<<message.volume<<" "<<message.price;
	return left;
}



typedef std::deque<exchange_message> exchange_message_queue;

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
							order_ids& bid_orders, order_ids& ask_orders){
	
	

	
	//check if we really need all of these vars
//	struct timeval tvs, tve;
	
	double result = 0.0;
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

		result = update_vwap_monotonic(
			price, volume, m_bids, q_bids,
			sv1_at_pmin_bids, spv_at_pmin_bids);
	}
	else if (action == 'S')
	{
		// Insert asks
		double price = new_tuple.price;
		double volume = new_tuple.volume;

		ask_orders[order_id] = make_tuple(price, volume);
		result = update_vwap_monotonic(
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

			result = update_vwap_monotonic(
				price, volume, m_bids, q_bids,
				sv1_at_pmin_bids, spv_at_pmin_bids, false);

			if ( new_volume > 0 )
			{
				bid_orders[order_id] = new_pv;
				result = update_vwap_monotonic(
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

				result = update_vwap_monotonic(
					price, volume, m_asks, q_asks,
					sv1_at_pmin_asks, spv_at_pmin_asks, false);

				if ( new_volume > 0 ) {
					result = update_vwap_monotonic(
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

			result = update_vwap_monotonic(
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
				result = update_vwap_monotonic(
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

			result = update_vwap_monotonic(
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
				result = update_vwap_monotonic(
					price, volume, m_asks, q_asks,
					sv1_at_pmin_asks, spv_at_pmin_asks, false);
			}
		}
	}
	
	
}


class proxy_client{

public:
	proxy_client(boost::asio::io_service& io_service,
		tcp::resolver::iterator endpoint_iterator, bool type)
		: io_service_(io_service),
		isToaster(type),
		socket_(io_service),
		read_stream (std::stringstream::in | std::stringstream::out),
		counter(0)

	{
		tcp::endpoint endpoint = *endpoint_iterator;
		socket_.async_connect(endpoint,
			boost::bind(&proxy_client::handle_connect, this,
			boost::asio::placeholders::error, ++endpoint_iterator));
		result = 0.0;
		tuple_counter = 0;

		tup_sum = 0.0;
		tup_sum_usec = 0.0;

		gettimeofday(&tvs, NULL);
	}
	void write(const exchange_message& msg){
		io_service_.post(boost::bind(&proxy_client::do_write, this, msg));
	}
	void read(const exchange_message& msg){
		io_service_.post(boost::bind(&proxy_client::do_read, this, msg));
	}
	void close()
	{
		io_service_.post(boost::bind(&proxy_client::do_close, this));
	}

private:

	void handle_connect(const boost::system::error_code& error,
		tcp::resolver::iterator endpoint_iterator)
	{
		if (!error)
		{
			cout << "entering handle_connect 1" << endl;
			/*
			unsigned int size_input=boost::asio::read(socket_,
				boost::asio::buffer(message, 5));


			unsigned size_input = 0;
			boost::asio::async_read(socket_,
				boost::asio::buffer(message, 512),
				boost::asio::transfer_at_least(5),
				boost::bind(&proxy_client::handle_yanif_read, this,
				boost::asio::placeholders::error));
			*/

			//cout << "after the output. 23875081347598  input size is ___"<<size_input << endl;

			if(isToaster){
				cout<<"sending message that this is toaster 3"<<endl;
				int x=1;
				boost::asio::async_write(socket_,
					boost::asio::buffer((void*) &x, sizeof(int)),
					boost::bind(&proxy_client::handle_toaster_read, this,
					boost::asio::placeholders::error));
			}else{
				cout<<"sending message that this is proxy 2"<<endl;
				int x=0;
				exchange_message msg;
				msg.price=10;
				msg.volume=20;
				write_msgs_.push_back(msg);
				boost::asio::async_write(socket_,
					boost::asio::buffer((void*) &x, sizeof(int)),
					boost::bind(&proxy_client::handle_proxy_write, this,
					boost::asio::placeholders::error));
			}
			cout<<"finished handle connect 4"<<endl;
		}
		else if (endpoint_iterator != tcp::resolver::iterator())
		{
			cout << "Reconnecting." << endl;
			socket_.close();
			tcp::endpoint endpoint = *endpoint_iterator;
			socket_.async_connect(endpoint,
				boost::bind(&proxy_client::handle_connect, this,
				boost::asio::placeholders::error, ++endpoint_iterator));
		}
	}


	void do_write(exchange_message msg)
	{
//		cout<<"Function do_write called by a client shouldn be here (not implemented). 5"<<endl;
		
		
		//TODO: the question is how to deal with the return
		
		cout<<"Entering do write 5"<<endl;
		write_msgs_.push_back(msg);
		handle_proxy_write(boost::system::errc::make_error_code(boost::system::errc::success));
		
	/*
		bool write_in_progress = !write_msgs_.empty();
		write_msgs_.push_back(msg);
		out_msg.push_back(msg);

		if (!write_in_progress)
		{


			char order=write_msgs_.front().action;
//			char order='B';
			std::stringstream ss (std::stringstream::in | std::stringstream::out);

			ss<<write_msgs_.front();

			write_msgs_.pop_front();
			std::string message=ss.str();

			if (order=='B' || order=='S'){
				counter=5;
				boost::asio::async_write(socket_,
					boost::asio::buffer(message.data() , message.size()),
					boost::bind(&proxy_client::handle_read, this,
					boost::asio::placeholders::error));
			}	else{
				boost::asio::async_write(socket_,
					boost::asio::buffer(message.data() ,message.size()),
					boost::bind(&proxy_client::handle_write, this,
					boost::asio::placeholders::error));
			}
		}
	*/
	}
	void do_read(exchange_message msg){
		handle_toaster_read(boost::system::errc::make_error_code(boost::system::errc::success));
	}

	void do_close()
	{
		socket_.close();
	}

	void handle_proxy_read(const boost::system::error_code& error)
	{
		cout<<"entering handle_proxy_read 11"<<endl;
		if (!error)
		{
			boost::asio::async_read_until(socket_,
				proxy_read_response, "\n",
				boost::bind(&proxy_client::do_proxy_read, this,
				boost::asio::placeholders::error));

//			if (counter>0){
//				counter--;
//				cout<<"No error in handle read "<<counter<<endl;
				/*
//				for (int i=0; i<512; i++)
//					cout<<message[i];
//				cout<<endl;
//				read_stream<<message;
*/


//			}else{
				//TODO: convert read_stream to send format and send it away 
//			}
		}
		else
		{
			do_close();
		}
	}

	void do_proxy_read(const boost::system::error_code& error)
	{
		cout<<"entering do_proxy_read 12"<<endl;
		std::istream ist(&proxy_read_response);
		string s;
		ist >> s;
		char * char_ptr;

		exchange_message msg;
		
		msg.timestamp=string_conversion(s);
		ist >> s;
		msg.order_id=atoi(s.c_str());
		ist >> s;
		msg.action=s.c_str()[0];
		ist >> s;
		msg.volume=atof(s.c_str());
		ist >> s;
		msg.price=atof(s.c_str());
		
//		ist>>msg.timestamp;
		
//		cout<<temp<<endl;
//		ist >> s;
//		cout<<s<<endl;
		
/*		sscanf(s.c_str(), "%ld %d %c %lf %lf",
			&(msg.timestamp), &(msg.order_id), &(msg.action),
			&(msg.volume), &(msg.price));
*/
		cout << "Received: " << msg << endl;

		//handle_proxy_read(boost::system::errc::make_error_code(boost::system::errc::success));
		handle_proxy_write(boost::system::errc::make_error_code(boost::system::errc::success));
		//TODO: this should go to handle_proxy_write
	}

	void handle_toaster_read(const boost::system::error_code& error)
	{
		cout<<"entered handle_toaster_read 13"<<endl;
		if (!error)
		{

			cout<<"No error in handle_toaster_read 14 "<<endl;
			boost::asio::streambuf response;
			boost::asio::async_read_until(socket_,
				toaster_read_response,
				"\n",
				boost::bind(&proxy_client::do_toaster_read, this,
				boost::asio::placeholders::error));

		} else{
			do_close();
		}
	}

	void do_toaster_read(const boost::system::error_code& error)
	{
		cout<<"entering do_toaster_read 15"<<endl;
		cout<<"number of chars available 16  -- "<<toaster_read_response.size()<<endl;
		std::istream ist(&toaster_read_response);
		string s;
		ist >> s;
		char * char_ptr;

		exchange_message msg;
		
		msg.timestamp=string_conversion(s);
		ist >> s;
		msg.order_id=atoi(s.c_str());
		ist >> s;
		msg.action=s.c_str()[0];
		ist >> s;
		msg.volume=atof(s.c_str());
		ist >> s;
		msg.price=atof(s.c_str());
		
	

		cout << "Received: " << msg << endl;
		
		cout<<"Calling vwap"<<endl;
		vwap_stream_monotonic(msg, bid_orders, ask_orders);
		
		//TODO: deal with just recived message.

		handle_toaster_read(boost::system::errc::make_error_code(boost::system::errc::success));
	}

	void handle_proxy_write(const boost::system::error_code& error)
	{
		cout<<"entered handle_proxy_write 6"<<endl;
		if (!error)
		{
			cout<<"no_errors in handle_proxy_write 7"<<endl;

			bool no_write_needed = write_msgs_.empty();

			if (!no_write_needed)
			{
				char order=write_msgs_.front().action;
//				char order='B';
				std::stringstream ss (std::stringstream::in | std::stringstream::out);

				ss<<write_msgs_.front();

				write_msgs_.pop_front();
				std::string message=ss.str();
				message=message+'\n';
				if (order=='B' || order=='S'){
//					counter=5;
					cout<<"Doing a buy/sell order in handle_proxy_write 8"<<endl;
					cout<<message<<endl;
					boost::asio::async_write(socket_,
						boost::asio::buffer(message.data() , message.size()),
						boost::bind(&proxy_client::handle_proxy_read, this,
						boost::asio::placeholders::error));
					cout<<"Message sent 8.5"<<endl;
				}
				else{
					cout<<"Doing a other orders in handle_proxy_write 9"<<endl;
					boost::asio::async_write(socket_,
						boost::asio::buffer(message.data() ,message.size()),
						boost::bind(&proxy_client::handle_proxy_write, this,
						boost::asio::placeholders::error));
				}
			} else{

			/*
			//TODO: stall on a message or sleep until get a message
				boost::asio::async_write(socket_,
					boost::asio::buffer(message , strlen(message)),
					boost::bind(&proxy_client::handle_proxy_read, this,
					boost::asio::placeholders::error));
			*/

			}

		}
		else
		{
			do_close();
		}
		cout<<"exiting handle proxi write 10"<<endl;
	}

		/*
	void handle_write_stream(const boost::system::error_code& error){
		if (!error){

			string message;
			if (ss>>message){
				boost::asio::async_write(socket_,
					boost::asio::buffer(message.data() , message.size()),
					boost::bind(&proxy_client::handle_read, this,
					boost::asio::placeholders::error));
			}

		}else{
			do_close();
		}
	}

	void handle_write(const boost::system::error_code& error)
	{
		cout<< "inside handle_write "<<message<<endl;
		if (!error)
		{

		}
		else
		{
			do_close();
		}
	}
	void handle_write2(const boost::system::error_code& error, unsigned int i)
	{
		cout<< "inside handle_write2 "<<i<<endl;
		if (!error)
		{

		}
		else
		{
			do_close();
		}
	}

	void handle_yanif_read(const boost::system::error_code& error)
	{
		cout<< "inside handle_yanif_read "<<endl;
		if (!error)
		{
			cout<< "no error handle_yanif_read "<<endl;
		}
		else
		{
			do_close();
		}
		cout<< "done handle_yanif_read "<<endl;
	}
	*/
	
	long long string_conversion(string s){
		long long value=0;
		
		for (int i=0; i<s.length(); i++){
			value=value*10+atoi((s.substr(i,1)).c_str());
		}
		return value;
	}

	char* body()
	{
		return message;
	}

	boost::asio::io_service& io_service_;
	tcp::socket socket_;	
	exchange_message_queue write_msgs_;
	exchange_message_queue out_msg;
	stringstream read_stream ;
	char message[512];
	int counter;
	bool isToaster;

	boost::asio::streambuf proxy_read_response;
	boost::asio::streambuf toaster_read_response;
	
	order_ids bid_orders;
	order_ids ask_orders;
	
	struct timeval tvs, tve;
	double result;
	unsigned long tuple_counter;

	double tup_sum ;
	double tup_sum_usec ;


};




int main(int argc, char* argv[])
{
	try
	{
		if (argc != 3)
		{
			std::cerr << "Usage: chat_client <host> <port>\n";
			return 1;
		}

		boost::asio::io_service io_service;

		tcp::resolver resolver(io_service);
		tcp::resolver::query query("127.0.0.1", argv[2]);
		tcp::resolver::iterator iterator = resolver.resolve(query);

		proxy_client toast(io_service, iterator, true);


		boost::thread toast_thread(boost::bind(&boost::asio::io_service::run, &io_service));
		

		boost::asio::io_service io_proxy_service;

		tcp::resolver proxy_resolver(io_proxy_service);
		tcp::resolver::query proxy_query("127.0.0.1", argv[2]);
		tcp::resolver::iterator proxy_iterator = proxy_resolver.resolve(proxy_query);

		proxy_client proxy(io_proxy_service, proxy_iterator, false);


		boost::thread proxy_thread(boost::bind(&boost::asio::io_service::run, &io_proxy_service));
		

		exchange_message msg;
//		cout << "Point A" << endl;

		/*
		exchange_message msg;

		c.write(msg);
		*/

		/* TODO:

		while (message){
			c.write(msg)
			}
		*/
			//io_service.run();
			//c.close();
			toast_thread.join();
//			proxy_thread.join();




		}
		catch (std::exception& e)
		{
			std::cerr << "Exception: " << e.what() << "\n";
		}

		return 0;
	}
