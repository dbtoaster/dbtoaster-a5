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

using namespace std;
using namespace tr1;

#define DBT_HASH_MAP unordered_map
#define DBT_HASH_SET unordered_set

exec sql include sqlca;

exec sql whenever sqlerror sqlprint;
exec sql whenever not found sqlprint;

//#define DEBUG 1
#define MEMORY 1
//#define BULK 1
//#define LOG_INPUT 1
#define OUTPUT 1
#define BULK_COPY 1

// Timestamp, order id, action, volume, price
typedef tuple<double, int, string, double, double> stream_tuple;

// Input stream
typedef list<stream_tuple> stream_buffer;


// Order books
// -- note we need to keep the order ids around for applying actions.

// Timestamp, price, volume
typedef tuple<double, double, double> input_tuple;

// order id -> order
typedef map<int, input_tuple> order_book;

// timestamp * price -> volume
typedef map<tuple<double, double>, double> sorted_order_book;

// DBToaster data structures for VWAP computation

// price * volume -> sum(vol), count(*) as rc from bids where bids.price > price
typedef map<tuple<double, double>, tuple<double, int> > bcv_index;

// total_volume -> sum(price * volume), count(price * volume) from bcv where bcv.cumsum < total_volume
typedef map<double, tuple<double, double> > vwap_index;

// total_volume -> count(*), avg(rc) from bcv group by cumsum_volume
// -- note rc must be the same for all tuples in the group, i.e. avg(rc) = rc
//typedef map<double, tuple<int, int> > cvol_index;

// total_volume -> price, count(*), avg(rc) from bcv group by cumsum_volume, price
// -- note rc must be the same for all tuples in the group, i.e. avg(rc) = rc
typedef DBT_HASH_MAP<double, DBT_HASH_MAP<double, tuple<int, int> > > cvol_index;


struct vwap_sum
{
	double sbbB;
	double sBBb;
	double sBbb;
	double sBbB;
	double sbbb;
	double sbBb;
};

double sBBB;
typedef DBT_HASH_MAP<double, double> ts_bBB;
typedef DBT_HASH_MAP<double, DBT_HASH_MAP<double, vwap_sum> > pv_tsBBB;

ts_bBB pv_sbBB;
pv_tsBBB pv_sBBB;

struct vwap_count
{
	double cbbB;
	double cBBb;
	double cBbb;
	double cBbB;
	double cbbb;
	double cbBb;
};

double cBBB;
typedef DBT_HASH_MAP<double, double> tc_bBB;
typedef DBT_HASH_MAP<double, DBT_HASH_MAP<double, vwap_count> > pv_tcBBB;

tc_bBB pv_cbBB;
pv_tcBBB pv_cBBB;


void split(string str, string delim, vector<string>& results)
{
	int cutAt;
	while( (cutAt = str.find_first_of(delim)) != str.npos )
	{
		if(cutAt > 0)
			results.push_back(str.substr(0,cutAt));

		str = str.substr(cutAt+1);
	}

	if(str.length() > 0)
		results.push_back(str);
}

void split(int num_fields, char delimiter, char* data, vector<int>& r)
{
	char* start = data;
	char* end = start;

	r.resize(num_fields);
	for (int i = 0; i < num_fields; ++i)
	{
		while ( *end && *end != delimiter ) ++end;
		if ( start == end ) {
			cerr << "Invalid field " << i << endl;
			r.clear();
			return;
		}
		if ( *end == '\0' && i != (num_fields-1) ) {
			cerr << "Invalid field " << i << endl;
			r.clear();
			return;
		}
		*end = '\0';
		int f = atoi(start);
		start = ++end;
		r[i] = f;
	}
}

inline void print_result(long sec, long usec, const char *text,
	ofstream* log = NULL, bool db = true)
{
        if (usec < 0)
        {
                sec--;
                usec+=1000000;
        }

		char d[64];
		sprintf(&(d[0]), "%li.%06li", sec, usec);
		string duration(d);

        cout << duration << " s for '" << text << "'" << endl;

        if ( db ) exec sql vacuum;

        if ( log != NULL ) {
        	(*log) << duration << "," << text << endl;
    	}

        //usleep(200000);
}


// Data structures
struct stream {
	virtual bool stream_has_inputs() = 0;
	virtual stream_tuple next_input() = 0;
};

struct test_stream : public stream
{
	stream_buffer s;

	void init_stream() {
		for ( int i = 1; i < 6; ++i)
			s.push_back(make_tuple(i*1.0, i, "B", i*10.0, i*1.0));
	}

	bool stream_has_inputs() {
		return !s.empty();
	}

	stream_tuple next_input() {
		stream_tuple r = s.front();
		s.pop_front();
		return r;
	}
};

struct file_stream : public stream
{
	ifstream* input_file;
	stream_buffer buffer;
	unsigned int buffer_count;
	unsigned long line;
	unsigned long threshold;
	unsigned long buffer_size;

	file_stream(string file_name, unsigned int c)
		: buffer_count(c), line(0), buffer_size(0)
	{
		input_file = new ifstream(file_name.c_str());
		if ( !(input_file->good()) )
			cerr << "Failed to open file " << file_name << endl;

		threshold =
			static_cast<long>(ceil(static_cast<double>(c) / 10));
	}

	inline bool parse_line(int line, char* data, stream_tuple& r)
	{
		char* start = data;
		char* end = start;

		char action;

		for (int i = 0; i < 5; ++i)
		{
			while ( *end && *end != ',' ) ++end;
			if ( start == end ) {
				cerr << "Invalid field " << i << " line " << line << endl;
				return false;
			}
			if ( *end == '\0' && i != 4 ) {
				cerr << "Invalid field " << i << " line " << line << endl;
				return false;
			}
			*end = '\0';

			switch (i) {
			case 0:
				get<0>(r) = atof(start);
				break;

			case 1:
				get<1>(r) = atoi(start);
				break;

			case 2:
				action = *start;
				if ( !(action == 'B' || action == 'S' ||
						action == 'E' || action == 'F' ||
						action == 'D' || action == 'X' ||
						action == 'C' || action == 'T') )
				{
					cerr << "Invalid action "
						<< action << " field " << i << " line " << line << endl;
					return false;
				}

				get<2>(r) = action;
				break;

			case 3:
				get<3>(r) = atof(start);
				break;

			case 4:
				get<4>(r) = atof(start);
				break;

			default:
				cerr << "Invalid field " << i << " line " << line << endl;
				break;
			}

			start = ++end;
		}

		return true;
	}

	tuple<bool, stream_tuple> read_tuple()
	{
		char buf[256];
		input_file->getline(buf, sizeof(buf));
		++line;

		stream_tuple r;
		if ( !parse_line(line, buf, r) ) {
			cerr << "Failed to parse record at line " << line << endl;
			return make_tuple(false, stream_tuple());
		}

		return make_tuple(true, r);
	}

	void buffer_stream()
	{
		while ( buffer_size < buffer_count && input_file->good() )
		{
			tuple<bool, stream_tuple> valid_input = read_tuple();
			if ( !get<0>(valid_input) )
				break;

			buffer.push_back(get<1>(valid_input));
			++buffer_size;
		}
	}

	void init_stream() { buffer_stream(); }

	bool stream_has_inputs() {
		return buffer_size > 0;
	}

	stream_tuple next_input()
	{
		if ( buffer_size < threshold && input_file->good() )
			buffer_stream();

		stream_tuple r = buffer.front();
		buffer.pop_front();
		--buffer_size;
		return r;
	}
};


///////////////////////////////////////
//
// Bulk loading and dispatch helper

struct vwap_query_dispatcher
{
	virtual void init() {}
	virtual void query() {}
	virtual void final() {}
};

void write_bulk_file(ofstream* bids_file, ofstream* asks_file,
	list< tuple<int, int, double, double> >& pending_bids,
	list< tuple<int, int, double, double> >& pending_asks)
{
	list< tuple<int, int, double, double> >::iterator pb_it = pending_bids.begin();
	list< tuple<int, int, double, double> >::iterator pb_end = pending_bids.end();

	for (; pb_it != pb_end; ++pb_it) {
		(*bids_file) << get<0>(*pb_it) << "," << get<1>(*pb_it) << ","
			<< get<2>(*pb_it) << "," << get<3>(*pb_it) << endl;
	}

	pb_it = pending_asks.begin();
	pb_end = pending_asks.end();

	for (; pb_it != pb_end; ++pb_it) {
		(*asks_file) << get<0>(*pb_it) << "," << get<1>(*pb_it) << ","
			<< get<2>(*pb_it) << "," << get<3>(*pb_it) << endl;
	}
}

void vwap_bulk(string query_type, string directory,
	long query_freq, long copy_freq,
	stream* s, vwap_query_dispatcher* d,
	ofstream* results, ofstream* log, bool dump = false)
{
	// Set up staging files for bulk insertion
	string bids_file_name = directory+"/copy_bids";
	string asks_file_name = directory+"/copy_asks";

	ofstream* bids_file = new ofstream(bids_file_name.c_str());
	ofstream* asks_file = new ofstream(asks_file_name.c_str());

	string bids_copy_stmt =
		"copy bids (ts, id, price, volume) from '" +
		bids_file_name + "' with delimiter ','";

	string asks_copy_stmt =
		"copy asks (ts, id, price, volume) from '" +
		asks_file_name + "' with delimiter ','";

	cout << "Bids copy: " << bids_copy_stmt << endl;
	cout << "Asks copy: " << asks_copy_stmt << endl;

	// Exchange message data structures
	order_book bids;
	order_book asks;

	DBT_HASH_SET<int> pending_bid_ids;
	DBT_HASH_SET<int> pending_ask_ids;
	typedef list< tuple<int, int, double, double> > pending_bids;
	typedef list< tuple<int, int, double, double> > pending_asks;
	pending_bids p_bids;
	pending_asks p_asks;

	exec sql connect to orderbook;

	exec sql set autocommit to on;

	exec sql begin declare section;
	int order_id;
	double ts, v, p, new_volume;

	#ifdef BULK_COPY
	const char* bids_copy_str = bids_copy_stmt.c_str();
	const char* asks_copy_str = asks_copy_stmt.c_str();
	#endif
	exec sql end declare section;

	long tuple_counter = 0;

	cout << "VWAP bulk using query frequency " << query_freq << endl;

	d->init();

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	while ( s->stream_has_inputs() )
	{
		stream_tuple in = s->next_input();

		ts = get<0>(in);
		order_id = get<1>(in);
		string action = get<2>(in);
		v = get<3>(in);
		p = get<4>(in);

		#ifdef DEBUG
		cout << "Tuple: order " << order_id << ", " << action
			<< " t=" << get<0>(in) << " p=" << get<4>(in)
			<< " v=" << get<3>(in) << endl;
		#endif

		if (action == "B") {
			// Insert bids
			bids[order_id] = make_tuple(ts, p, v);

			p_bids.push_back(make_tuple(ts, order_id, p, v));
			pending_bid_ids.insert(order_id);

		}

		else if (action == "S") {
			// Insert asks
			asks[order_id] = make_tuple(ts, p, v);

			p_asks.push_back(make_tuple(ts, order_id, p, v));
			pending_ask_ids.insert(order_id);

		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.

			// Note, we process these updates by attempting to apply them to
			// pending entries first, otherwise we force through to database.
			// This reorders updates to execute before any pending (independent)
			// insertions, and will result in different query outputs for now...

			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double order_v = get<2>(found->second);
				new_volume = order_v - v;

				bids[order_id] = make_tuple(order_ts, order_p, new_volume);

				DBT_HASH_SET<int>::iterator pending_bid_id_found =
					pending_bid_ids.find(order_id);

				if ( pending_bid_id_found != pending_bid_ids.end() )
				{
					tuple<int, int, double, double> old_entry =
						make_tuple(order_ts, order_id, order_p, order_v);

					tuple<int, int, double, double> new_entry =
						make_tuple(order_ts, order_id, order_p, new_volume);

					pending_bids::iterator p_bid_found =
						find(p_bids.begin(), p_bids.end(), old_entry);

					assert ( p_bid_found != p_bids.end() );

					pending_bids::iterator next = p_bids.erase(p_bid_found);
					p_bids.insert(next, new_entry);
				}
				else
				{
					exec sql update bids set volume = :new_volume where id = :order_id;
				}

			}
			else
			{
				found = asks.find(order_id);
				if ( found != asks.end() )
				{
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);
					new_volume = order_v - v;

					asks[order_id] = make_tuple(order_ts, order_p, new_volume);

					DBT_HASH_SET<int>::iterator pending_ask_id_found =
						pending_ask_ids.find(order_id);
					if ( pending_ask_id_found != pending_ask_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						tuple<int, int, double, double> new_entry =
							make_tuple(order_ts, order_id, order_p, new_volume);

						pending_asks::iterator p_ask_found =
							find(p_asks.begin(), p_asks.end(), old_entry);

						assert ( p_ask_found != p_asks.end() );

						pending_asks::iterator next = p_asks.erase(p_ask_found);
						p_asks.insert(next, new_entry);
					}
					else
					{
						exec sql update asks set volume = :new_volume where id = :order_id;
					}
				}
				else
					cerr << "Unmatched order execution " << order_id << endl;
			}
		}

		else if (action == "F")
		{
			// Order executed in full

			// Note, we process these deletions by attempting to apply them to
			// pending entries first, otherwise we force through to database.
			// This reorders deletions to execute before any pending (independent)
			// insertions, and will result in different query outputs for now...

			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double order_v = get<2>(found->second);

				DBT_HASH_SET<int>::iterator pending_bid_id_found =
					pending_bid_ids.find(order_id);

				if ( pending_bid_id_found != pending_bid_ids.end() )
				{
					tuple<int, int, double, double> old_entry =
						make_tuple(order_ts, order_id, order_p, order_v);

					pending_bids::iterator p_bid_found =
						find(p_bids.begin(), p_bids.end(), old_entry);

					assert ( p_bid_found != p_bids.end() );

					p_bids.erase(p_bid_found);
					pending_bid_ids.erase(order_id);
				}
				else
				{
					exec sql delete from bids where id = :order_id;
				}

				bids.erase(found);

				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(order_id);
				if ( found != asks.end() )
				{
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);

					DBT_HASH_SET<int>::iterator pending_ask_id_found =
						pending_ask_ids.find(order_id);
					if ( pending_ask_id_found != pending_ask_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						pending_asks::iterator p_ask_found =
							find(p_asks.begin(), p_asks.end(), old_entry);

						assert ( p_ask_found != p_asks.end() );

						p_asks.erase(p_ask_found);
						pending_ask_ids.erase(order_id);
					}
					else
					{
						exec sql delete from asks where id = :order_id;
					}

					asks.erase(found);

					// TODO: should we remove volume from the top of the asks book?
				}
			}

		}

		else if (action == "D")
		{
			// Delete from relevant order book.

			// Note, we process these deletions by attempting to apply them to
			// pending entries first, otherwise we force through to database.
			// This reorders deletions to execute before any pending (independent)
			// insertions, and will result in different query outputs for now...

			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double order_v = get<2>(found->second);

				DBT_HASH_SET<int>::iterator pending_bid_id_found =
					pending_bid_ids.find(order_id);

				if ( pending_bid_id_found != pending_bid_ids.end() )
				{
					tuple<int, int, double, double> old_entry =
						make_tuple(order_ts, order_id, order_p, order_v);

					pending_bids::iterator p_bid_found =
						find(p_bids.begin(), p_bids.end(), old_entry);

					assert ( p_bid_found != p_bids.end() );

					p_bids.erase(p_bid_found);
					pending_bid_ids.erase(order_id);
				}
				else
				{
					exec sql delete from bids where id = :order_id;
				}

				bids.erase(found);
			}

			else
			{
				found = asks.find(order_id);
				if ( found != asks.end() )
				{

					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);

					DBT_HASH_SET<int>::iterator pending_ask_id_found =
						pending_ask_ids.find(order_id);

					if ( pending_ask_id_found != pending_ask_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						pending_asks::iterator p_ask_found =
							find(p_asks.begin(), p_asks.end(), old_entry);

						assert ( p_ask_found != p_asks.end() );

						p_asks.erase(p_ask_found);
						pending_ask_ids.erase(order_id);
					}
					else
					{
						exec sql delete from asks where id = :order_id;
					}

					asks.erase(found);
				}
				else
					cerr << "Unmatched order deletion " << order_id << endl;
			}
		}

		/*
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		*/

		++tuple_counter;
		if ( (tuple_counter % 10000) == 0 )
		{
			cout << "Processed " << tuple_counter << " tuples." << endl;
		}

		if ( (tuple_counter % copy_freq) == 0 )
		{
			// Write out bulk files.
			write_bulk_file(bids_file, asks_file, p_bids, p_asks);
			p_bids.clear(); p_asks.clear();
			pending_bid_ids.clear(); pending_ask_ids.clear();

			// Clean up update files.
			bids_file->flush(); bids_file->close(); delete bids_file;
			asks_file->flush(); asks_file->close(); delete asks_file;

			// Copy bids and asks updates to postgres.
			exec sql execute immediate :bids_copy_str;
			exec sql execute immediate :asks_copy_str;

			// Open up new files for the next update.
			bids_file = new ofstream(bids_file_name.c_str(), ios::trunc);
			asks_file = new ofstream(asks_file_name.c_str(), ios::trunc);
		}

		if ( (tuple_counter % query_freq) == 0 )
			d->query();
	}

	// Write out bulk files.
	write_bulk_file(bids_file, asks_file, p_bids, p_asks);
	p_bids.clear(); p_asks.clear();
	pending_bid_ids.clear(); pending_ask_ids.clear();

	// Do final query
	bids_file->flush(); bids_file->close(); delete bids_file;
	asks_file->flush(); asks_file->close(); delete asks_file;

	exec sql execute immediate :bids_copy_str;
	exec sql execute immediate :asks_copy_str;

	d->final();

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		query_type.c_str(), log);

	exec sql set autocommit = off;
	exec sql disconnect;
}

///////////////////////////////////////////
//
// ECPG

tuple<bool, double> ecpg_query(ofstream* log)
{
	struct timeval tvs, tve;

	exec sql begin declare section;
	double vwap;
	int vwap_ind;
	exec sql end declare section;

	exec sql begin transaction;

	gettimeofday(&tvs, NULL);

	exec sql select avg(price*volume) into :vwap :vwap_ind from
		(select sum(volume) as total_volume from bids) as bv,
		(select b2.price, b2.volume, sum(b1.volume) as cumsum_volume
			from bids b1, bids b2
			where b1.price < b2.price
			group by b2.price, b2.volume) as bcv
		where bcv.cumsum_volume < 0.25 * bv.total_volume;

	exec sql commit;

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP ecpg iter", log);

	return make_tuple(vwap_ind == 0, vwap);
}

struct vwap_ecpg_dispatcher : vwap_query_dispatcher
{
	ofstream* log;
	ofstream* results;

	vwap_ecpg_dispatcher(ofstream* l, ofstream* r)
		: log(l), results(r)
	{}

	void init() {}

	void query() {
		// Do query
		tuple<bool, double> valid_result = ecpg_query(log);
		if ( get<0>(valid_result) )
			(*results) << get<1>(valid_result) << endl;
	}

	void final() {
		tuple<bool, double> valid_result = ecpg_query(log);
		if ( get<0>(valid_result) )
			(*results) << get<1>(valid_result) << endl;
	}
};

void vwap_snapshot_ecpg(string directory, long query_freq,
	stream* s, ofstream* results, ofstream* log, bool dump = false)
{
	order_book bids;
	order_book asks;

	#ifdef BULK_COPY
	string bids_file_name = directory+"/copy_bids";
	string asks_file_name = directory+"/copy_asks";

	ofstream* bids_file = new ofstream(bids_file_name.c_str());
	ofstream* asks_file = new ofstream(asks_file_name.c_str());

	string bids_copy_stmt =
		"copy bids (ts, id, price, volume) from '" +
		bids_file_name + "' with delimiter ','";

	string asks_copy_stmt =
		"copy asks (ts, id, price, volume) from '" +
		asks_file_name + "' with delimiter ','";

	cout << "Bids copy: " << bids_copy_stmt << endl;
	cout << "Asks copy: " << asks_copy_stmt << endl;

	DBT_HASH_SET<int> pending_bid_ids;
	DBT_HASH_SET<int> pending_ask_ids;
	typedef list< tuple<int, int, double, double> > pending_bids;
	typedef list< tuple<int, int, double, double> > pending_asks;
	pending_bids p_bids;
	pending_asks p_asks;
	#endif

	exec sql connect to orderbook;

	exec sql set autocommit to on;

	exec sql begin declare section;
	int order_id;
	double ts, v, p, new_volume;

	#ifdef BULK_COPY
	const char* bids_copy_str = bids_copy_stmt.c_str();
	const char* asks_copy_str = asks_copy_stmt.c_str();
	#endif
	exec sql end declare section;

	long tuple_counter = 0;

	cout << "VWAP ECPG using query frequency " << query_freq << endl;

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	while ( s->stream_has_inputs() )
	{
		stream_tuple in = s->next_input();

		ts = get<0>(in);
		order_id = get<1>(in);
		string action = get<2>(in);
		v = get<3>(in);
		p = get<4>(in);

		#ifdef DEBUG
		cout << "Tuple: order " << order_id << ", " << action
			<< " t=" << get<0>(in) << " p=" << get<4>(in)
			<< " v=" << get<3>(in) << endl;
		#endif

		if (action == "B") {
			// Insert bids
			bids[order_id] = make_tuple(ts, p, v);

			#ifdef BULK_COPY
				p_bids.push_back(make_tuple(ts, order_id, p, v));
				pending_bid_ids.insert(order_id);
			#else
				exec sql insert into bids values(:ts, :order_id, :p, :v);
			#endif
		}
		else if (action == "S") {
			// Insert asks
			asks[order_id] = make_tuple(ts, p, v);

			#ifdef BULK_COPY
				p_asks.push_back(make_tuple(ts, order_id, p, v));
				pending_ask_ids.insert(order_id);
			#else
				exec sql insert into asks values(:ts, :order_id, :p, :v);
			#endif
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() ) {
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double order_v = get<2>(found->second);
				new_volume = order_v - v;

				bids[order_id] = make_tuple(order_ts, order_p, new_volume);

				#ifdef BULK_COPY
					DBT_HASH_SET<int>::iterator pending_bid_id_found =
						pending_bid_ids.find(order_id);

					if ( pending_bid_id_found != pending_bid_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						tuple<int, int, double, double> new_entry =
							make_tuple(order_ts, order_id, order_p, new_volume);

						pending_bids::iterator p_bid_found =
							find(p_bids.begin(), p_bids.end(), old_entry);

						assert ( p_bid_found != p_bids.end() );

						pending_bids::iterator next = p_bids.erase(p_bid_found);
						p_bids.insert(next, new_entry);
					}
					else
					{
						exec sql update bids set volume = :new_volume where id = :order_id;
					}
				#else
					exec sql update bids set volume = :new_volume where id = :order_id;
				#endif

				// TODO: should we remove volume from the top of the asks book?
			}
			else
			{
				found = asks.find(order_id);
				if ( found != asks.end() ) {
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);
					new_volume = order_v - v;

					asks[order_id] = make_tuple(order_ts, order_p, new_volume);

					#ifdef BULK_COPY
						DBT_HASH_SET<int>::iterator pending_ask_id_found =
							pending_ask_ids.find(order_id);
						if ( pending_ask_id_found != pending_ask_ids.end() )
						{
							tuple<int, int, double, double> old_entry =
								make_tuple(order_ts, order_id, order_p, order_v);

							tuple<int, int, double, double> new_entry =
								make_tuple(order_ts, order_id, order_p, new_volume);

							pending_asks::iterator p_ask_found =
								find(p_asks.begin(), p_asks.end(), old_entry);

							assert ( p_ask_found != p_asks.end() );

							pending_asks::iterator next = p_asks.erase(p_ask_found);
							p_asks.insert(next, new_entry);
						}
						else
						{
							exec sql update asks set volume = :new_volume where id = :order_id;
						}
					#else
						exec sql update asks set volume = :new_volume where id = :order_id;
					#endif

					// TODO: should we remove volume from top of the bids book?
				}
				else
					cerr << "Unmatched order execution " << order_id << endl;
			}
		}

		else if (action == "F")
		{
			// Order executed in full
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				#ifdef BULK_COPY
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);

					DBT_HASH_SET<int>::iterator pending_bid_id_found =
						pending_bid_ids.find(order_id);

					if ( pending_bid_id_found != pending_bid_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						pending_bids::iterator p_bid_found =
							find(p_bids.begin(), p_bids.end(), old_entry);

						assert ( p_bid_found != p_bids.end() );

						p_bids.erase(p_bid_found);
						pending_bid_ids.erase(order_id);
					}
					else
					{
						exec sql delete from bids where id = :order_id;
					}
				#else
					exec sql delete from bids where id = :order_id;
				#endif

				bids.erase(found);

				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(order_id);
				if ( found != asks.end() )
				{
					#ifdef BULK_COPY
						double order_ts = get<0>(found->second);
						double order_p = get<1>(found->second);
						double order_v = get<2>(found->second);

						DBT_HASH_SET<int>::iterator pending_ask_id_found =
							pending_ask_ids.find(order_id);
						if ( pending_ask_id_found != pending_ask_ids.end() )
						{
							tuple<int, int, double, double> old_entry =
								make_tuple(order_ts, order_id, order_p, order_v);

							pending_asks::iterator p_ask_found =
								find(p_asks.begin(), p_asks.end(), old_entry);

							assert ( p_ask_found != p_asks.end() );

							p_asks.erase(p_ask_found);
							pending_ask_ids.erase(order_id);
						}
						else
						{
							exec sql delete from asks where id = :order_id;
						}
					#else
						exec sql delete from asks where id = :order_id;
					#endif

					asks.erase(found);

					// TODO: should we remove volume from the top of the asks book?
				}
			}
		}

		else if (action == "D")
		{
			// Delete from relevant order book.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{

				#ifdef BULK_COPY
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);

					DBT_HASH_SET<int>::iterator pending_bid_id_found =
						pending_bid_ids.find(order_id);

					if ( pending_bid_id_found != pending_bid_ids.end() )
					{
						tuple<int, int, double, double> old_entry =
							make_tuple(order_ts, order_id, order_p, order_v);

						pending_bids::iterator p_bid_found =
							find(p_bids.begin(), p_bids.end(), old_entry);

						assert ( p_bid_found != p_bids.end() );

						p_bids.erase(p_bid_found);
						pending_bid_ids.erase(order_id);
					}
					else
					{
						exec sql delete from bids where id = :order_id;
					}
				#else
					exec sql delete from bids where id = :order_id;
				#endif

				bids.erase(found);
			}

			else {
				found = asks.find(order_id);
				if ( found != asks.end() )
				{

					#ifdef BULK_COPY
						double order_ts = get<0>(found->second);
						double order_p = get<1>(found->second);
						double order_v = get<2>(found->second);

						DBT_HASH_SET<int>::iterator pending_ask_id_found =
							pending_ask_ids.find(order_id);

						if ( pending_ask_id_found != pending_ask_ids.end() )
						{
							tuple<int, int, double, double> old_entry =
								make_tuple(order_ts, order_id, order_p, order_v);

							pending_asks::iterator p_ask_found =
								find(p_asks.begin(), p_asks.end(), old_entry);

							assert ( p_ask_found != p_asks.end() );

							p_asks.erase(p_ask_found);
							pending_ask_ids.erase(order_id);
						}
						else
						{
							exec sql delete from asks where id = :order_id;
						}
					#else
						exec sql delete from asks where id = :order_id;
					#endif

					asks.erase(found);

				}
				else
					cerr << "Unmatched order deletion " << order_id << endl;
			}
		}

		/*
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		*/

		++tuple_counter;
		if ( (tuple_counter % 10000) == 0 )
		{
			cout << "Processed " << tuple_counter << " tuples." << endl;
		}

		if ( (tuple_counter % query_freq) == 0 )
		{
			#ifdef BULK_COPY
			// Write out bulk files.
			write_bulk_file(bids_file, asks_file, p_bids, p_asks);
			p_bids.clear(); p_asks.clear();
			pending_bid_ids.clear(); pending_ask_ids.clear();

			// Clean up update files.
			bids_file->flush(); bids_file->close(); delete bids_file;
			asks_file->flush(); asks_file->close(); delete asks_file;

			// Copy bids and asks updates to postgres.
			exec sql execute immediate :bids_copy_str;
			exec sql execute immediate :asks_copy_str;
			#endif

			// Do query
			tuple<bool, double> valid_result = ecpg_query(log);
			if ( get<0>(valid_result) )
				(*results) << get<1>(valid_result) << endl;

			#ifdef BULK_COPY
			// Open up new files for the next update.
			bids_file = new ofstream(bids_file_name.c_str(), ios::trunc);
			asks_file = new ofstream(asks_file_name.c_str(), ios::trunc);
			#endif
		}
	}

	#ifdef BULK_COPY
	// Write out bulk files.
	write_bulk_file(bids_file, asks_file, p_bids, p_asks);
	p_bids.clear(); p_asks.clear();
	pending_bid_ids.clear(); pending_ask_ids.clear();

	// Do final query
	bids_file->flush(); bids_file->close(); delete bids_file;
	asks_file->flush(); asks_file->close(); delete asks_file;

	exec sql execute immediate :bids_copy_str;
	exec sql execute immediate :asks_copy_str;
	#endif

	tuple<bool, double> valid_result = ecpg_query(log);
	if ( get<0>(valid_result) )
		(*results) << get<1>(valid_result) << endl;

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP ecpg", log);

	exec sql set autocommit = off;
	exec sql disconnect;
}


/////////////////////////////////////////////
//
// Materialized views

void set_up_matviews_and_triggers(string sql_file, ofstream* log)
{
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	ifstream sqlf(sql_file.c_str());

	if ( !sqlf.good() ) {
		cerr << "Failed to open sql cmds file " << sql_file << endl;
		return;
	}

	string cmds;

	while ( sqlf.good() ) {
		char buf[256];
		sqlf.getline(buf, sizeof(buf));

		string cmd(buf);
		cmds += (cmds.empty()? "" : "\n") + cmd;
	}

	cout << "Executing script " << cmds << endl;

	exec sql begin declare section;
	const char* cmds_str = cmds.c_str();
	exec sql end declare section;

	exec sql execute immediate :cmds_str;

	gettimeofday(&tve, NULL);

	print_result(
		tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP matview setup", log, false);
}

tuple<bool, double> matview_query(ofstream* log)
{
	struct timeval tvs, tve;

	exec sql begin declare section;
	double vwap;
	int vwap_ind;
	exec sql end declare section;

	exec sql begin transaction;

	gettimeofday(&tvs, NULL);

	exec sql select avg(price*volume) into :vwap :vwap_ind from vwap;

	exec sql commit;

	gettimeofday(&tve, NULL);

	print_result(
		tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP matview iter", log);

	return make_tuple(vwap_ind == 0, vwap);
}

struct vwap_matview_dispatcher : vwap_query_dispatcher
{
	string sql_file;
	ofstream* log;
	ofstream* results;

	vwap_matview_dispatcher(string sf, ofstream* l, ofstream* r)
		: sql_file(sf), log(l), results(r)
	{}

	void init() {
		set_up_matviews_and_triggers(sql_file, log);
	}

	void query() {
		tuple<bool, double> valid_result = matview_query(log);
		if ( get<0>(valid_result) )
			(*results) << get<1>(valid_result) << endl;
	}

	void final() {
		tuple<bool, double> valid_result = matview_query(log);
		if ( get<0>(valid_result) )
			(*results) << get<1>(valid_result) << endl;
	}
};

void vwap_matviews(string setup_cmds_file, string directory,
	long query_freq, long copy_freq,
	stream* s, ofstream* results, ofstream* log, bool dump = false)
{
	vwap_matview_dispatcher d(setup_cmds_file, log, results);

	string query_type = "VWAP matview";
	vwap_bulk(query_type, directory, query_freq, copy_freq, s, &d, results, log, dump);
}

void vwap_triggers(string sql_file, string directory, long query_freq,
	stream* s, ofstream* results, ofstream* log, bool dump = false)
{
	order_book bids;
	order_book asks;

	exec sql connect to orderbook;

	exec sql set autocommit to on;

	exec sql begin declare section;
	int order_id;
	double ts, v, p, new_volume;
	exec sql end declare section;

	set_up_matviews_and_triggers(sql_file, log);

	long tuple_counter = 0;

	cout << "VWAP triggers using query frequency " << query_freq << endl;

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);
	double tup_sum = 0.0;

	while ( s->stream_has_inputs() )
	{
		struct timeval tups, tupe;
		gettimeofday(&tups, NULL);

		stream_tuple in = s->next_input();

		ts = get<0>(in);
		order_id = get<1>(in);
		string action = get<2>(in);
		v = get<3>(in);
		p = get<4>(in);

		#ifdef DEBUG
		cout << "Tuple: order " << order_id << ", " << action
			<< " t=" << get<0>(in) << " p=" << get<4>(in)
			<< " v=" << get<3>(in) << endl;
		#endif

		if (action == "B") {
			// Insert bids
			bids[order_id] = make_tuple(ts, p, v);

			exec sql insert into bids values(:ts, :order_id, :p, :v);
		}
		else if (action == "S") {
			// Insert asks
			asks[order_id] = make_tuple(ts, p, v);

			exec sql insert into asks values(:ts, :order_id, :p, :v);
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() ) {
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double order_v = get<2>(found->second);
				new_volume = order_v - v;

				bids[order_id] = make_tuple(order_ts, order_p, new_volume);

				exec sql update bids set volume = :new_volume where id = :order_id;

				// TODO: should we remove volume from the top of the asks book?
			}
			else
			{
				found = asks.find(order_id);
				if ( found != asks.end() ) {
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double order_v = get<2>(found->second);
					new_volume = order_v - v;

					asks[order_id] = make_tuple(order_ts, order_p, new_volume);

					exec sql update asks set volume = :new_volume where id = :order_id;

					// TODO: should we remove volume from top of the bids book?
				}
				else
					cerr << "Unmatched order execution " << order_id << endl;
			}
		}

		else if (action == "F")
		{
			// Order executed in full
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				exec sql delete from bids where id = :order_id;
				bids.erase(found);

				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(order_id);
				if ( found != asks.end() )
				{
					exec sql delete from asks where id = :order_id;
					asks.erase(found);

					// TODO: should we remove volume from the top of the asks book?
				}
			}
		}

		else if (action == "D")
		{
			// Delete from relevant order book.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() )
			{
				exec sql delete from bids where id = :order_id;
				bids.erase(found);
			}

			else {
				found = asks.find(order_id);
				if ( found != asks.end() )
				{
					exec sql delete from asks where id = :order_id;
					asks.erase(found);
				}
				else
					cerr << "Unmatched order deletion " << order_id << endl;
			}
		}

		/*
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		*/

		++tuple_counter;
		if ( (tuple_counter % 10000) == 0 )
		{
			cout << "Processed " << tuple_counter << " tuples." << endl;
			cout << "Exec time " << (tup_sum / 10000.0) << endl;
			tup_sum = 0.0;
		}

		if ( (tuple_counter % query_freq) == 0 )
		{
			// Do query
			tuple<bool, double> valid_result = matview_query(log);
			if ( get<0>(valid_result) )
				(*results) << get<1>(valid_result) << endl;
		}

		gettimeofday(&tupe, NULL);
		long sec = tupe.tv_sec - tups.tv_sec;
		long usec = tupe.tv_usec - tups.tv_usec;
        if (usec < 0) { --sec; usec+=1000000; }
		tup_sum += (sec + (usec / 1000000.0));
	}

	tuple<bool, double> valid_result = matview_query(log);
	if ( get<0>(valid_result) )
		(*results) << get<1>(valid_result) << endl;

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP triggers", log);

	exec sql set autocommit = off;
	exec sql disconnect;
}


////////////////////////////////////////
//
// Naive compilation

double naive_query(ofstream* log, sorted_order_book& sorted_bids)
{
	struct timeval vwap_iter_s, vwap_iter_e;

	gettimeofday(&vwap_iter_s, NULL);

	// Query plan for computing VWAP.
	double k = 0.25;

	// B2 iterator
	sorted_order_book::iterator b2_it = sorted_bids.begin();
	sorted_order_book::iterator b2_end = sorted_bids.end();

	typedef map<tuple<double, double>, double> cumsum_groups;
	cumsum_groups cumsum_volumes;
	double total_volume = 0.0;

	// Indexed NL join b1, b2 where b1.price > b2.price
	// -- note b2 is outer so we can use forward iterators for walking over b1.
	for (; b2_it != b2_end; ++b2_it) {

		// B1 iterator
		sorted_order_book::iterator b1_it = sorted_bids.upper_bound(b2_it->first);
		sorted_order_book::iterator b1_end = sorted_bids.end();

		// Grouping columns
		double b2_price = get<0>(b2_it->first);
		double b2_volume = b2_it->second;

		tuple<double, double> cs_key = make_tuple(b2_price, b2_volume);
		cumsum_volumes[cs_key] = 0.0;

		// Move b1 to p+
		while ( get<0>(b1_it->first) == b2_price && b1_it != b1_end )
			++b1_it;

		// Group-by sum(b1.volume)
		for (; b1_it != b1_end; ++b1_it)
			cumsum_volumes[cs_key] += b1_it->second;

		total_volume += b2_volume;
	}

	// NL join bv.total_volume > bcv.cumsum_volume
	// -- note we use a single loop since total_volume is always a scalar.
	cumsum_groups::iterator cs_it = cumsum_volumes.begin();
	cumsum_groups::iterator cs_end = cumsum_volumes.end();

	double running_sum = 0.0;
	long running_count = 1;
	for (; cs_it != cs_end; ++cs_it)
	{
		double cumsum_v = cs_it->second;

		// where cumsum_volume < k * total_volume
		if ( cumsum_v < k * total_volume ) {

			// avg(price*volume)
			running_sum += (get<0>(cs_it->first) * get<1>(cs_it->first));
			running_count++;
		}
	}

	gettimeofday(&vwap_iter_e, NULL);

	print_result(
		vwap_iter_e.tv_sec - vwap_iter_s.tv_sec,
		vwap_iter_e.tv_usec - vwap_iter_s.tv_usec,
		"VWAP QP iter", log, false);

	return (running_sum / running_count);
}


void vwap_snapshot(stream* s, ofstream* results, ofstream* log,
	long query_freq, bool single_shot, bool dump = false)
{
	order_book bids;
	order_book asks;

	// Incrementally maintain sorted order books
	sorted_order_book sorted_bids;
	sorted_order_book sorted_asks;

	unsigned long tuple_counter = 0;

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	while ( s->stream_has_inputs() )
	{
		++tuple_counter;
		if ( (tuple_counter % 10000) == 0 ) {
			cout << "Processed " << tuple_counter << " tuples." << endl;
		}

		stream_tuple in = s->next_input();

		double ts = get<0>(in);
		int order_id = get<1>(in);
		string action = get<2>(in);
		double v = get<3>(in);
		double p = get<4>(in);

		if (action == "B") {
			// Insert bids
			bids[order_id] = make_tuple(ts, p, v);

			sorted_bids[make_tuple(p,ts)] = v;
		}
		else if (action == "S") {
			/*
			// Insert asks
			asks[order_id] = make_tuple(ts, p, v);

			sorted_asks[make_tuple(p,ts)] = v;
			*/
		}

		else if (action == "E") {
			// Partial execution based from order book according to order id.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() ) {
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);
				double new_volume = get<2>(found->second) - v;

				bids[order_id] = make_tuple(order_ts, order_p, new_volume);
				sorted_bids[make_tuple(order_p, order_ts)] = new_volume;

				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(order_id);
				if ( found != asks.end() ) {
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);
					double new_volume = get<2>(found->second) - v;

					asks[order_id] = make_tuple(order_ts, order_p, new_volume);
					sorted_asks[make_tuple(order_p, order_ts)] = new_volume;

					// TODO: should we remove volume from top of the bids book?
				}
				#ifdef DEBUG
				else
					cerr << "Unmatched order execution " << order_id << endl;
				#endif
			}
		}

		else if (action == "F") {
			// Order executed in full
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() ) {
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);

				bids.erase(found);
				sorted_bids.erase(make_tuple(order_p, order_ts));
				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(order_id);
				if ( found != asks.end() ) {
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);

					asks.erase(found);
					sorted_asks.erase(make_tuple(order_p, order_ts));
					// TODO: should we remove volume from the top of the asks book?
				}
			}
		}

		else if (action == "D") {
			// Delete from relevant order book.
			order_book::iterator found = bids.find(order_id);
			if ( found != bids.end() ) {
				double order_ts = get<0>(found->second);
				double order_p = get<1>(found->second);

				bids.erase(found);
				sorted_bids.erase(make_tuple(order_p, order_ts));
			}

			else {
				found = asks.find(order_id);
				if ( found != asks.end() ) {
					double order_ts = get<0>(found->second);
					double order_p = get<1>(found->second);

					asks.erase(found);
					sorted_asks.erase(make_tuple(order_p, order_ts));
				}
				#ifdef DEBUG
				else
					cerr << "Unmatched order deletion " << order_id << endl;
				#endif
			}
		}

		/*
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		*/

		/*
		order_book::iterator ob_it = bids.begin();
		order_book::iterator ob_end = bids.end();

		// Compute snapshot of VWAP.
		double running_bids_avg = 0.0;
		for (int i = 1; ob_it != ob_end; ++ob_it, ++i) {
			double pv = get<1>(ob_it->second) * get<2>(ob_it->second);
			running_bids_avg += ((pv - running_bids_avg) / i);
		}

		ob_it = asks.begin(); ob_end = asks.end();
		double running_asks_avg = 0.0;
		for (int i = 1; ob_it != ob_end; ++ob_it, ++i) {
			double pv = get<1>(ob_it->second) * get<2>(ob_it->second);
			running_asks_avg += ((pv - running_asks_avg) / i);
		}
		*/

		if ( (tuple_counter % query_freq) == 0 ) {
			double r = naive_query(log, sorted_bids);
			(*results) << r << endl;
		}
	}

	if ( single_shot ) {
		cout << "Final book size " << sorted_bids.size() << endl;
		double r = naive_query(log, sorted_bids);
		(*results) << r << endl;
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP QP", log, false);

}

///////////////////////////////////////////
//
// DBToaster

void print_bcv_index(bcv_index& sv1)
{
	bcv_index::iterator bcv_it = sv1.begin();
	bcv_index::iterator bcv_end = sv1.end();

	for (; bcv_it != bcv_end; ++bcv_it) {
		cout << get<0>(bcv_it->first) << ", "
			<< get<1>(bcv_it->first) << ", "
			<< get<0>(bcv_it->second) << ", "
			<< get<1>(bcv_it->second) << endl;
	}
}

void print_vwap_index(vwap_index& spv)
{
	vwap_index::iterator spv_it = spv.begin();
	vwap_index::iterator spv_end = spv.end();

	for (; spv_it != spv_end; ++spv_it)
	{
		cout << spv_it->first << ", "
			<< get<0>(spv_it->second) << ", "
			<< get<1>(spv_it->second) << endl;
	}
}

void print_cvol_index(cvol_index& cvc)
{
	cvol_index::iterator cvc_it = cvc.begin();
	cvol_index::iterator cvc_end = cvc.end();

	for (; cvc_it != cvc_end; ++cvc_it) {
		DBT_HASH_MAP<double, tuple<int, int> >::iterator p_it = cvc_it->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator p_end = cvc_it->second.end();

		for (; p_it != p_end; ++p_it) {
			cout << cvc_it->first << ", "
				<< p_it->first << ", "
				<< get<0>(p_it->second) << ", "
				<< get<1>(p_it->second) << endl;
		}
	}
}

double update_vwap(double price, double volume,
	double& stv, bcv_index& sv1, vwap_index& spv,
	cvol_index& cvc, bool insert = true)
{
	//cout << "At invoke " << spv.size() << endl;

	// Delta stv
	stv = (insert? stv + volume : stv - volume);

	tuple<double, double> pv_key = make_tuple(price, volume);

	// Delta bcv.b1

	// Metadata to track for delta vwap.
	// -- note this must support ordered iteration
	set<double> shifted_sv1;

	double cached_sv1 = 0.0;
	int cached_ref_count = 0;

	// Get iterator to p-, i.e. possibly (p,v+=) or (p+,v*)
	bcv_index::iterator sv1_lb = sv1.lower_bound(pv_key);
	if ( sv1_lb == sv1.end() && !sv1.empty() )
		--sv1_lb;

	#ifdef DEBUG
	cout << "sv1_lb: "
		<< (sv1_lb == sv1.begin())
		<< (sv1_lb == sv1.end())
		<< (sv1_lb == sv1.end()? -1.0 : get<0>(sv1_lb->first)) << endl;
	#endif

	// Advance iterator to (p-)+, i.e. last row of sv1-
	while ( get<0>(sv1_lb->first) >= price && sv1_lb != sv1.begin() )
	{

		// Cache sv1 for any (p,v*)
		if ( get<0>(sv1_lb->first) == price ) {
			cached_sv1 = get<0>(sv1_lb->second);
			cached_ref_count = get<1>(sv1_lb->second);
		}

		--sv1_lb;
	}

	if ( sv1_lb == sv1.begin() && get<0>(sv1_lb->first) >= price )
		sv1_lb = sv1.end();

	#ifdef DEBUG
	cout << "sv1_lb: "
		<< (sv1_lb == sv1.begin())
		<< (sv1_lb == sv1.end())
		<< (sv1_lb == sv1.end()? -1.0 : get<0>(sv1_lb->first)) << endl;

	cout << "Before insertions " << sv1.size() << endl;
	print_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif

	bcv_index new_sv1;
	for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
		double sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		#ifdef DEBUG
		cout << get<0>(sv1_lb->first)
			<< ", " << get<1>(sv1_lb->first) << ", "
			<< (insert? sv1_val+(ref_count*volume) : sv1_val-(ref_count*volume))
			<< ", " << ref_count << endl;
		#endif

		new_sv1[sv1_lb->first] = make_tuple(
			insert? sv1_val+(ref_count*volume) :
				sv1_val-(ref_count*volume),
			ref_count);

		shifted_sv1.insert(sv1_val);
	}

	if ( sv1_lb != sv1.end() ) {
		double final_sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		#ifdef DEBUG
		cout << get<0>(sv1_lb->first)
			<< ", " << get<1>(sv1_lb->first) << ", "
			<< (insert? final_sv1_val+(ref_count*volume) : final_sv1_val-(ref_count*volume))
			<< ", " << ref_count << endl;
		#endif

		new_sv1[sv1_lb->first] = make_tuple(
			insert? final_sv1_val + (ref_count*volume) :
				final_sv1_val - (ref_count*volume),
			ref_count);

		shifted_sv1.insert(final_sv1_val);
	}

	bcv_index::iterator nsv1_it, nsv1_end;
	nsv1_it = new_sv1.begin(); nsv1_end = new_sv1.end();
	for (; nsv1_it != nsv1_end; ++nsv1_it)
		sv1[nsv1_it->first] = nsv1_it->second;

	#ifdef DEBUG
	cout << "After insertions " << sv1.size() << endl;
	print_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif

	// Delta bcv.b2
	bcv_index::iterator found = sv1.find(pv_key);
	bool new_pv = found == sv1.end();
	bool new_max_pv = false;
	double delete_sv1 = 0.0;
	int delete_ref_count = 0;
	bool delete_pv = false;

	if ( insert ) {
		// Handle duplicates in b2 groups.
		if ( found != sv1.end() ) {
			int ref_count = get<1>(found->second);
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1_pv = get<0>(found->second) + sv1_per_ref;
			sv1[pv_key] = make_tuple(new_sv1_pv, ref_count + 1);
		}
		else {
			// Get iterator to (sv1+)-, i.e. next (p,v+) or first (p+,v*)
			bcv_index::iterator sv1_ub = sv1.upper_bound(pv_key);
			if ( sv1_ub != sv1.end() || (cached_sv1 > 0.0 && cached_ref_count > 0) )
			{
				double price_ub = get<0>(sv1_ub->first);
				double vol_ub = get<0>(sv1_ub->second);
				int ref_count_ub = get<1>(sv1_ub->second);
				if ( price_ub == price ) {
					sv1[pv_key] = make_tuple(vol_ub / ref_count_ub, 1);
				}
				else if (cached_sv1 > 0.0 && cached_ref_count > 0) {
					sv1[pv_key] = make_tuple(cached_sv1 / cached_ref_count, 1);
				}
				else {
					double total_p_vol = 0.0;
					while ( get<0>(sv1_ub->first) == price_ub &&
								(sv1_ub != sv1.end()))
					{
						int rc = get<1>(sv1_ub->second);
						total_p_vol += (rc * get<1>(sv1_ub->first));
						++sv1_ub;
					}

					sv1[pv_key] =
						make_tuple(vol_ub / ref_count_ub + total_p_vol, 1);
				}
			}
			else {
				if ( new_pv ) {
					sv1[pv_key] = make_tuple(0.0, 1);
					new_max_pv = true;
				}
			}
		}
	}
	else {
		assert ( found != sv1.end() );
		int ref_count = get<1>(found->second);
		if ( ref_count == 1 ) {
			// Delete group
			delete_sv1 = get<0>(found->second);
			delete_ref_count = 1;
			delete_pv = true;
			sv1.erase(pv_key);
		}
		else {
			// Decrement ref count
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1 = get<0>(found->second) - sv1_per_ref;
			sv1[pv_key] = make_tuple(new_sv1, --ref_count);
		}
	}

	#ifdef DEBUG
	cout << "Before vwap " << sv1.size() << endl;
	print_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif

	// Delta vwap.bcv

	double sv1_val = (delete_pv? delete_sv1 : get<0>(sv1[pv_key]));
	int sv1_ref_count = (delete_pv? delete_ref_count : get<1>(sv1[pv_key]));


	/*
	// Get iterator at sv1+, i.e. spv[sv1[(p-,v*)]]
	bcv_index::iterator sv1_plus = sv1.lower_bound(pv_key);
	if ( sv1_plus == sv1.end() && !sv1.empty() ) --sv1_plus;

	vwap_index::iterator spv_ub = spv.end();
	*/

	/*
	bcv_index::iterator shift_it = sv1_plus;
	double prev_price;
	double shift_sv1;
	double shift_vol = 0.0;
	int shift_rc;
	*/

	/*
	#ifdef DEBUG
	cout << "sv1_plus: "
		<< (sv1_plus == sv1.begin())
		<< (sv1_plus == sv1.end())
		<< (sv1_plus == sv1.end()? -1.0 : get<0>(sv1_plus->first)) << endl;
	#endif

	if ( !new_max_pv ) {
		while ( get<0>(sv1_plus->first) >= price && sv1_plus != sv1.begin() )
			--sv1_plus;

		if ( sv1_plus == sv1.begin() && get<0>(sv1_plus->first) >= price )
			sv1_plus = sv1.end();
	}

	#ifdef DEBUG
	cout << "sv1_plus: "
		<< (sv1_plus == sv1.begin())
		<< (sv1_plus == sv1.end())
		<< (sv1_plus == sv1.end()? -1.0 : get<0>(sv1_plus->first)) << endl;
	#endif
	*/

	/*
	if ( !new_max_pv ) {
		shift_rc = get<1>(shift_it->second);
		shift_it = sv1_plus;
		shift_sv1 = insert?
			get<0>(shift_it->second)-(shift_rc*volume) :
			get<0>(shift_it->second)+(shift_rc*volume);
		prev_price = get<0>(shift_it->first);
	}
	else {
		while ( get<0>(shift_it->first) >= price && shift_it != sv1.begin() )
			--shift_it;

		if ( shift_it == sv1.begin() && get<0>(shift_it->first) >= price )
			shift_it = sv1.end();
	}

	if ( shift_it != sv1.end() ) {
		shift_rc = get<1>(shift_it->second);
		shift_sv1 = insert?
			get<0>(shift_it->second)-(shift_rc*volume) :
			get<0>(shift_it->second)+(shift_rc*volume);
		prev_price = get<0>(shift_it->first);
	}

	#ifdef DEBUG
	cout << "pp: " << prev_price << endl;
	cout << "shift sv1: " << shift_sv1 << endl;

	cout << "shift_it: "
		<< (shift_it == sv1.begin())
		<< (shift_it == sv1.end())
		<< (shift_it == sv1.end()? -1.0 : get<0>(shift_it->first)) << endl;
	#endif
	*/

	/*
	if ( sv1_plus != sv1.end() )
	{
		int sv1_plus_rc = get<1>(sv1_plus->second);
		double spv_ub_val = insert?
			get<0>(sv1_plus->second)-(sv1_plus_rc*volume) :
			get<0>(sv1_plus->second)+(sv1_plus_rc*volume);

		spv_ub = spv.lower_bound(spv_ub_val);

		#ifdef DEBUG
		cout << "spv+: " << spv_ub_val << " "
			<< (spv_ub == spv.end()? -1.0 : spv_ub->first) << endl;
		#endif
	}
	*/

	// Shift sv1+ by sv1+volume, svp+(price*volume), cnt+1
	vwap_index new_spv;
	cvol_index new_cvc;
	list<vwap_index::iterator> old_sv1s;

	set<double>::iterator shift_it = shifted_sv1.begin();
	set<double>::iterator shift_end = shifted_sv1.end();

	#ifdef DEBUG
	cout << "Testing shift tracking " << endl;
	for (; shift_it != shift_end; ++shift_it)
		cout << (*shift_it) << endl;
	cout << " ----- " << endl;
	#endif

	shift_it = shifted_sv1.begin();
	for (; shift_it != shift_end; ++shift_it)
	{
		double old_sv1 = *shift_it;

		vwap_index::iterator spv_found = spv.find(old_sv1);
		assert ( spv_found != spv.end() );

		tuple<double, double>& old_sum_cnt = spv_found->second;

		cvol_index::iterator cvc_found = cvc.find(old_sv1);
		assert ( cvc_found != cvc.end() );

		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_found->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_found->second.end();

		for (; cvcp_it != cvcp_end; ++cvcp_it)
		{
			double cvcp_price = cvcp_it->first;

			if ( price > cvcp_price )
			{
				double shift_ref_count = get<1>(cvcp_it->second);

				#ifdef DEBUG
				cout << "RC: " << shift_ref_count << endl;

				cout << "Final shift rc: " << shift_ref_count
					<< ", " << old_sv1 << endl;
				#endif

				double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
					old_sv1 - (shift_ref_count*volume));
				double new_sum = (insert?
					get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
					get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
				int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
				new_spv[new_sv1] = make_tuple(new_sum, new_cnt);

				// Shift cvc simultaneously.
				// -- clean up eagerly here while we have new_cvcp
				new_cvc[new_sv1][cvcp_price] = cvcp_it->second;

				cvc_found->second.erase(cvcp_price);

				if ( cvc_found->second.empty() ) {
					old_sv1s.push_back(spv_found);
					cvc.erase(old_sv1);
				}
			}
		}
	}

	/*
	for (bool init_shift = true; spv_ub != spv.end(); ++spv_ub, init_shift = false)
	{
		double old_sv1 = spv_ub->first;
		tuple<double, double>& old_sum_cnt = spv_ub->second;

		cvol_index::iterator cvc_found = cvc.find(old_sv1);
		assert ( cvc_found != cvc.end() );

		double shift_ref_count = get<1>(cvc_found->second);

		**
		double shift_ref_count = 1.0;

		#ifdef DEBUG
		cout << "Match RC: " << shift_sv1 << " + " << shift_vol << " = " << old_sv1 << endl;
		#endif

		if ( fmod(old_sv1, (shift_sv1 + shift_vol)) == 0.0 ) {
			shift_ref_count = old_sv1 / (shift_sv1 + shift_vol);
			shift_sv1 += shift_vol;
			shift_vol = 0.0;
			while ( get<0>(shift_it->first) == prev_price && shift_it != sv1.end() ) {
				shift_vol += get<1>(shift_it->first);
				--shift_it;
				if ( shift_it == sv1.begin() ) break;
			}

			if ( shift_it != sv1.end() )
				prev_price = get<0>(shift_it->first);

			#ifdef DEBUG
			cout << "New pp " << prev_price << endl;
			cout << "New shift sv1, vol "
				<< shift_sv1 << ", " << shift_vol << endl;
			#endif

		}
		else if ( (shift_sv1 + shift_vol) > old_sv1 ) {
			shift_ref_count = old_sv1 / shift_sv1;
		}

		if ( init_shift )
			shift_ref_count = static_cast<double>(shift_rc);
		**

		#ifdef DEBUG
		cout << "RC: " << shift_ref_count << endl;

		cout << "Final shift rc: " << shift_ref_count
			<< ", " << old_sv1 << endl;
		#endif


		double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
			old_sv1 - (shift_ref_count*volume));
		double new_sum = (insert?
			get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
			get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
		int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
		new_spv[new_sv1] = make_tuple(new_sum, new_cnt);
		old_sv1s.push_back(spv_ub);

		// Shift cvc simultaneously.
		new_cvc[new_sv1] = cvc[old_sv1];
	}
	*/

	#ifdef DEBUG
	cout << "Deferred " << new_spv.size() << endl;
	print_vwap_index(new_spv);
	cout << " ----- CVC -----" << endl;
	print_cvol_index(new_cvc);
	cout << " ----- " << endl;
	#endif

	list<vwap_index::iterator>::iterator old_it = old_sv1s.begin();
	list<vwap_index::iterator>::iterator old_end = old_sv1s.end();
	for (; old_it != old_end; ++old_it) {
		spv.erase(*old_it);
	}

	#ifdef DEBUG
	cout << "After erase " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC -----" << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	// Check if sv1 exists.
	vwap_index::iterator sv1_found = spv.find(sv1_val);

	// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
	// -- note because of duplicates, we should get the iterator at the first (p+,v*)
	bcv_index::iterator sv1_minus = sv1.upper_bound(pv_key);

	#ifdef DEBUG
	cout << "sv1_minus: "
		<< (sv1_minus == sv1.begin())
		<< (sv1_minus == sv1.end())
		<< (sv1_minus == sv1.end()? -1.0 : get<0>(sv1_minus->first)) << endl;
	#endif

	while ( get<0>(sv1_minus->first) == price && sv1_minus != sv1.end() )
		++sv1_minus;

	#ifdef DEBUG
	cout << "sv1_minus: "
		<< (sv1_minus == sv1.begin())
		<< (sv1_minus == sv1.end())
		<< (sv1_minus == sv1.end()? -1.0 : get<0>(sv1_minus->first)) << endl;
	#endif

	//bcv_index::iterator price_plus = sv1.end();

	vwap_index::iterator spv_lb = spv.end();

	if ( sv1_minus != sv1.end() )
	{
		//price_plus = sv1_minus;
		spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

		#ifdef DEBUG
		cout << "sv1-: " << get<0>(sv1_minus->second) << endl;
		cout << "spv-: "
			<< (spv_lb == spv.end()? -1.0 : spv_lb->first) << endl;
		#endif
	}

	if ( insert )
	{
		if ( new_pv )
		{
			#ifdef DEBUG
			cout << "sv1_found: "
				<< (sv1_found == spv.begin())
				<< (sv1_found == spv.end())
				<< (sv1_found == spv.end()? -1.0 : sv1_found->first) << endl;
			#endif

			if ( sv1_found != spv.end() ) {
				tuple<double, double> sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(sum_cnt) + (price*volume), get<1>(sum_cnt)+1);

				#ifdef DEBUG
				cout << "Case I1 1 " << endl;
				#endif
			}
			else {
				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(price*volume, 1.0);

					#ifdef DEBUG
					cout << "Case I1 2 " << endl;
					#endif
				}
				else {
					tuple<double, double> sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(sum_cnt)+(price*volume), get<1>(sum_cnt)+1);

					#ifdef DEBUG
					cout << "Case I1 3 " << endl;
					#endif
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() ) {
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != 1 ) {
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected 1 (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == 1);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, 1);
				}
				else
					cvc[sv1_val][price] = make_tuple(1,1);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1,1);
			}
		}
		else
		{
			double old_ref_count = sv1_ref_count - 1;
			double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
			double old_spv = old_ref_count * (price * volume);

			if ( spv.find(old_sv1) == spv.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			// Check if we delete from spv
			assert ( spv.find(old_sv1) != spv.end() );
			tuple<double, double>& old_sum_cnt = spv[old_sv1];

			int check_count = 0;

			/*
			#ifdef DEBUG
			cout << "p+: "
				<< (price_plus == sv1.begin())
				<< (price_plus == sv1.end())
				<< (price_plus == sv1.end()? -1.0 : get<0>(price_plus->first)) << endl;
			#endif

			if ( price_plus != sv1.end() ) {
				DBT_HASH_SET<double> sv1_vals;
				int p_ref_count = 0;
				double upper_price = get<0>(price_plus->first);
				while ( (get<0>(price_plus->first) == upper_price) &&
						(price_plus != sv1.end()) )
				{
					p_ref_count += get<1>(price_plus->second);
					sv1_vals.insert(get<0>(price_plus->second));
					++price_plus;
				}

				int dup_ref_count = 0;
				DBT_HASH_SET<double>::iterator sv1_it = sv1_vals.begin();
				DBT_HASH_SET<double>::iterator sv1_end = sv1_vals.end();

				for (; sv1_it != sv1_end; ++sv1_it) {
					vwap_index::iterator spv_found = spv.find(*sv1_it);
					if ( spv_found != spv.end() )
						dup_ref_count += get<1>(spv_found->second);
					else
						cerr << "Failed to find spv[sv1] for " << (*sv1_it);
				}

				int pp_ref_count = (dup_ref_count - p_ref_count) / sv1_vals.size();
				check_count = pp_ref_count + p_ref_count + old_ref_count;

				#ifdef DEBUG
				cout << "pp_ref_count " << pp_ref_count << ", "
					<< "p_ref_count " << p_ref_count << ", "
					<< "sv1_ref_count " << old_ref_count << endl;
				#endif

			}
			else {
				check_count = 1;
			}
			*/

			cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
			if ( old_cvc_found == cvc.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				cerr << "Failed to find cvcp for "
					<< old_sv1 << ", " << price << endl;
				print_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			#ifdef DEBUG
			cout << "check_count " << check_count << ", " << get<1>(old_sum_cnt) << endl;
			#endif

			/*
			if ( get<1>(old_sum_cnt) == check_count ) {
				spv.erase(old_sv1);
				cvc.erase(old_sv1);
			}
			*/

			if ( check_count == 1 ) {
				cvc[old_sv1].erase(price);
				if ( cvc[old_sv1].empty() ) {
					spv.erase(old_sv1);
					cvc.erase(old_sv1);
				}
			}

			// Otherwise update sum_cnt at old_spv
			else {
				spv[old_sv1] =
					make_tuple(get<0>(old_sum_cnt) - old_spv,
						get<1>(old_sum_cnt) - old_ref_count);

				int existing_gc = get<0>(cvc[old_sv1][price]);
				int existing_rc = get<1>(cvc[old_sv1][price]);

				if ( existing_rc != old_ref_count ) {
					cerr << "Found inconsistent cvc rc "
						<< existing_rc << ", expected 1 (gc"
						<< existing_gc << ")" << endl;
					print_vwap_index(spv);
					cout << " ----- CVC ----- " << endl;
					print_cvol_index(cvc);
					cout << " ----- " << endl;
				}

				assert (existing_rc == old_ref_count);
				cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
			}

			// Update sum_cnt at new_spv
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);

				#ifdef DEBUG
				cout << "Case I2 1" << endl;
				#endif

			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] =
						make_tuple(sv1_ref_count*price*volume, sv1_ref_count);

					#ifdef DEBUG
					cout << "Case I2 2" << endl;
					#endif
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);

					#ifdef DEBUG
					cout << "Case I2 3" << endl;
					#endif

				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}

		}

		//cout << "After insert " << spv.size() << endl;
	}
	else
	{
		double old_ref_count = delete_pv? sv1_ref_count : sv1_ref_count+1;
		double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
		double old_spv = old_ref_count * (price*volume);

		if ( spv.find(old_sv1) == spv.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_bcv_index(sv1);
			print_vwap_index(spv);
		}

		// Check if we delete from spv
		assert ( spv.find(old_sv1) != spv.end() );
		tuple<double, double>& old_sum_cnt = spv[old_sv1];

		int check_count = 0;

		/*
		#ifdef DEBUG
		cout << "p+: "
			<< (price_plus == sv1.begin())
			<< (price_plus == sv1.end())
			<< (price_plus == sv1.end()? -1.0 : get<0>(price_plus->first)) << endl;
		#endif

		if ( price_plus != sv1.end() )
		{
			DBT_HASH_SET<double> sv1_vals;
			int p_ref_count = 0;
			double upper_price = get<0>(price_plus->first);
			while ( (get<0>(price_plus->first) == upper_price) &&
					(price_plus != sv1.end()) )
			{
				p_ref_count += get<1>(price_plus->second);
				sv1_vals.insert(get<0>(price_plus->second));
				++price_plus;
			}

			int dup_ref_count = 0;
			DBT_HASH_SET<double>::iterator sv1_it = sv1_vals.begin();
			DBT_HASH_SET<double>::iterator sv1_end = sv1_vals.end();

			for (; sv1_it != sv1_end; ++sv1_it) {
				vwap_index::iterator spv_found = spv.find(*sv1_it);
				if ( spv_found != spv.end() )
					dup_ref_count += get<1>(spv_found->second);
				else
					cerr << "Failed to find spv[sv1] for " << (*sv1_it);
			}

			int pp_ref_count = (dup_ref_count - p_ref_count) / sv1_vals.size();
			check_count = pp_ref_count + p_ref_count + old_ref_count;

			#ifdef DEBUG
			cout << "pp_ref_count " << pp_ref_count << ", "
				<< "p_ref_count " << p_ref_count << ", "
				<< "sv1_ref_count " << old_ref_count << endl;
			#endif

		}
		else {
			check_count = 1;
		}
		*/

		cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
		if ( old_cvc_found == cvc.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvc_found != cvc.end() );
		DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
			old_cvc_found->second.find(price);

		if ( old_cvcp_found == old_cvc_found->second.end() ) {
			cerr << "Failed to find cvcp for "
				<< old_sv1 << ", " << price << endl;
			print_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvcp_found != old_cvc_found->second.end() );
		check_count = get<0>(old_cvcp_found->second);

		#ifdef DEBUG
		cout << "check_count " << check_count << ", " << get<1>(old_sum_cnt) << endl;
		#endif

		if ( check_count == 1 ) {
			cvc[old_sv1].erase(price);
			if ( cvc[old_sv1].empty() ) {
				spv.erase(old_sv1);
				cvc.erase(old_sv1);
			}
		}

		// Otherwise update sum_cnt at old_spv
		else {
			spv[old_sv1] =
				make_tuple(get<0>(old_sum_cnt) - old_spv,
					get<1>(old_sum_cnt) - old_ref_count);

			int existing_gc = get<0>(cvc[old_sv1][price]);
			int existing_rc = get<1>(cvc[old_sv1][price]);

			if ( existing_rc != old_ref_count )
			{
				cerr << "Found inconsistent cvc rc "
					<< existing_rc << ", expected "
					<< old_ref_count << " (gc"
					<< existing_gc << ")" << endl;
				print_vwap_index(spv);
				cout << " ----- CVC ----- " << endl;
				print_cvol_index(cvc);
				cout << " ----- " << endl;
			}

			assert (existing_rc == old_ref_count);
			cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
		}

		// Update sum_cnt at new spv
		if ( !delete_pv ) {
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);
			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}
		}
	}

	#ifdef DEBUG
	cout << "Before merge " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	vwap_index::iterator nspv_it = new_spv.begin();
	vwap_index::iterator nspv_end = new_spv.end();

	for (; nspv_it != nspv_end; ++nspv_it)
		spv[nspv_it->first] = nspv_it->second;

	cvol_index::iterator ncvc_it = new_cvc.begin();
	cvol_index::iterator ncvc_end = new_cvc.end();

	for (; ncvc_it != ncvc_end; ++ncvc_it)
	{
		cvol_index::iterator cvc_found = cvc.find(ncvc_it->first);
		if ( cvc_found != cvc.end() )
		{
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_it = ncvc_it->second.begin();
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_end = ncvc_it->second.end();

			for (; ncvcp_it != ncvcp_end; ++ncvcp_it)
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(ncvcp_it->first);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count ) {
						cerr << "Found inconsistent cvc for sv1 "
							<< ncvc_it->first << " p " << ncvcp_it->first
							<< " rc "
							<< existing_rc << ", expected "
							<< get<1>(ncvcp_it->second) << " (gc "
							<< existing_gc << ")" << endl;

						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == get<1>(ncvcp_it->second));
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
				else {
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
			}
		}
		else
			cvc[ncvc_it->first] = ncvc_it->second;
	}

	#ifdef DEBUG
	cout << "After merge " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif


	// Delta vwap.bv
	vwap_index::iterator result_it = spv.upper_bound(stv);

	while ( get<0>(result_it->second) == stv && (result_it != spv.end()) )
		++result_it;

	double result = 0.0;
	if ( result_it != spv.end() ) {
		tuple<double, double> result_sum_cnt = result_it->second;
		result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
	}
	else {
		if ( !spv.empty() ) {
			vwap_index::iterator back = spv.begin();
			tuple<double, double> result_sum_cnt = back->second;
			result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
		}
	}

	#ifdef DEBUG
	cout << "At return " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	return result;
}

void validate_indexes(bcv_index& sv1, vwap_index& spv, cvol_index& cvc)
{
	/*
	vwap_index::iterator spv_it = spv.begin();
	vwap_index::iterator spv_end = spv.end();

	for (; spv_it != spv_end; ++spv_it) {
		bcv_index::iterator sv1_it = sv1.begin();
		bcv_index::iterator sv1_end = sv1.end();

		int ref_count = 0;
		for (; sv1_it != sv1_end; ++sv1_it) {
			if ( get<0>(sv1_it->second) == spv_it->first )
				ref_count += get<1>(sv1_it->second);
		}

		cvol_index::iterator cvc_found = cvc.find(spv_it->first);
		if ( ref_count == 0 ) {
			cerr << "No matching sv1 for " << spv_it->first << endl;
		}
		if ( cvc_found != cvc.end() && ref_count != cvc_found->second ) {
			cerr << "Incorrect gb ref count for " << spv_it->first
				<< ": " << cvc_found->second << " vs. " << ref_count << endl;
		}
		else if ( cvc_found == cvc.end() ) {
			cerr << "No matching cvc for " << spv_it->first << endl;
		}

		assert ( cvc_found != cvc.end() && ref_count == cvc_found->second );
	}
	*/

	bcv_index::iterator sv1_it = sv1.begin();
	bcv_index::iterator sv1_end = sv1.end();

	for ( ; sv1_it != sv1_end; ++sv1_it )
	{
		if ( get<0>(sv1_it->first) == 0 ) {
			cerr << "Found inconsistent price for "
				<< get<0>(sv1_it->second) << endl;
		}

		assert ( get<0>(sv1_it->first) > 0 );

		if ( get<1>(sv1_it->first) == 0 ) {
			cerr << "Found inconsistent volume for "
				<< get<0>(sv1_it->second) << endl;
		}

		assert ( get<1>(sv1_it->first) > 0 );

		vwap_index::iterator spv_found = spv.find(get<0>(sv1_it->second));
		if ( spv_found == spv.end() )
			cerr << "No matching spv for " << get<0>(sv1_it->second);

		assert ( spv_found != spv.end() );

		cvol_index::iterator cvc_found = cvc.find(get<0>(sv1_it->second));
		if ( cvc_found == cvc.end() )
			cerr << "No matching cvc for " << get<0>(sv1_it->second) << endl;

		assert ( cvc_found != cvc.end() );
	}

	cvol_index::iterator cvc_it = cvc.begin();
	cvol_index::iterator cvc_end = cvc.end();

	for (; cvc_it != cvc_end; ++cvc_it)
	{
		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_it->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_it->second.end();

		for (; cvcp_it != cvcp_end; ++cvcp_it)
		{
			if ( get<0>(cvcp_it->second) == 0 ) {
				cerr << "Found inconsistent cvcp for "
					<< cvc_it->first << " "
					<< cvcp_it->first << endl;
			}

			assert ( get<0>(cvcp_it->second) > 0 );
		}
	}
}


// price * volume -> sum(vol), count(*) as rc from bids where bids.price > price
typedef map<double, tuple<double, int> > price_bcv_index;

void debug_bcv_iterator(price_bcv_index::iterator it, price_bcv_index& bcv)
{
	cout << " sv1_lb "
		<< (it == bcv.begin()) << (it == bcv.end())
		<< (it == bcv.end()? -1.0 : it->first) << endl;
}

void print_price_bcv_index(price_bcv_index& bcv)
{
	price_bcv_index::iterator bcv_it = bcv.begin();
	price_bcv_index::iterator bcv_end = bcv.end();

	for (; bcv_it != bcv_end; ++bcv_it) {
		cout << bcv_it->first << ", "
			<< get<0>(bcv_it->second)
			<< ", " << get<1>(bcv_it->second) << endl;
	}
}

inline double update_vwap2(double price, double volume, double& stv,
	price_bcv_index& sv1, vwap_index& spv, cvol_index& cvc, bool insert = true)
{
	// Delta stv
	stv = (insert? stv + volume : stv - volume);

	// Delta bcv.b1

	// Metadata to track for delta vwap.
	// -- note this must support ordered iteration
	set<double> shifted_sv1;

	// Get iterator to p-, i.e. possibly (p,v+=) or (p+,v*)
	price_bcv_index::iterator sv1_lb = sv1.lower_bound(price);
	if ( sv1_lb == sv1.end() && !sv1.empty() )
		--sv1_lb;

	#ifdef DEBUG
	debug_bcv_iterator(sv1_lb, sv1);
	#endif

	// Advance iterator to (p-)+, i.e. last row of sv1-
	while ( sv1_lb->first >= price && sv1_lb != sv1.begin() )
		--sv1_lb;

	if ( sv1_lb == sv1.begin() && sv1_lb->first >= price )
		sv1_lb = sv1.end();

	#ifdef DEBUG
	debug_bcv_iterator(sv1_lb, sv1);

	cout << "Before insertions " << sv1.size() << endl;
	print_price_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif


	price_bcv_index new_sv1;
	for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
		double sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		#ifdef DEBUG
		cout << sv1_lb->first << ", "
			<< (insert? sv1_val+(ref_count*volume) : sv1_val-(ref_count*volume))
			<< ", " << ref_count << endl;
		#endif

		new_sv1[sv1_lb->first] = make_tuple(
			insert? sv1_val+(ref_count*volume) :
				sv1_val-(ref_count*volume),
			ref_count);

		shifted_sv1.insert(sv1_val);
	}

	if ( sv1_lb != sv1.end() )
	{
		double final_sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		#ifdef DEBUG
		cout << sv1_lb->first << ", "
			<< (insert? final_sv1_val+(ref_count*volume) : final_sv1_val-(ref_count*volume))
			<< ", " << ref_count << endl;
		#endif

		new_sv1[sv1_lb->first] = make_tuple(
			insert? final_sv1_val + (ref_count*volume) :
				final_sv1_val - (ref_count*volume),
			ref_count);

		shifted_sv1.insert(final_sv1_val);
	}

	price_bcv_index::iterator nsv1_it, nsv1_end;
	nsv1_it = new_sv1.begin(); nsv1_end = new_sv1.end();
	for (; nsv1_it != nsv1_end; ++nsv1_it)
		sv1[nsv1_it->first] = nsv1_it->second;

	#ifdef DEBUG
	cout << "After insertions " << sv1.size() << endl;
	print_price_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif

	// Delta bcv.b2
	price_bcv_index::iterator found = sv1.find(price);
	bool new_p = found == sv1.end();
	bool new_max_p = false;
	double delete_sv1 = 0.0;
	int delete_ref_count = 0;
	bool delete_p = false;

	if ( insert )
	{
		// Handle duplicates in b2 groups.
		if ( found != sv1.end() ) {
			int ref_count = get<1>(found->second);
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1 = get<0>(found->second) + sv1_per_ref;
			sv1[price] = make_tuple(new_sv1, ref_count + 1);
		}
		else {
			// Get iterator to (sv1+)-, i.e. next (p,v+) or first (p+,v*)
			price_bcv_index::iterator sv1_ub = sv1.upper_bound(price);
			if ( sv1_ub != sv1.end() )
			{
				double price_ub = sv1_ub->first;
				double sv1_val_ub = get<0>(sv1_ub->second);
				int ref_count_ub = get<1>(sv1_ub->second);
				sv1[price] = make_tuple(sv1_val_ub, 1);
			}
			else {
				sv1[price] = make_tuple(0.0, 1);
				new_max_p = true;
			}
		}
	}
	else {
		assert ( found != sv1.end() );
		int ref_count = get<1>(found->second);
		if ( ref_count == 1 ) {
			// Delete group
			delete_sv1 = get<0>(found->second);
			delete_ref_count = 1;
			delete_p = true;
			sv1.erase(price);
		}
		else {
			// Decrement ref count
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1 = get<0>(found->second) - sv1_per_ref;
			sv1[price] = make_tuple(new_sv1, --ref_count);
		}
	}

	#ifdef DEBUG
	cout << "Before vwap " << sv1.size() << endl;
	print_price_bcv_index(sv1);
	cout << " ----- " << endl;
	#endif


	// Delta vwap.bcv
	double sv1_val = (delete_p? delete_sv1 : get<0>(sv1[price]));
	int sv1_ref_count = (delete_p? delete_ref_count : get<1>(sv1[price]));

	// Shift sv1+ by sv1+volume, svp+(price*volume), cnt+1
	vwap_index new_spv;
	cvol_index new_cvc;
	set<double> old_sv1s;

	set<double>::iterator shift_it = shifted_sv1.begin();
	set<double>::iterator shift_end = shifted_sv1.end();

	#ifdef DEBUG
	cout << "Testing shift tracking " << endl;
	for (; shift_it != shift_end; ++shift_it)
		cout << (*shift_it) << endl;
	cout << " ----- " << endl;
	#endif

	shift_it = shifted_sv1.begin();
	for (; shift_it != shift_end; ++shift_it)
	{
		double old_sv1 = *shift_it;

		vwap_index::iterator spv_found = spv.find(old_sv1);
		assert ( spv_found != spv.end() );

		tuple<double, double>& old_sum_cnt = spv_found->second;

		cvol_index::iterator cvc_found = cvc.find(old_sv1);
		assert ( cvc_found != cvc.end() );

		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_found->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_found->second.end();

		DBT_HASH_SET<double> cvcp_deletions;

		for (; cvcp_it != cvcp_end; ++cvcp_it)
		{
			double cvcp_price = cvcp_it->first;

			if ( price > cvcp_price )
			{
				double shift_ref_count = get<1>(cvcp_it->second);

				#ifdef DEBUG
				cout << "RC: " << shift_ref_count << endl;

				cout << "Final shift rc: " << shift_ref_count
					<< ", " << old_sv1 << endl;
				#endif

				double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
					old_sv1 - (shift_ref_count*volume));
				double new_sum = (insert?
					get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
					get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
				int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
				new_spv[new_sv1] = make_tuple(new_sum, new_cnt);

				// Shift cvc simultaneously.
				// -- clean up eagerly here while we have new_cvcp
				new_cvc[new_sv1][cvcp_price] = cvcp_it->second;
				cvcp_deletions.insert(cvcp_price);

			}
		}

		DBT_HASH_SET<double>::iterator del_it = cvcp_deletions.begin();
		DBT_HASH_SET<double>::iterator del_end = cvcp_deletions.end();

		for (; del_it != del_end; ++del_it)
		{
			cvc_found->second.erase(*del_it);
			if ( cvc_found->second.empty() ) {
				old_sv1s.insert(spv_found->first);
			}
		}
	}

	#ifdef DEBUG
	cout << "Deferred " << new_spv.size() << endl;
	print_vwap_index(new_spv);
	cout << " ----- CVC -----" << endl;
	print_cvol_index(new_cvc);
	cout << " ----- " << endl;
	#endif

	set<double>::iterator old_it = old_sv1s.begin();
	set<double>::iterator old_end = old_sv1s.end();
	for (; old_it != old_end; ++old_it) {
		spv.erase(*old_it);
		cvc.erase(*old_it);
	}

	#ifdef DEBUG
	cout << "After erase " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC -----" << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	// Check if sv1 exists.
	vwap_index::iterator sv1_found = spv.find(sv1_val);

	// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
	// -- note because of duplicates, we should get the iterator at the first (p+,v*)
	price_bcv_index::iterator sv1_minus = sv1.upper_bound(price);

	#ifdef DEBUG
	cout << "sv1_minus: "
		<< (sv1_minus == sv1.begin())
		<< (sv1_minus == sv1.end())
		<< (sv1_minus == sv1.end()? -1.0 : sv1_minus->first) << endl;
	#endif

	while ( sv1_minus->first == price && sv1_minus != sv1.end() )
		++sv1_minus;

	#ifdef DEBUG
	cout << "sv1_minus: "
		<< (sv1_minus == sv1.begin())
		<< (sv1_minus == sv1.end())
		<< (sv1_minus == sv1.end()? -1.0 : sv1_minus->first) << endl;
	#endif

	vwap_index::iterator spv_lb = spv.end();

	if ( sv1_minus != sv1.end() )
	{
		spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

		#ifdef DEBUG
		cout << "sv1-: " << get<0>(sv1_minus->second) << endl;
		cout << "spv-: "
			<< (spv_lb == spv.end()? -1.0 : spv_lb->first) << endl;
		#endif
	}

	if ( insert )
	{
		if ( new_p )
		{
			#ifdef DEBUG
			cout << "sv1_found: "
				<< (sv1_found == spv.begin())
				<< (sv1_found == spv.end())
				<< (sv1_found == spv.end()? -1.0 : sv1_found->first) << endl;
			#endif

			if ( sv1_found != spv.end() ) {
				tuple<double, double> sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(sum_cnt) + (price*volume), get<1>(sum_cnt)+1);
			}
			else {
				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(price*volume, 1.0);
				}
				else
				{
					tuple<double, double> sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(sum_cnt)+(price*volume), get<1>(sum_cnt)+1);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != 1 ) {
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected 1 (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == 1);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, 1);
				}
				else
					cvc[sv1_val][price] = make_tuple(1,1);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1,1);
			}
		}
		else
		{
			double old_ref_count = sv1_ref_count - 1;
			double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
			double old_spv = old_ref_count * (price * volume);

			if ( spv.find(old_sv1) == spv.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			// Check if we delete from spv
			assert ( spv.find(old_sv1) != spv.end() );
			tuple<double, double>& old_sum_cnt = spv[old_sv1];

			int check_count = 0;

			cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
			if ( old_cvc_found == cvc.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				cerr << "Failed to find cvcp for "
					<< old_sv1 << ", " << price << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			#ifdef DEBUG
			cout << "check_count " << check_count
				<< ", " << get<1>(old_sum_cnt) << endl;
			#endif

			if ( check_count == 1 ) {
				cvc[old_sv1].erase(price);
				if ( cvc[old_sv1].empty() ) {
					spv.erase(old_sv1);
					cvc.erase(old_sv1);
				}
			}

			// Otherwise update sum_cnt at old_spv
			else {
				spv[old_sv1] =
					make_tuple(get<0>(old_sum_cnt) - old_spv,
						get<1>(old_sum_cnt) - old_ref_count);

				int existing_gc = get<0>(cvc[old_sv1][price]);
				int existing_rc = get<1>(cvc[old_sv1][price]);

				if ( existing_rc != old_ref_count ) {
					cerr << "Found inconsistent cvc rc "
						<< existing_rc << ", expected 1 (gc"
						<< existing_gc << ")" << endl;
					print_vwap_index(spv);
					cout << " ----- CVC ----- " << endl;
					print_cvol_index(cvc);
					cout << " ----- " << endl;
				}

				assert (existing_rc == old_ref_count);
				cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
			}

			// Update sum_cnt at new_spv
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);
			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] =
						make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}

		}
	}
	else
	{
		double old_ref_count = delete_p? sv1_ref_count : sv1_ref_count+1;
		double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
		double old_spv = old_ref_count * (price*volume);

		if ( spv.find(old_sv1) == spv.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
		}

		// Check if we delete from spv
		assert ( spv.find(old_sv1) != spv.end() );
		tuple<double, double>& old_sum_cnt = spv[old_sv1];

		int check_count = 0;

		cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
		if ( old_cvc_found == cvc.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvc_found != cvc.end() );
		DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
			old_cvc_found->second.find(price);

		if ( old_cvcp_found == old_cvc_found->second.end() ) {
			cerr << "Failed to find cvcp for "
				<< old_sv1 << ", " << price << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvcp_found != old_cvc_found->second.end() );
		check_count = get<0>(old_cvcp_found->second);

		#ifdef DEBUG
		cout << "check_count "
			<< check_count << ", " << get<1>(old_sum_cnt) << endl;
		#endif

		if ( check_count == 1 ) {
			cvc[old_sv1].erase(price);
			if ( cvc[old_sv1].empty() ) {
				spv.erase(old_sv1);
				cvc.erase(old_sv1);
			}
		}

		// Otherwise update sum_cnt at old_spv
		else {
			spv[old_sv1] =
				make_tuple(get<0>(old_sum_cnt) - old_spv,
					get<1>(old_sum_cnt) - old_ref_count);

			int existing_gc = get<0>(cvc[old_sv1][price]);
			int existing_rc = get<1>(cvc[old_sv1][price]);

			if ( existing_rc != old_ref_count )
			{
				cerr << "Found inconsistent cvc rc "
					<< existing_rc << ", expected "
					<< old_ref_count << " (gc"
					<< existing_gc << ")" << endl;
				print_vwap_index(spv);
				cout << " ----- CVC ----- " << endl;
				print_cvol_index(cvc);
				cout << " ----- " << endl;
			}

			assert (existing_rc == old_ref_count);
			cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
		}

		// Update sum_cnt at new spv
		if ( !delete_p )
		{
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);
			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}
		}
	}

	#ifdef DEBUG
	cout << "Before merge " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	vwap_index::iterator nspv_it = new_spv.begin();
	vwap_index::iterator nspv_end = new_spv.end();

	for (; nspv_it != nspv_end; ++nspv_it)
		spv[nspv_it->first] = nspv_it->second;

	cvol_index::iterator ncvc_it = new_cvc.begin();
	cvol_index::iterator ncvc_end = new_cvc.end();

	for (; ncvc_it != ncvc_end; ++ncvc_it)
	{
		cvol_index::iterator cvc_found = cvc.find(ncvc_it->first);
		if ( cvc_found != cvc.end() )
		{
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_it = ncvc_it->second.begin();
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_end = ncvc_it->second.end();

			for (; ncvcp_it != ncvcp_end; ++ncvcp_it)
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(ncvcp_it->first);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count ) {
						cerr << "Found inconsistent cvc for sv1 "
							<< ncvc_it->first << " p " << ncvcp_it->first
							<< " rc "
							<< existing_rc << ", expected "
							<< get<1>(ncvcp_it->second) << " (gc "
							<< existing_gc << ")" << endl;

						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == get<1>(ncvcp_it->second));
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
				else {
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
			}
		}
		else
			cvc[ncvc_it->first] = ncvc_it->second;
	}

	#ifdef DEBUG
	cout << "After merge " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif


	// Delta vwap.bv
	vwap_index::iterator result_it = spv.upper_bound(stv);

	while ( get<0>(result_it->second) == stv && (result_it != spv.end()) )
		++result_it;

	double result = 0.0;
	if ( result_it != spv.end() ) {
		tuple<double, double> result_sum_cnt = result_it->second;
		result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
	}
	else {
		if ( !spv.empty() ) {
			vwap_index::iterator back = spv.begin();
			tuple<double, double> result_sum_cnt = back->second;
			result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
		}
	}

	#ifdef DEBUG
	cout << "At return " << spv.size() << endl;
	print_vwap_index(spv);
	cout << " ----- CVC ----- " << endl;
	print_cvol_index(cvc);
	cout << " ----- " << endl;
	#endif

	return result;
}


///////////////////////
//
// Bulk operations

list<unsigned long> bulk_executions;
unsigned long bulk5_counter;

typedef map<tuple<double, double>, tuple<bool, bool, double, int, bool> > b2_state;

double bulk_update_vwap_varying(
	list<tuple<double, double> >& price_vols,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc, bool insert = true)
{
	// Collect bulk exec stats
	bulk_executions.push_back(price_vols.size());

	list< tuple<double, double> >::iterator pv_it = price_vols.begin();
	list< tuple<double, double> >::iterator pv_end = price_vols.end();

	map< tuple<double, double>, set<double> > shifted_sv1s;

	// Batch delta stv, B1
	for (; pv_it != pv_end; ++pv_it)
	{
		double price = get<0>(*pv_it);
		double volume = get<1>(*pv_it);

		set<double>& shifted_sv1 = shifted_sv1s[*pv_it];

		// Delta stv
		stv = (insert? stv + volume : stv - volume);

		// Delta bcv.b1

		// Get iterator to p-, i.e. possibly (p,v+=) or (p+,v*)
		price_bcv_index::iterator sv1_lb = sv1.lower_bound(price);
		if ( sv1_lb == sv1.end() && !sv1.empty() )
			--sv1_lb;

		if ( sv1_lb == sv1.begin() && sv1_lb->first >= price )
			sv1_lb = sv1.end();

		price_bcv_index new_sv1;
		for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
			double sv1_val = get<0>(sv1_lb->second);
			int ref_count = get<1>(sv1_lb->second);

			new_sv1[sv1_lb->first] = make_tuple(
				insert? sv1_val+(ref_count*volume) :
					sv1_val-(ref_count*volume),
				ref_count);

			shifted_sv1.insert(sv1_val);
		}

		if ( sv1_lb != sv1.end() )
		{
			double final_sv1_val = get<0>(sv1_lb->second);
			int ref_count = get<1>(sv1_lb->second);

			new_sv1[sv1_lb->first] = make_tuple(
				insert? final_sv1_val + (ref_count*volume) :
					final_sv1_val - (ref_count*volume),
				ref_count);

			shifted_sv1.insert(final_sv1_val);
		}

		price_bcv_index::iterator nsv1_it, nsv1_end;
		nsv1_it = new_sv1.begin(); nsv1_end = new_sv1.end();
		for (; nsv1_it != nsv1_end; ++nsv1_it)
			sv1[nsv1_it->first] = nsv1_it->second;
	}

	// Batch delta B2
	pv_it = price_vols.begin();
	b2_state b2_st;

	for (; pv_it != pv_end; ++pv_it)
	{
		double price = get<0>(*pv_it);
		double volume = get<1>(*pv_it);

		price_bcv_index::iterator found = sv1.find(price);

		bool new_p = found == sv1.end();
		bool new_max_p = false;
		double delete_sv1 = 0.0;
		int delete_ref_count = 0;
		bool delete_p = false;

		if ( insert )
		{
			// Handle duplicates in b2 groups.
			if ( found != sv1.end() ) {
				int ref_count = get<1>(found->second);
				double sv1_per_ref = get<0>(found->second) / ref_count;
				double new_sv1 = get<0>(found->second) + sv1_per_ref;
				sv1[price] = make_tuple(new_sv1, ref_count + 1);
			}
			else {
				// Get iterator to (sv1+)-, i.e. next (p,v+) or first (p+,v*)
				price_bcv_index::iterator sv1_ub = sv1.upper_bound(price);
				if ( sv1_ub != sv1.end() )
				{
					double price_ub = sv1_ub->first;
					double sv1_val_ub = get<0>(sv1_ub->second);
					int ref_count_ub = get<1>(sv1_ub->second);
					sv1[price] = make_tuple(sv1_val_ub, 1);
				}
				else {
					sv1[price] = make_tuple(0.0, 1);
					new_max_p = true;
				}
			}
		}
		else {
			assert ( found != sv1.end() );
			int ref_count = get<1>(found->second);
			if ( ref_count == 1 ) {
				// Delete group
				delete_sv1 = get<0>(found->second);
				delete_ref_count = 1;
				delete_p = true;
				sv1.erase(price);
			}
			else {
				// Decrement ref count
				double sv1_per_ref = get<0>(found->second) / ref_count;
				double new_sv1 = get<0>(found->second) - sv1_per_ref;
				sv1[price] = make_tuple(new_sv1, --ref_count);
			}
		}

		b2_st[*pv_it] =
			make_tuple(new_p, new_max_p, delete_sv1, delete_ref_count, delete_p);
	}

	// Batch delta vwap.bcv
	pv_it = price_vols.begin();

	for (; pv_it != pv_end; ++pv_it)
	{
		double price = get<0>(*pv_it);
		double volume = get<1>(*pv_it);

		tuple<bool, bool, double, int, bool>& pv_b2_st = b2_st[*pv_it];

		set<double>& shifted_sv1 = shifted_sv1s[*pv_it];

		bool new_p = get<0>(pv_b2_st);
		bool new_max_p = get<1>(pv_b2_st);
		double delete_sv1 = get<2>(pv_b2_st);
		int delete_ref_count = get<3>(pv_b2_st);
		bool delete_p = get<4>(pv_b2_st);

		// Delta vwap.bcv
		double sv1_val = (delete_p? delete_sv1 : get<0>(sv1[price]));
		int sv1_ref_count = (delete_p? delete_ref_count : get<1>(sv1[price]));

		// Shift sv1+ by sv1+volume, svp+(price*volume), cnt+1
		vwap_index new_spv;
		cvol_index new_cvc;
		set<double> old_sv1s;

		set<double>::iterator shift_it = shifted_sv1.begin();
		set<double>::iterator shift_end = shifted_sv1.end();

		shift_it = shifted_sv1.begin();
		for (; shift_it != shift_end; ++shift_it)
		{
			double old_sv1 = *shift_it;

			vwap_index::iterator spv_found = spv.find(old_sv1);
			assert ( spv_found != spv.end() );

			tuple<double, double>& old_sum_cnt = spv_found->second;

			cvol_index::iterator cvc_found = cvc.find(old_sv1);
			assert ( cvc_found != cvc.end() );

			DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_found->second.begin();
			DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_found->second.end();

			DBT_HASH_SET<double> cvcp_deletions;

			for (; cvcp_it != cvcp_end; ++cvcp_it)
			{
				double cvcp_price = cvcp_it->first;

				if ( price > cvcp_price )
				{
					double shift_ref_count = get<1>(cvcp_it->second);

					double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
						old_sv1 - (shift_ref_count*volume));
					double new_sum = (insert?
						get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
						get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
					int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
					new_spv[new_sv1] = make_tuple(new_sum, new_cnt);

					// Shift cvc simultaneously.
					// -- clean up eagerly here while we have new_cvcp
					new_cvc[new_sv1][cvcp_price] = cvcp_it->second;
					cvcp_deletions.insert(cvcp_price);

				}
			}

			DBT_HASH_SET<double>::iterator del_it = cvcp_deletions.begin();
			DBT_HASH_SET<double>::iterator del_end = cvcp_deletions.end();

			for (; del_it != del_end; ++del_it)
			{
				cvc_found->second.erase(*del_it);
				if ( cvc_found->second.empty() ) {
					old_sv1s.insert(spv_found->first);
				}
			}
		}

		set<double>::iterator old_it = old_sv1s.begin();
		set<double>::iterator old_end = old_sv1s.end();
		for (; old_it != old_end; ++old_it) {
			spv.erase(*old_it);
			cvc.erase(*old_it);
		}

		// Check if sv1 exists.
		vwap_index::iterator sv1_found = spv.find(sv1_val);

		// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
		// -- note because of duplicates, we should get the iterator at the first (p+,v*)
		price_bcv_index::iterator sv1_minus = sv1.upper_bound(price);

		while ( sv1_minus->first == price && sv1_minus != sv1.end() )
			++sv1_minus;

		vwap_index::iterator spv_lb = spv.end();

		if ( sv1_minus != sv1.end() )
			spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

		if ( insert )
		{
			if ( new_p )
			{
				if ( sv1_found != spv.end() ) {
					tuple<double, double> sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(sum_cnt) + (price*volume), get<1>(sum_cnt)+1);
				}
				else {
					if ( spv_lb == spv.end() ) {
						spv[sv1_val] = make_tuple(price*volume, 1.0);
					}
					else
					{
						tuple<double, double> sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(sum_cnt)+(price*volume), get<1>(sum_cnt)+1);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != 1 ) {
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected 1 (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == 1);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, 1);
					}
					else
						cvc[sv1_val][price] = make_tuple(1,1);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1,1);
				}
			}
			else
			{
				double old_ref_count = sv1_ref_count - 1;
				double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
				double old_spv = old_ref_count * (price * volume);

				if ( spv.find(old_sv1) == spv.end() ) {
					cerr << "Failed to find spv for " << old_sv1 << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				// Check if we delete from spv
				assert ( spv.find(old_sv1) != spv.end() );
				tuple<double, double>& old_sum_cnt = spv[old_sv1];

				int check_count = 0;

				cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
				if ( old_cvc_found == cvc.end() ) {
					cerr << "Failed to find spv for " << old_sv1 << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				assert ( old_cvc_found != cvc.end() );
				DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
					old_cvc_found->second.find(price);

				if ( old_cvcp_found == old_cvc_found->second.end() ) {
					cerr << "Failed to find cvcp for "
						<< old_sv1 << ", " << price << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				assert ( old_cvcp_found != old_cvc_found->second.end() );
				check_count = get<0>(old_cvcp_found->second);

				if ( check_count == 1 ) {
					cvc[old_sv1].erase(price);
					if ( cvc[old_sv1].empty() ) {
						spv.erase(old_sv1);
						cvc.erase(old_sv1);
					}
				}

				// Otherwise update sum_cnt at old_spv
				else {
					spv[old_sv1] =
						make_tuple(get<0>(old_sum_cnt) - old_spv,
							get<1>(old_sum_cnt) - old_ref_count);

					int existing_gc = get<0>(cvc[old_sv1][price]);
					int existing_rc = get<1>(cvc[old_sv1][price]);

					if ( existing_rc != old_ref_count ) {
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected 1 (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == old_ref_count);
					cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
				}

				// Update sum_cnt at new_spv
				if ( sv1_found != spv.end() ) {
					tuple<double, double> new_sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				} else {

					if ( spv_lb == spv.end() ) {
						spv[sv1_val] =
							make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
					}
					else {
						tuple<double, double> new_sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
								get<1>(new_sum_cnt)+sv1_ref_count);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count )
						{
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected "
								<< sv1_ref_count << " (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == sv1_ref_count);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
					}
					else
						cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
				}

			}
		}
		else
		{
			double old_ref_count = delete_p? sv1_ref_count : sv1_ref_count+1;
			double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
			double old_spv = old_ref_count * (price*volume);

			if ( spv.find(old_sv1) == spv.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
			}

			// Check if we delete from spv
			assert ( spv.find(old_sv1) != spv.end() );
			tuple<double, double>& old_sum_cnt = spv[old_sv1];

			int check_count = 0;

			cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
			if ( old_cvc_found == cvc.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				cerr << "Failed to find cvcp for "
					<< old_sv1 << ", " << price << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			if ( check_count == 1 ) {
				cvc[old_sv1].erase(price);
				if ( cvc[old_sv1].empty() ) {
					spv.erase(old_sv1);
					cvc.erase(old_sv1);
				}
			}

			// Otherwise update sum_cnt at old_spv
			else {
				spv[old_sv1] =
					make_tuple(get<0>(old_sum_cnt) - old_spv,
						get<1>(old_sum_cnt) - old_ref_count);

				int existing_gc = get<0>(cvc[old_sv1][price]);
				int existing_rc = get<1>(cvc[old_sv1][price]);

				if ( existing_rc != old_ref_count )
				{
					cerr << "Found inconsistent cvc rc "
						<< existing_rc << ", expected "
						<< old_ref_count << " (gc"
						<< existing_gc << ")" << endl;
					print_vwap_index(spv);
					cout << " ----- CVC ----- " << endl;
					print_cvol_index(cvc);
					cout << " ----- " << endl;
				}

				assert (existing_rc == old_ref_count);
				cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
			}

			// Update sum_cnt at new spv
			if ( !delete_p )
			{
				if ( sv1_found != spv.end() ) {
					tuple<double, double> new_sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				} else {

					if ( spv_lb == spv.end() ) {
						spv[sv1_val] = make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
					}
					else {
						tuple<double, double> new_sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
								get<1>(new_sum_cnt)+sv1_ref_count);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count )
						{
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected "
								<< sv1_ref_count << " (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == sv1_ref_count);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
					}
					else
						cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
				}
			}
		}

		vwap_index::iterator nspv_it = new_spv.begin();
		vwap_index::iterator nspv_end = new_spv.end();

		for (; nspv_it != nspv_end; ++nspv_it)
			spv[nspv_it->first] = nspv_it->second;

		cvol_index::iterator ncvc_it = new_cvc.begin();
		cvol_index::iterator ncvc_end = new_cvc.end();

		for (; ncvc_it != ncvc_end; ++ncvc_it)
		{
			cvol_index::iterator cvc_found = cvc.find(ncvc_it->first);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_it = ncvc_it->second.begin();
				DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_end = ncvc_it->second.end();

				for (; ncvcp_it != ncvcp_end; ++ncvcp_it)
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(ncvcp_it->first);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count ) {
							cerr << "Found inconsistent cvc for sv1 "
								<< ncvc_it->first << " p " << ncvcp_it->first
								<< " rc "
								<< existing_rc << ", expected "
								<< get<1>(ncvcp_it->second) << " (gc "
								<< existing_gc << ")" << endl;

							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == get<1>(ncvcp_it->second));
						cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
					}
					else {
						cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
					}
				}
			}
			else
				cvc[ncvc_it->first] = ncvc_it->second;
		}
	}

	// Delta vwap.bv
	vwap_index::iterator result_it = spv.upper_bound(stv);

	while ( get<0>(result_it->second) == stv && (result_it != spv.end()) )
		++result_it;

	double result = 0.0;
	if ( result_it != spv.end() ) {
		tuple<double, double> result_sum_cnt = result_it->second;
		result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
	}
	else {
		if ( !spv.empty() ) {
			vwap_index::iterator back = spv.begin();
			tuple<double, double> result_sum_cnt = back->second;
			result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
		}
	}

	return result;
}


double bulk_update_vwap_varying2(
	list<tuple<double, double> >& price_vols,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc, bool insert = true)
{
	// Collect bulk exec stats
	bulk_executions.push_back(price_vols.size());

	list< tuple<double, double> >::iterator pv_it = price_vols.begin();
	list< tuple<double, double> >::iterator pv_end = price_vols.end();

	double result = 0.0;

	// Batch delta stv, B1
	for (; pv_it != pv_end; ++pv_it)
	{
		double price = get<0>(*pv_it);
		double volume = get<1>(*pv_it);

		// Delta stv
		stv = (insert? stv + volume : stv - volume);

		// Delta bcv.b1

		// Metadata to track for delta vwap.
		// -- note this must support ordered iteration
		set<double> shifted_sv1;

		// Get iterator to p-, i.e. possibly (p,v+=) or (p+,v*)
		price_bcv_index::iterator sv1_lb = sv1.lower_bound(price);
		if ( sv1_lb == sv1.end() && !sv1.empty() )
			--sv1_lb;

		#ifdef DEBUG
		debug_bcv_iterator(sv1_lb, sv1);
		#endif

		// Advance iterator to (p-)+, i.e. last row of sv1-
		while ( sv1_lb->first >= price && sv1_lb != sv1.begin() )
			--sv1_lb;

		if ( sv1_lb == sv1.begin() && sv1_lb->first >= price )
			sv1_lb = sv1.end();

		#ifdef DEBUG
		debug_bcv_iterator(sv1_lb, sv1);

		cout << "Before insertions " << sv1.size() << endl;
		print_price_bcv_index(sv1);
		cout << " ----- " << endl;
		#endif


		price_bcv_index new_sv1;
		for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
			double sv1_val = get<0>(sv1_lb->second);
			int ref_count = get<1>(sv1_lb->second);

			#ifdef DEBUG
			cout << sv1_lb->first << ", "
				<< (insert? sv1_val+(ref_count*volume) : sv1_val-(ref_count*volume))
				<< ", " << ref_count << endl;
			#endif

			new_sv1[sv1_lb->first] = make_tuple(
				insert? sv1_val+(ref_count*volume) :
					sv1_val-(ref_count*volume),
				ref_count);

			shifted_sv1.insert(sv1_val);
		}

		if ( sv1_lb != sv1.end() )
		{
			double final_sv1_val = get<0>(sv1_lb->second);
			int ref_count = get<1>(sv1_lb->second);

			#ifdef DEBUG
			cout << sv1_lb->first << ", "
				<< (insert? final_sv1_val+(ref_count*volume) : final_sv1_val-(ref_count*volume))
				<< ", " << ref_count << endl;
			#endif

			new_sv1[sv1_lb->first] = make_tuple(
				insert? final_sv1_val + (ref_count*volume) :
					final_sv1_val - (ref_count*volume),
				ref_count);

			shifted_sv1.insert(final_sv1_val);
		}

		price_bcv_index::iterator nsv1_it, nsv1_end;
		nsv1_it = new_sv1.begin(); nsv1_end = new_sv1.end();
		for (; nsv1_it != nsv1_end; ++nsv1_it)
			sv1[nsv1_it->first] = nsv1_it->second;

		#ifdef DEBUG
		cout << "After insertions " << sv1.size() << endl;
		print_price_bcv_index(sv1);
		cout << " ----- " << endl;
		#endif

		// Delta bcv.b2
		price_bcv_index::iterator found = sv1.find(price);
		bool new_p = found == sv1.end();
		bool new_max_p = false;
		double delete_sv1 = 0.0;
		int delete_ref_count = 0;
		bool delete_p = false;

		if ( insert )
		{
			// Handle duplicates in b2 groups.
			if ( found != sv1.end() ) {
				int ref_count = get<1>(found->second);
				double sv1_per_ref = get<0>(found->second) / ref_count;
				double new_sv1 = get<0>(found->second) + sv1_per_ref;
				sv1[price] = make_tuple(new_sv1, ref_count + 1);
			}
			else {
				// Get iterator to (sv1+)-, i.e. next (p,v+) or first (p+,v*)
				price_bcv_index::iterator sv1_ub = sv1.upper_bound(price);
				if ( sv1_ub != sv1.end() )
				{
					double price_ub = sv1_ub->first;
					double sv1_val_ub = get<0>(sv1_ub->second);
					int ref_count_ub = get<1>(sv1_ub->second);
					sv1[price] = make_tuple(sv1_val_ub, 1);
				}
				else {
					sv1[price] = make_tuple(0.0, 1);
					new_max_p = true;
				}
			}
		}
		else {
			assert ( found != sv1.end() );
			int ref_count = get<1>(found->second);
			if ( ref_count == 1 ) {
				// Delete group
				delete_sv1 = get<0>(found->second);
				delete_ref_count = 1;
				delete_p = true;
				sv1.erase(price);
			}
			else {
				// Decrement ref count
				double sv1_per_ref = get<0>(found->second) / ref_count;
				double new_sv1 = get<0>(found->second) - sv1_per_ref;
				sv1[price] = make_tuple(new_sv1, --ref_count);
			}
		}

		#ifdef DEBUG
		cout << "Before vwap " << sv1.size() << endl;
		print_price_bcv_index(sv1);
		cout << " ----- " << endl;
		#endif


		// Delta vwap.bcv
		double sv1_val = (delete_p? delete_sv1 : get<0>(sv1[price]));
		int sv1_ref_count = (delete_p? delete_ref_count : get<1>(sv1[price]));

		// Shift sv1+ by sv1+volume, svp+(price*volume), cnt+1
		vwap_index new_spv;
		cvol_index new_cvc;
		set<double> old_sv1s;

		set<double>::iterator shift_it = shifted_sv1.begin();
		set<double>::iterator shift_end = shifted_sv1.end();

		#ifdef DEBUG
		cout << "Testing shift tracking " << endl;
		for (; shift_it != shift_end; ++shift_it)
			cout << (*shift_it) << endl;
		cout << " ----- " << endl;
		#endif

		shift_it = shifted_sv1.begin();
		for (; shift_it != shift_end; ++shift_it)
		{
			double old_sv1 = *shift_it;

			vwap_index::iterator spv_found = spv.find(old_sv1);
			assert ( spv_found != spv.end() );

			tuple<double, double>& old_sum_cnt = spv_found->second;

			cvol_index::iterator cvc_found = cvc.find(old_sv1);
			assert ( cvc_found != cvc.end() );

			DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_found->second.begin();
			DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_found->second.end();

			DBT_HASH_SET<double> cvcp_deletions;

			for (; cvcp_it != cvcp_end; ++cvcp_it)
			{
				double cvcp_price = cvcp_it->first;

				if ( price > cvcp_price )
				{
					double shift_ref_count = get<1>(cvcp_it->second);

					#ifdef DEBUG
					cout << "RC: " << shift_ref_count << endl;

					cout << "Final shift rc: " << shift_ref_count
						<< ", " << old_sv1 << endl;
					#endif

					double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
						old_sv1 - (shift_ref_count*volume));
					double new_sum = (insert?
						get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
						get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
					int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
					new_spv[new_sv1] = make_tuple(new_sum, new_cnt);

					// Shift cvc simultaneously.
					// -- clean up eagerly here while we have new_cvcp
					new_cvc[new_sv1][cvcp_price] = cvcp_it->second;
					cvcp_deletions.insert(cvcp_price);

				}
			}

			DBT_HASH_SET<double>::iterator del_it = cvcp_deletions.begin();
			DBT_HASH_SET<double>::iterator del_end = cvcp_deletions.end();

			for (; del_it != del_end; ++del_it)
			{
				cvc_found->second.erase(*del_it);
				if ( cvc_found->second.empty() ) {
					old_sv1s.insert(spv_found->first);
				}
			}
		}

		#ifdef DEBUG
		cout << "Deferred " << new_spv.size() << endl;
		print_vwap_index(new_spv);
		cout << " ----- CVC -----" << endl;
		print_cvol_index(new_cvc);
		cout << " ----- " << endl;
		#endif

		set<double>::iterator old_it = old_sv1s.begin();
		set<double>::iterator old_end = old_sv1s.end();
		for (; old_it != old_end; ++old_it) {
			spv.erase(*old_it);
			cvc.erase(*old_it);
		}

		#ifdef DEBUG
		cout << "After erase " << spv.size() << endl;
		print_vwap_index(spv);
		cout << " ----- CVC -----" << endl;
		print_cvol_index(cvc);
		cout << " ----- " << endl;
		#endif

		// Check if sv1 exists.
		vwap_index::iterator sv1_found = spv.find(sv1_val);

		// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
		// -- note because of duplicates, we should get the iterator at the first (p+,v*)
		price_bcv_index::iterator sv1_minus = sv1.upper_bound(price);

		#ifdef DEBUG
		cout << "sv1_minus: "
			<< (sv1_minus == sv1.begin())
			<< (sv1_minus == sv1.end())
			<< (sv1_minus == sv1.end()? -1.0 : sv1_minus->first) << endl;
		#endif

		while ( sv1_minus->first == price && sv1_minus != sv1.end() )
			++sv1_minus;

		#ifdef DEBUG
		cout << "sv1_minus: "
			<< (sv1_minus == sv1.begin())
			<< (sv1_minus == sv1.end())
			<< (sv1_minus == sv1.end()? -1.0 : sv1_minus->first) << endl;
		#endif

		vwap_index::iterator spv_lb = spv.end();

		if ( sv1_minus != sv1.end() )
		{
			spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

			#ifdef DEBUG
			cout << "sv1-: " << get<0>(sv1_minus->second) << endl;
			cout << "spv-: "
				<< (spv_lb == spv.end()? -1.0 : spv_lb->first) << endl;
			#endif
		}

		if ( insert )
		{
			if ( new_p )
			{
				#ifdef DEBUG
				cout << "sv1_found: "
					<< (sv1_found == spv.begin())
					<< (sv1_found == spv.end())
					<< (sv1_found == spv.end()? -1.0 : sv1_found->first) << endl;
				#endif

				if ( sv1_found != spv.end() ) {
					tuple<double, double> sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(sum_cnt) + (price*volume), get<1>(sum_cnt)+1);
				}
				else {
					if ( spv_lb == spv.end() ) {
						spv[sv1_val] = make_tuple(price*volume, 1.0);
					}
					else
					{
						tuple<double, double> sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(sum_cnt)+(price*volume), get<1>(sum_cnt)+1);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != 1 ) {
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected 1 (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == 1);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, 1);
					}
					else
						cvc[sv1_val][price] = make_tuple(1,1);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1,1);
				}
			}
			else
			{
				double old_ref_count = sv1_ref_count - 1;
				double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
				double old_spv = old_ref_count * (price * volume);

				if ( spv.find(old_sv1) == spv.end() ) {
					cerr << "Failed to find spv for " << old_sv1 << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				// Check if we delete from spv
				assert ( spv.find(old_sv1) != spv.end() );
				tuple<double, double>& old_sum_cnt = spv[old_sv1];

				int check_count = 0;

				cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
				if ( old_cvc_found == cvc.end() ) {
					cerr << "Failed to find spv for " << old_sv1 << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				assert ( old_cvc_found != cvc.end() );
				DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
					old_cvc_found->second.find(price);

				if ( old_cvcp_found == old_cvc_found->second.end() ) {
					cerr << "Failed to find cvcp for "
						<< old_sv1 << ", " << price << endl;
					print_price_bcv_index(sv1);
					print_vwap_index(spv);
					print_cvol_index(cvc);
				}

				assert ( old_cvcp_found != old_cvc_found->second.end() );
				check_count = get<0>(old_cvcp_found->second);

				#ifdef DEBUG
				cout << "check_count " << check_count
					<< ", " << get<1>(old_sum_cnt) << endl;
				#endif

				if ( check_count == 1 ) {
					cvc[old_sv1].erase(price);
					if ( cvc[old_sv1].empty() ) {
						spv.erase(old_sv1);
						cvc.erase(old_sv1);
					}
				}

				// Otherwise update sum_cnt at old_spv
				else {
					spv[old_sv1] =
						make_tuple(get<0>(old_sum_cnt) - old_spv,
							get<1>(old_sum_cnt) - old_ref_count);

					int existing_gc = get<0>(cvc[old_sv1][price]);
					int existing_rc = get<1>(cvc[old_sv1][price]);

					if ( existing_rc != old_ref_count ) {
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected 1 (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == old_ref_count);
					cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
				}

				// Update sum_cnt at new_spv
				if ( sv1_found != spv.end() ) {
					tuple<double, double> new_sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				} else {

					if ( spv_lb == spv.end() ) {
						spv[sv1_val] =
							make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
					}
					else {
						tuple<double, double> new_sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
								get<1>(new_sum_cnt)+sv1_ref_count);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count )
						{
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected "
								<< sv1_ref_count << " (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == sv1_ref_count);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
					}
					else
						cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
				}

			}
		}
		else
		{
			double old_ref_count = delete_p? sv1_ref_count : sv1_ref_count+1;
			double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
			double old_spv = old_ref_count * (price*volume);

			if ( spv.find(old_sv1) == spv.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
			}

			// Check if we delete from spv
			assert ( spv.find(old_sv1) != spv.end() );
			tuple<double, double>& old_sum_cnt = spv[old_sv1];

			int check_count = 0;

			cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
			if ( old_cvc_found == cvc.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				cerr << "Failed to find cvcp for "
					<< old_sv1 << ", " << price << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			#ifdef DEBUG
			cout << "check_count "
				<< check_count << ", " << get<1>(old_sum_cnt) << endl;
			#endif

			if ( check_count == 1 ) {
				cvc[old_sv1].erase(price);
				if ( cvc[old_sv1].empty() ) {
					spv.erase(old_sv1);
					cvc.erase(old_sv1);
				}
			}

			// Otherwise update sum_cnt at old_spv
			else {
				spv[old_sv1] =
					make_tuple(get<0>(old_sum_cnt) - old_spv,
						get<1>(old_sum_cnt) - old_ref_count);

				int existing_gc = get<0>(cvc[old_sv1][price]);
				int existing_rc = get<1>(cvc[old_sv1][price]);

				if ( existing_rc != old_ref_count )
				{
					cerr << "Found inconsistent cvc rc "
						<< existing_rc << ", expected "
						<< old_ref_count << " (gc"
						<< existing_gc << ")" << endl;
					print_vwap_index(spv);
					cout << " ----- CVC ----- " << endl;
					print_cvol_index(cvc);
					cout << " ----- " << endl;
				}

				assert (existing_rc == old_ref_count);
				cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
			}

			// Update sum_cnt at new spv
			if ( !delete_p )
			{
				if ( sv1_found != spv.end() ) {
					tuple<double, double> new_sum_cnt = sv1_found->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				} else {

					if ( spv_lb == spv.end() ) {
						spv[sv1_val] = make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
					}
					else {
						tuple<double, double> new_sum_cnt = spv_lb->second;
						spv[sv1_val] =
							make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
								get<1>(new_sum_cnt)+sv1_ref_count);
					}
				}

				cvol_index::iterator cvc_found = cvc.find(sv1_val);
				if ( cvc_found != cvc.end() )
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(price);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count )
						{
							cerr << "Found inconsistent cvc rc "
								<< existing_rc << ", expected "
								<< sv1_ref_count << " (gc"
								<< existing_gc << ")" << endl;
							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == sv1_ref_count);
						cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
					}
					else
						cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

				}
				else {
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
				}
			}
		}

		#ifdef DEBUG
		cout << "Before merge " << spv.size() << endl;
		print_vwap_index(spv);
		cout << " ----- CVC ----- " << endl;
		print_cvol_index(cvc);
		cout << " ----- " << endl;
		#endif

		vwap_index::iterator nspv_it = new_spv.begin();
		vwap_index::iterator nspv_end = new_spv.end();

		for (; nspv_it != nspv_end; ++nspv_it)
			spv[nspv_it->first] = nspv_it->second;

		cvol_index::iterator ncvc_it = new_cvc.begin();
		cvol_index::iterator ncvc_end = new_cvc.end();

		for (; ncvc_it != ncvc_end; ++ncvc_it)
		{
			cvol_index::iterator cvc_found = cvc.find(ncvc_it->first);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_it = ncvc_it->second.begin();
				DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_end = ncvc_it->second.end();

				for (; ncvcp_it != ncvcp_end; ++ncvcp_it)
				{
					DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
						cvc_found->second.find(ncvcp_it->first);

					if ( cvcp_found != cvc_found->second.end() )
					{
						int existing_gc = get<0>(cvcp_found->second);
						int existing_rc = get<1>(cvcp_found->second);

						if ( existing_rc != sv1_ref_count ) {
							cerr << "Found inconsistent cvc for sv1 "
								<< ncvc_it->first << " p " << ncvcp_it->first
								<< " rc "
								<< existing_rc << ", expected "
								<< get<1>(ncvcp_it->second) << " (gc "
								<< existing_gc << ")" << endl;

							print_vwap_index(spv);
							cout << " ----- CVC ----- " << endl;
							print_cvol_index(cvc);
							cout << " ----- " << endl;
						}

						assert (existing_rc == get<1>(ncvcp_it->second));
						cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
					}
					else {
						cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
					}
				}
			}
			else
				cvc[ncvc_it->first] = ncvc_it->second;
		}

		#ifdef DEBUG
		cout << "After merge " << spv.size() << endl;
		print_vwap_index(spv);
		cout << " ----- CVC ----- " << endl;
		print_cvol_index(cvc);
		cout << " ----- " << endl;
		#endif


		// Delta vwap.bv
		vwap_index::iterator result_it = spv.upper_bound(stv);

		while ( get<0>(result_it->second) == stv && (result_it != spv.end()) )
			++result_it;

		if ( result_it != spv.end() ) {
			tuple<double, double> result_sum_cnt = result_it->second;
			result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
		}
		else {
			if ( !spv.empty() ) {
				vwap_index::iterator back = spv.begin();
				tuple<double, double> result_sum_cnt = back->second;
				result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
			}
		}

		#ifdef DEBUG
		cout << "At return " << spv.size() << endl;
		print_vwap_index(spv);
		cout << " ----- CVC ----- " << endl;
		print_cvol_index(cvc);
		cout << " ----- " << endl;
		#endif
	}

	return result;
}

double bulk_update_vwap_varying3(
	list<tuple<double, double> >& price_vols,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc, bool insert = true)
{
	// Collect bulk exec stats
	bulk_executions.push_back(price_vols.size());

	list< tuple<double, double> >::iterator pv_it = price_vols.begin();
	list< tuple<double, double> >::iterator pv_end = price_vols.end();

	// Batch delta stv, B1
	double r;

	for (; pv_it != pv_end; ++pv_it)
	{
		double price = get<0>(*pv_it);
		double volume = get<1>(*pv_it);

		r = update_vwap2(price, volume, stv, sv1, spv, cvc, insert);
	}

	return r;
}

//////////////////////////
//
// Fixed size bulk helpers

inline void delta_b1(
	double& price, double& volume,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc,
	set<double>& shifted_sv1, bool insert = true)
{
	// Delta stv
	stv = (insert? stv + volume : stv - volume);

	// Delta bcv.b1

	// Get iterator to p-, i.e. possibly (p,v+=) or (p+,v*)
	price_bcv_index::iterator sv1_lb = sv1.lower_bound(price);
	if ( sv1_lb == sv1.end() && !sv1.empty() )
		--sv1_lb;

	if ( sv1_lb == sv1.begin() && sv1_lb->first >= price )
		sv1_lb = sv1.end();

	price_bcv_index new_sv1;
	for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
		double sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		new_sv1[sv1_lb->first] = make_tuple(
			insert? sv1_val+(ref_count*volume) :
				sv1_val-(ref_count*volume),
			ref_count);

		shifted_sv1.insert(sv1_val);
	}

	if ( sv1_lb != sv1.end() )
	{
		double final_sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		new_sv1[sv1_lb->first] = make_tuple(
			insert? final_sv1_val + (ref_count*volume) :
				final_sv1_val - (ref_count*volume),
			ref_count);

		shifted_sv1.insert(final_sv1_val);
	}

	price_bcv_index::iterator nsv1_it, nsv1_end;
	nsv1_it = new_sv1.begin(); nsv1_end = new_sv1.end();
	for (; nsv1_it != nsv1_end; ++nsv1_it)
		sv1[nsv1_it->first] = nsv1_it->second;

}

inline void delta_b2(
	double& price, double& volume,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc,
	b2_state& b2_st, bool insert = true)
{
	price_bcv_index::iterator found = sv1.find(price);

	bool new_p = found == sv1.end();
	bool new_max_p = false;
	double delete_sv1 = 0.0;
	int delete_ref_count = 0;
	bool delete_p = false;

	if ( insert )
	{
		// Handle duplicates in b2 groups.
		if ( found != sv1.end() ) {
			int ref_count = get<1>(found->second);
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1 = get<0>(found->second) + sv1_per_ref;
			sv1[price] = make_tuple(new_sv1, ref_count + 1);
		}
		else {
			// Get iterator to (sv1+)-, i.e. next (p,v+) or first (p+,v*)
			price_bcv_index::iterator sv1_ub = sv1.upper_bound(price);
			if ( sv1_ub != sv1.end() )
			{
				double price_ub = sv1_ub->first;
				double sv1_val_ub = get<0>(sv1_ub->second);
				int ref_count_ub = get<1>(sv1_ub->second);
				sv1[price] = make_tuple(sv1_val_ub, 1);
			}
			else {
				sv1[price] = make_tuple(0.0, 1);
				new_max_p = true;
			}
		}
	}
	else {
		assert ( found != sv1.end() );
		int ref_count = get<1>(found->second);
		if ( ref_count == 1 ) {
			// Delete group
			delete_sv1 = get<0>(found->second);
			delete_ref_count = 1;
			delete_p = true;
			sv1.erase(price);
		}
		else {
			// Decrement ref count
			double sv1_per_ref = get<0>(found->second) / ref_count;
			double new_sv1 = get<0>(found->second) - sv1_per_ref;
			sv1[price] = make_tuple(new_sv1, --ref_count);
		}
	}

	b2_st[make_tuple(price, volume)] =
		make_tuple(new_p, new_max_p, delete_sv1, delete_ref_count, delete_p);
}

inline void delta_vwap(
	double& price, double& volume,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc,
	set<double>& shifted_sv1,
	tuple<bool, bool, double, int, bool>& pv_b2_st,
	bool insert = true)
{
	bool new_p = get<0>(pv_b2_st);
	bool new_max_p = get<1>(pv_b2_st);
	double delete_sv1 = get<2>(pv_b2_st);
	int delete_ref_count = get<3>(pv_b2_st);
	bool delete_p = get<4>(pv_b2_st);

	// Delta vwap.bcv
	double sv1_val = (delete_p? delete_sv1 : get<0>(sv1[price]));
	int sv1_ref_count = (delete_p? delete_ref_count : get<1>(sv1[price]));

	// Shift sv1+ by sv1+volume, svp+(price*volume), cnt+1
	vwap_index new_spv;
	cvol_index new_cvc;
	set<double> old_sv1s;

	set<double>::iterator shift_it = shifted_sv1.begin();
	set<double>::iterator shift_end = shifted_sv1.end();

	shift_it = shifted_sv1.begin();
	for (; shift_it != shift_end; ++shift_it)
	{
		double old_sv1 = *shift_it;

		vwap_index::iterator spv_found = spv.find(old_sv1);
		assert ( spv_found != spv.end() );

		tuple<double, double>& old_sum_cnt = spv_found->second;

		cvol_index::iterator cvc_found = cvc.find(old_sv1);
		assert ( cvc_found != cvc.end() );

		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_it = cvc_found->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_end = cvc_found->second.end();

		DBT_HASH_SET<double> cvcp_deletions;

		for (; cvcp_it != cvcp_end; ++cvcp_it)
		{
			double cvcp_price = cvcp_it->first;

			if ( price > cvcp_price )
			{
				double shift_ref_count = get<1>(cvcp_it->second);

				double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
					old_sv1 - (shift_ref_count*volume));
				double new_sum = (insert?
					get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
					get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
				int new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
				new_spv[new_sv1] = make_tuple(new_sum, new_cnt);

				// Shift cvc simultaneously.
				// -- clean up eagerly here while we have new_cvcp
				new_cvc[new_sv1][cvcp_price] = cvcp_it->second;
				cvcp_deletions.insert(cvcp_price);

			}
		}

		DBT_HASH_SET<double>::iterator del_it = cvcp_deletions.begin();
		DBT_HASH_SET<double>::iterator del_end = cvcp_deletions.end();

		for (; del_it != del_end; ++del_it)
		{
			cvc_found->second.erase(*del_it);
			if ( cvc_found->second.empty() ) {
				old_sv1s.insert(spv_found->first);
			}
		}
	}

	set<double>::iterator old_it = old_sv1s.begin();
	set<double>::iterator old_end = old_sv1s.end();
	for (; old_it != old_end; ++old_it) {
		spv.erase(*old_it);
		cvc.erase(*old_it);
	}

	// Check if sv1 exists.
	vwap_index::iterator sv1_found = spv.find(sv1_val);

	// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
	// -- note because of duplicates, we should get the iterator at the first (p+,v*)
	price_bcv_index::iterator sv1_minus = sv1.upper_bound(price);

	while ( sv1_minus->first == price && sv1_minus != sv1.end() )
		++sv1_minus;

	vwap_index::iterator spv_lb = spv.end();

	if ( sv1_minus != sv1.end() )
		spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

	if ( insert )
	{
		if ( new_p )
		{
			if ( sv1_found != spv.end() ) {
				tuple<double, double> sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(sum_cnt) + (price*volume), get<1>(sum_cnt)+1);
			}
			else {
				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(price*volume, 1.0);
				}
				else
				{
					tuple<double, double> sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(sum_cnt)+(price*volume), get<1>(sum_cnt)+1);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != 1 ) {
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected 1 (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == 1);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, 1);
				}
				else
					cvc[sv1_val][price] = make_tuple(1,1);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1,1);
			}
		}
		else
		{
			double old_ref_count = sv1_ref_count - 1;
			double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
			double old_spv = old_ref_count * (price * volume);

			if ( spv.find(old_sv1) == spv.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			// Check if we delete from spv
			assert ( spv.find(old_sv1) != spv.end() );
			tuple<double, double>& old_sum_cnt = spv[old_sv1];

			int check_count = 0;

			cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
			if ( old_cvc_found == cvc.end() ) {
				cerr << "Failed to find spv for " << old_sv1 << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				cerr << "Failed to find cvcp for "
					<< old_sv1 << ", " << price << endl;
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			if ( check_count == 1 ) {
				cvc[old_sv1].erase(price);
				if ( cvc[old_sv1].empty() ) {
					spv.erase(old_sv1);
					cvc.erase(old_sv1);
				}
			}

			// Otherwise update sum_cnt at old_spv
			else {
				spv[old_sv1] =
					make_tuple(get<0>(old_sum_cnt) - old_spv,
						get<1>(old_sum_cnt) - old_ref_count);

				int existing_gc = get<0>(cvc[old_sv1][price]);
				int existing_rc = get<1>(cvc[old_sv1][price]);

				if ( existing_rc != old_ref_count ) {
					cerr << "Found inconsistent cvc rc "
						<< existing_rc << ", expected 1 (gc"
						<< existing_gc << ")" << endl;
					print_vwap_index(spv);
					cout << " ----- CVC ----- " << endl;
					print_cvol_index(cvc);
					cout << " ----- " << endl;
				}

				assert (existing_rc == old_ref_count);
				cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
			}

			// Update sum_cnt at new_spv
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);
			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] =
						make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}

		}
	}
	else
	{
		double old_ref_count = delete_p? sv1_ref_count : sv1_ref_count+1;
		double old_sv1 = old_ref_count * (sv1_val / sv1_ref_count);
		double old_spv = old_ref_count * (price*volume);

		if ( spv.find(old_sv1) == spv.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
		}

		// Check if we delete from spv
		assert ( spv.find(old_sv1) != spv.end() );
		tuple<double, double>& old_sum_cnt = spv[old_sv1];

		int check_count = 0;

		cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
		if ( old_cvc_found == cvc.end() ) {
			cerr << "Failed to find spv for " << old_sv1 << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvc_found != cvc.end() );
		DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
			old_cvc_found->second.find(price);

		if ( old_cvcp_found == old_cvc_found->second.end() ) {
			cerr << "Failed to find cvcp for "
				<< old_sv1 << ", " << price << endl;
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvcp_found != old_cvc_found->second.end() );
		check_count = get<0>(old_cvcp_found->second);

		if ( check_count == 1 ) {
			cvc[old_sv1].erase(price);
			if ( cvc[old_sv1].empty() ) {
				spv.erase(old_sv1);
				cvc.erase(old_sv1);
			}
		}

		// Otherwise update sum_cnt at old_spv
		else {
			spv[old_sv1] =
				make_tuple(get<0>(old_sum_cnt) - old_spv,
					get<1>(old_sum_cnt) - old_ref_count);

			int existing_gc = get<0>(cvc[old_sv1][price]);
			int existing_rc = get<1>(cvc[old_sv1][price]);

			if ( existing_rc != old_ref_count )
			{
				cerr << "Found inconsistent cvc rc "
					<< existing_rc << ", expected "
					<< old_ref_count << " (gc"
					<< existing_gc << ")" << endl;
				print_vwap_index(spv);
				cout << " ----- CVC ----- " << endl;
				print_cvol_index(cvc);
				cout << " ----- " << endl;
			}

			assert (existing_rc == old_ref_count);
			cvc[old_sv1][price] = make_tuple(existing_gc-1, existing_rc);
		}

		// Update sum_cnt at new spv
		if ( !delete_p )
		{
			if ( sv1_found != spv.end() ) {
				tuple<double, double> new_sum_cnt = sv1_found->second;
				spv[sv1_val] =
					make_tuple(get<0>(new_sum_cnt) + (sv1_ref_count*price*volume),
						get<1>(new_sum_cnt)+sv1_ref_count);
			} else {

				if ( spv_lb == spv.end() ) {
					spv[sv1_val] = make_tuple(sv1_ref_count*price*volume, sv1_ref_count);
				}
				else {
					tuple<double, double> new_sum_cnt = spv_lb->second;
					spv[sv1_val] =
						make_tuple(get<0>(new_sum_cnt)+(sv1_ref_count*price*volume),
							get<1>(new_sum_cnt)+sv1_ref_count);
				}
			}

			cvol_index::iterator cvc_found = cvc.find(sv1_val);
			if ( cvc_found != cvc.end() )
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(price);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count )
					{
						cerr << "Found inconsistent cvc rc "
							<< existing_rc << ", expected "
							<< sv1_ref_count << " (gc"
							<< existing_gc << ")" << endl;
						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == sv1_ref_count);
					cvc[sv1_val][price] = make_tuple(existing_gc+1, sv1_ref_count);
				}
				else
					cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);

			}
			else {
				cvc[sv1_val][price] = make_tuple(1, sv1_ref_count);
			}
		}
	}

	vwap_index::iterator nspv_it = new_spv.begin();
	vwap_index::iterator nspv_end = new_spv.end();

	for (; nspv_it != nspv_end; ++nspv_it)
		spv[nspv_it->first] = nspv_it->second;

	cvol_index::iterator ncvc_it = new_cvc.begin();
	cvol_index::iterator ncvc_end = new_cvc.end();

	for (; ncvc_it != ncvc_end; ++ncvc_it)
	{
		cvol_index::iterator cvc_found = cvc.find(ncvc_it->first);
		if ( cvc_found != cvc.end() )
		{
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_it = ncvc_it->second.begin();
			DBT_HASH_MAP<double, tuple<int, int> >::iterator ncvcp_end = ncvc_it->second.end();

			for (; ncvcp_it != ncvcp_end; ++ncvcp_it)
			{
				DBT_HASH_MAP<double, tuple<int, int> >::iterator cvcp_found =
					cvc_found->second.find(ncvcp_it->first);

				if ( cvcp_found != cvc_found->second.end() )
				{
					int existing_gc = get<0>(cvcp_found->second);
					int existing_rc = get<1>(cvcp_found->second);

					if ( existing_rc != sv1_ref_count ) {
						cerr << "Found inconsistent cvc for sv1 "
							<< ncvc_it->first << " p " << ncvcp_it->first
							<< " rc "
							<< existing_rc << ", expected "
							<< get<1>(ncvcp_it->second) << " (gc "
							<< existing_gc << ")" << endl;

						print_vwap_index(spv);
						cout << " ----- CVC ----- " << endl;
						print_cvol_index(cvc);
						cout << " ----- " << endl;
					}

					assert (existing_rc == get<1>(ncvcp_it->second));
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
				else {
					cvc[ncvc_it->first][ncvcp_it->first] = ncvcp_it->second;
				}
			}
		}
		else
			cvc[ncvc_it->first] = ncvc_it->second;
	}
}


double bulk_update_vwap_5(vector<tuple<double, double> >& price_vols,
	double& stv, price_bcv_index& sv1,
	vwap_index& spv, cvol_index& cvc, bool insert = true)
{
	assert ( price_vols.size() == 5 );

	// Collect bulk exec stats
	++bulk5_counter;

	double price0 = get<0>(price_vols[0]);
	double price1 = get<0>(price_vols[1]);
	double price2 = get<0>(price_vols[2]);
	double price3 = get<0>(price_vols[3]);
	double price4 = get<0>(price_vols[4]);

	double volume0 = get<1>(price_vols[0]);
	double volume1 = get<1>(price_vols[1]);
	double volume2 = get<1>(price_vols[2]);
	double volume3 = get<1>(price_vols[3]);
	double volume4 = get<1>(price_vols[4]);

	set<double> shifted_sv1_0;
	set<double> shifted_sv1_1;
	set<double> shifted_sv1_2;
	set<double> shifted_sv1_3;
	set<double> shifted_sv1_4;

	delta_b1(price0, volume0, stv, sv1, spv, cvc, shifted_sv1_0, insert);
	delta_b1(price1, volume1, stv, sv1, spv, cvc, shifted_sv1_1, insert);
	delta_b1(price2, volume2, stv, sv1, spv, cvc, shifted_sv1_2, insert);
	delta_b1(price3, volume3, stv, sv1, spv, cvc, shifted_sv1_3, insert);
	delta_b1(price4, volume4, stv, sv1, spv, cvc, shifted_sv1_4, insert);

	b2_state b2_st;

	delta_b2(price0, volume0, stv, sv1, spv, cvc, b2_st, insert);
	delta_b2(price1, volume1, stv, sv1, spv, cvc, b2_st, insert);
	delta_b2(price2, volume2, stv, sv1, spv, cvc, b2_st, insert);
	delta_b2(price3, volume3, stv, sv1, spv, cvc, b2_st, insert);
	delta_b2(price4, volume4, stv, sv1, spv, cvc, b2_st, insert);

	delta_vwap(price0, volume0, stv, sv1, spv, cvc, shifted_sv1_0,
		b2_st[make_tuple(price0, volume0)], insert);

	delta_vwap(price1, volume1, stv, sv1, spv, cvc, shifted_sv1_1,
		b2_st[make_tuple(price1, volume1)], insert);

	delta_vwap(price2, volume2, stv, sv1, spv, cvc, shifted_sv1_2,
		b2_st[make_tuple(price2, volume2)], insert);

	delta_vwap(price3, volume3, stv, sv1, spv, cvc, shifted_sv1_3,
		b2_st[make_tuple(price3, volume3)], insert);

	delta_vwap(price4, volume4, stv, sv1, spv, cvc, shifted_sv1_4,
		b2_st[make_tuple(price4, volume4)], insert);

	// Delta vwap.bv
	vwap_index::iterator result_it = spv.upper_bound(stv);

	while ( get<0>(result_it->second) == stv && (result_it != spv.end()) )
		++result_it;

	double result = 0.0;
	if ( result_it != spv.end() ) {
		tuple<double, double> result_sum_cnt = result_it->second;
		result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
	}
	else {
		if ( !spv.empty() ) {
			vwap_index::iterator back = spv.begin();
			tuple<double, double> result_sum_cnt = back->second;
			result = get<0>(result_sum_cnt) / get<1>(result_sum_cnt);
		}
	}

	return result;
}

/*
double new_update_vwap2(double p, double v, bool insert = true)
{
	vwap_sum& vs = pv_sBBB[p][v];
	double& sbBB = pv_sbBB[v];
	double& sBBb = vs.sBBb;
	double& sBbB = vs.sBbB;
	double& sBbb = vs.sBbb;
	double& sbBb = vs.sbBb;
	double& sbbB = vs.sbbB;
	double& sbbb = vs.sbbb;

	vwap_count& vc = pv_cBBB[p][v];
	double& cbBB = pv_cbBB[v];
	double& cBBb = vc.cBBb;
	double& cBbB = vc.cBbB;
	double& cBbb = vc.cBbb;
	double& cbBb = vc.cbBb;
	double& cbbB = vc.cbbB;
	double& cbbb = vc.cbbb;

	if ( insert )
	{
		sbbb = ((1.0*v>0) ? (p*v) : 0);

		double snew_BBB = sBBB + sBBb + sBbB + sBbb + sbBB + sbBb + sbbB + sbbb;

		double snew_bBB = sbBB + sbBb + sbbB + sbbb;
		double snew_bbB = sbbB + sbbb;
		double snew_BBb = sBBb + sBbb + sbBb + sbbb;
		double snew_Bbb = sBbb + sbbb;
		double snew_BbB = sBbB + sBbb + sbbB + sbbb;
		double snew_bBb = sbBb + sbbb;

		sBBB = snew_BBB;
		sbBB = snew_bBB;
		sbbB = snew_bbB;
		sBBb = snew_BBb;
		sBbb = snew_Bbb;
		sBbB = snew_BbB;
		//sbbb = snew_bbb;
		sbBb = snew_bBb;

#ifdef DEBUG
		cout << "------------------------- " << endl;
		cout << "sBBB " << sBBB << "\n"
			<< "sbBB " << sbBB[v] << "\n"
			<< "sbbB " << sbbB << "\n"
			<< "sBBb " << sBBb << "\n"
			<< "sBbb " << sBbb << "\n"
			<< "sBbB " << sBbB << "\n"
			<< "sbbb " << sbbb << "\n"
			<< "sbBb " << sbBb << endl;
#endif


		cbbb = (1.0*v>0) ? 1 : 0;

		double cnew_BBB = cBBB + cBBb + cBbB + cBbb + cbBB + cbBb + cbbB + cbbb;

		double cnew_bBB = cbBB + cbBb + cbbB + cbbb;
		double cnew_bbB = cbbB + cbbb;
		double cnew_BBb = cBBb + cBbb + cbBb + cbbb;
		double cnew_Bbb = cBbb + cbbb;
		double cnew_BbB = cBbB + cBbb + cbbB + cbbb;
		double cnew_bBb = cbBb + cbbb;

		cBBB = cnew_BBB;
		cbBB = cnew_bBB;
		cbbB = cnew_bbB;
		cBBb = cnew_BBb;
		cBbb = cnew_Bbb;
		cBbB = cnew_BbB;
		//cbbb = cnew_bbb;
		cbBb = cnew_bBb;

#ifdef DEBUG
		cout << "------------------------- " << endl;
		cout << "cBBB " << cBBB << "\n"
			<< "cbBB " << cbBB << "\n"
			<< "cbbB " << cbbB << "\n"
			<< "cBBb " << cBBb << "\n"
			<< "cBbb " << cBbb << "\n"
			<< "cBbB " << cBbB << "\n"
			<< "cbbb " << cbbb << "\n"
			<< "cbBb " << cbBb << endl;
#endif

	}
	else
	{
		sbbb = ((1.0*v>0) ? -(p*v) : 0);

		double snew_BBB = sBBB - (sBBb + sBbB + sBbb
				 + sbBB + sbBb + sbbB + sbbb);

		double snew_bBB = sbBB - (sbBb + sbbB + sbbb);
		double snew_bbB = sbbB - sbbb;
		double snew_BBb = sBBb - (sBbb + sbBb + sbbb);
		double snew_Bbb = sBbb - sbbb;
		double snew_BbB = sBbB - (sBbb + sbbB + sbbb);
		double snew_bBb = sbBb - sbbb;

		sBBB = snew_BBB;
		sbBB = snew_bBB;
		sbbB = snew_bbB;
		sBBb = snew_BBb;
		sBbb = snew_Bbb;
		sBbB = snew_BbB;
		//sbbb = snew_bbb;
		sbBb = snew_bBb;

#ifdef DEBUG
		cout << "------------------------- " << endl;
		cout << "sBBB " << sBBB << "\n"
			<< "sbBB " << sbBB << "\n"
			<< "sbbB " << sbbB << "\n"
			<< "sBBb " << sBBb << "\n"
			<< "sBbb " << sBbb << "\n"
			<< "sBbB " << sBbB << "\n"
			<< "sbbb " << sbbb << "\n"
			<< "sbBb " << sbBb << endl;
#endif

		cbbb = (1.0*v>0) ? -1 : 0;

		double cnew_BBB = cBBB - (cBBb + cBbB + cBbb + cbBB + cbBb + cbbB + cbbb);

		double cnew_bBB = cbBB - (cbBb + cbbB + cbbb);
		double cnew_bbB = cbbB - cbbb;
		double cnew_BBb = cBBb - (cBbb + cbBb + cbbb);
		double cnew_Bbb = cBbb - cbbb;
		double cnew_BbB = cBbB - (cBbb + cbbB + cbbb);
		double cnew_bBb = cbBb - cbbb;

		cBBB = cnew_BBB;
		cbBB = cnew_bBB;
		cbbB = cnew_bbB;
		cBBb = cnew_BBb;
		cBbb = cnew_Bbb;
		cBbB = cnew_BbB;
		//cbbb = cnew_bbb;
		cbBb = cnew_bBb;

#ifdef DEBUG
		cout << "------------------------- " << endl;
		cout << "cBBB " << cBBB << "\n"
			<< "cbBB " << cbBB << "\n"
			<< "cbbB " << cbbB << "\n"
			<< "cBBb " << cBBb << "\n"
			<< "cBbb " << cBbb << "\n"
			<< "cBbB " << cBbB << "\n"
			<< "cbbb " << cbbb << "\n"
			<< "cbBb " << cbBb << endl;
#endif
	}

	return sBBB / cBBB;
}
*/

double stv_bids;
bcv_index sv1_bids;
price_bcv_index sv1_pbids;
vwap_index spv_bids;
cvol_index cvc_bids;

double stv_asks;
bcv_index sv1_asks;
price_bcv_index sv1_pasks;
vwap_index spv_asks;
cvol_index cvc_asks;

#ifdef MEMORY
static const size_t bcv_index_entry_sz = 2*sizeof(tuple<double, double>);
static const size_t vwap_index_entry_sz = sizeof(tuple<double, double>)+sizeof(double);
static const size_t cvol_index_entry_sz = sizeof(tuple<int,int>)+sizeof(double);

// Memory usage format:
// snapshot id
// total DBToaster maps size
// # sv1_pbids entries
// sv1_pbids size
// # spv_bids entries
// spv_bids size
// # cvc_bids entries
// cvc_bids size
// Fields from /proc/<pid>/statm:
//   Total program size, in kilobytes.
//   Size of memory portions, in kilobytes.
//   Number of pages that are shared.
//   Number of pages that are code.
//   Number of pages of data/stack.
//   Number of library pages.
//   Number of dirty pages.

void analyse_memory_usage(ofstream* stats_file, unsigned long counter)
{
	size_t bcv_size = sv1_pbids.size()*bcv_index_entry_sz;
	size_t vwap_size = spv_bids.size()*vwap_index_entry_sz;

	size_t cvol_entries = 0;
	size_t cvol_size = 0;
	cvol_index::iterator cv_it = cvc_bids.begin();
	cvol_index::iterator cv_end = cvc_bids.end();

	for (; cv_it != cv_end; ++cv_it) {
		cvol_entries += cv_it->second.size();
		cvol_size += (cv_it->second.size()*cvol_index_entry_sz);
	}

	(*stats_file) << counter << ","
		<< (bcv_size+vwap_size+cvol_size) << ","
		<< sv1_pbids.size() << "," << bcv_size << ","
		<< spv_bids.size() << "," << vwap_size << ","
		<< cvol_entries << "," << cvol_size;

	// Get memory usage from /proc
	pid_t my_pid = getpid();
	ostringstream proc_file_name;
	proc_file_name << "/proc/" << my_pid << "/statm";
	string proc_file = proc_file_name.str();

	ifstream statm_if(proc_file.c_str());
	if ( statm_if.good() ) {
		char buf[256];
		statm_if.getline(buf, sizeof(buf));
		vector<int> mem_stats;
		split(7, ' ', &(buf[0]), mem_stats);
		vector<int>::iterator ms_it = mem_stats.begin();
		vector<int>::iterator ms_end = mem_stats.end();

		for (; ms_it != ms_end; ++ms_it)
			(*stats_file) << "," << *ms_it;
	}
	statm_if.close();

	(*stats_file) << endl;
}
#endif


void vwap_stream(stream* s, ofstream* results, ofstream* log, ofstream* stats,
	bool dump = false)
{

	typedef DBT_HASH_MAP<int, tuple<double, double> > order_ids;

	order_ids bid_orders;
	order_ids ask_orders;

	struct timeval tvs, tve;

	double result = 0.0;

	#ifdef MEMORY
	// Space analysis.
	unsigned long tuple_counter = 0;
	unsigned long mem_sample_freq = 100;		// Sample every 100 tuples.
	#endif

	#ifdef BULK
	// Bulk execution.
	list< tuple<double, double> > bids_batch;
	DBT_HASH_SET<int> bulk_order_ids;
	unsigned long batch_threshold = 10;

	vector< tuple<double, double> > bids_batch5(5);
	DBT_HASH_SET<int> bulk5_order_ids;
	unsigned long bid_counter;

	bool bulk = false;
	bool varying = false;
	#endif

	gettimeofday(&tvs, NULL);

	while ( s->stream_has_inputs() )
	{
		#ifdef MEMORY
		++tuple_counter;
		if ( (tuple_counter % mem_sample_freq) == 0 )
			analyse_memory_usage(stats, tuple_counter / mem_sample_freq);
		#endif

		stream_tuple in = s->next_input();

		int order_id = get<1>(in);
		string action = get<2>(in);

		#ifdef LOG_INPUT
		cout << "Tuple: order " << order_id << ", " << action
			<< " t=" << get<0>(in) << " p=" << get<4>(in)
			<< " v=" << get<3>(in) << endl;
		#endif

		if (action == "B") {
			// Insert bids
			double price = get<4>(in);
			double volume = get<3>(in);

			tuple<double, double> pv = make_tuple(price, volume);
			bid_orders[order_id] = pv;

			#ifdef BULK
			if ( bulk && varying )
			{
				bids_batch.push_back(pv);
				if ( bids_batch.size() == batch_threshold ) {
					result = bulk_update_vwap_varying2(
						bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);
					bids_batch.clear();
					bulk_order_ids.clear();
				}
				else {
					bulk_order_ids.insert(order_id);
				}
			}
			else if ( bulk )
			{
				if ( (bid_counter % 5) == 0 )
				{
					result = bulk_update_vwap_5(
						bids_batch5, stv_bids, sv1_pbids, spv_bids, cvc_bids);

					bids_batch5.clear();
					bulk5_order_ids.clear();

					bids_batch5.reserve(5);
					bids_batch5[0] = pv;
					++bid_counter;
				}
				else {
					bids_batch5[bid_counter] = pv;
					++bid_counter;
				}

				bulk5_order_ids.insert(order_id);
			}
			else {
			#endif

				result = update_vwap2(
					price, volume, stv_bids, sv1_pbids, spv_bids, cvc_bids);
				//result = new_update_vwap2(price, volume);

			#ifdef BULK
			}
			#endif
		}

		else if (action == "S")
		{
			/*
			// Insert asks
			double price = get<4>(in);
			double volume = get<3>(in);

			ask_orders[order_id] = make_tuple(price, volume);
			result = update_vwap2(
				price, volume, stv_asks, sv1_pasks, spv_asks, cvc_asks);
			*/
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			double delta_volume = get<3>(in);

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

				#ifdef BULK
				if ( bulk && varying )
				{
					// Apply the batch and the delete partial order.
					list<tuple<double, double> >::iterator bulk_bid_found =
						find(bids_batch.begin(), bids_batch.end(), old_pv);

					result = bulk_update_vwap_varying2(
						bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

					bids_batch.clear();
					bulk_order_ids.clear();
				}
				else if ( bulk )
				{
					// Apply the batch and the delete partial order.
					// Serially execute the batch given there may be less than 5 orders.
					for (unsigned int i = 0; i < bids_batch5.size(); ++i)
					{
						result = update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]),
							stv_bids, sv1_pbids, spv_bids, cvc_bids);

						/*
						result = new_update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));
						*/
					}

					bids_batch5.clear();
					bids_batch5.reserve(5);
					bulk5_order_ids.clear();
				}
				#endif

				result = update_vwap2(price, volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);

				//result = new_update_vwap2(price, volume, false);


				if ( new_volume > 0 )
				{
					bid_orders[order_id] = new_pv;
					result = update_vwap2(price, new_volume,
						stv_bids, sv1_pbids, spv_bids, cvc_bids);
					//result = new_update_vwap2(price, new_volume);
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

					result = update_vwap2(price, volume,
						stv_asks, sv1_pasks, spv_asks, cvc_asks, false);

					//result = new_update_vwap2(price, volume, false);

					if ( new_volume > 0 ) {
						result = update_vwap2(price, new_volume,
							stv_asks, sv1_pasks, spv_asks, cvc_asks);

						//result = new_update_vwap2(price, new_volume);
					}
				}
			}
		}

		else if (action == "F")
		{
			// Order executed in full
			order_ids::iterator bid_found = bid_orders.find(order_id);
			if ( bid_found != bid_orders.end() )
			{
				double price =  get<0>(bid_found->second);
				double volume =  get<1>(bid_found->second);

				bid_orders.erase(bid_found);

				#ifdef BULK
				if ( bulk && varying )
				{
					result = bulk_update_vwap_varying2(
						bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

					bids_batch.clear();
					bulk_order_ids.clear();
				}
				else if ( bulk )
				{
					// Serially execute the batch.
					for (unsigned int i = 0; i < bids_batch5.size(); ++i)
					{
						result = update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]),
							stv_bids, sv1_pbids, spv_bids, cvc_bids);
						/*
						result = new_update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));
						*/
					}

					bids_batch5.clear();
					bids_batch5.reserve(5);
					bulk5_order_ids.clear();
				}
				#endif

				result = update_vwap2(price, volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);
				//result = new_update_vwap2(price, volume, false);
			}
			else
			{
				order_ids::iterator ask_found = ask_orders.find(order_id);
				if ( ask_found != ask_orders.end() )
				{
					double price =  get<0>(ask_found->second);
					double volume =  get<1>(ask_found->second);
					ask_orders.erase(ask_found);
					result = update_vwap2(price, volume,
						stv_asks, sv1_pasks, spv_asks, cvc_asks, false);
					//result = new_update_vwap2(price, volume, false);
				}
			}
		}

		else if (action == "D")
		{
			// Delete from relevant order book.
			order_ids::iterator bid_found = bid_orders.find(order_id);
			if ( bid_found != bid_orders.end() )
			{
				double price =  get<0>(bid_found->second);
				double volume =  get<1>(bid_found->second);

				bid_orders.erase(bid_found);

				#ifdef BULK
				if ( bulk && varying )
				{
					result = bulk_update_vwap_varying2(
						bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

					bids_batch.clear();
					bulk_order_ids.clear();
				}
				else if ( bulk )
				{
					// Serially execute the batch.
					for (unsigned int i = 0; i < bids_batch5.size(); ++i)
					{
						result = update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]),
							stv_bids, sv1_pbids, spv_bids, cvc_bids);

						/*
						result = new_update_vwap2(
							get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));
						*/
					}

					bids_batch5.clear();
					bids_batch5.reserve(5);
					bulk5_order_ids.clear();
				}
				#endif

				result = update_vwap2(price, volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);

				//result = new_update_vwap2(price, volume, false);
			}
			else {
				order_ids::iterator ask_found = ask_orders.find(order_id);
				if ( ask_found != ask_orders.end() )
				{
					double price =  get<0>(ask_found->second);
					double volume =  get<1>(ask_found->second);

					ask_orders.erase(ask_found);
					result = update_vwap2(
						price, volume, stv_asks, sv1_pasks, spv_asks, cvc_asks, false);
					//result = new_update_vwap2(price, volume, false);
				}
			}
		}

		/*
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		*/

		#ifdef OUTPUT
		(*results) << result << endl;
		#endif

		//cout << "After results (bids) " << spv_bids.size() << endl;
		//cout << "After results (asks) " << spv_asks.size() << endl;

		#ifdef DEBUG
		print_price_bcv_index(sv1_pbids);
		print_vwap_index(spv_bids);
		#endif

		//#ifdef DEBUG
		//validate_indexes(sv1_pbids, spv_bids, cvc_bids);
		//#endif
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP toasted QP", log, false);

	double bulk_total = 0.0;
	double bulk_avg_size = 0.0;
	list<unsigned long>::iterator be_it = bulk_executions.begin();
	list<unsigned long>::iterator be_end = bulk_executions.end();

	for (double i = 1.0; be_it != be_end; ++be_it, ++i) {
		bulk_total += static_cast<double>(*be_it);
		bulk_avg_size += ((static_cast<double>(*be_it) - bulk_avg_size) / i);
	}

	cout << "Bulk executions: " << bulk_executions.size()
		<< " avg: " << bulk_avg_size << ", "
		<< (bulk_total / bulk_executions.size()) << endl;

	cout << "Bulk[5] executions: " << bulk5_counter << endl;
}


/*
void vwap_stream2(stream* s, ofstream* results, ofstream* log, ofstream* stats,
	bool dump = false)
{
	struct timeval tvs, tve;

	double result = 0.0;

	#ifdef MEMORY
	// Space analysis.
	unsigned long tuple_counter = 0;
	unsigned long mem_sample_freq = 100;		// Sample every 100 tuples.
	#endif

	#ifdef BULK
	// Bulk execution.
	list< tuple<double, double> > bids_batch;
	DBT_HASH_SET<int> bulk_order_ids;
	unsigned long batch_threshold = 10;

	vector< tuple<double, double> > bids_batch5(5);
	DBT_HASH_SET<int> bulk5_order_ids;
	unsigned long bid_counter;

	bool bulk = false;
	bool varying = false;
	#endif

	gettimeofday(&tvs, NULL);

	while ( s->stream_has_inputs() )
	{
		#ifdef MEMORY
		++tuple_counter;
		if ( (tuple_counter % mem_sample_freq) == 0 )
			analyse_memory_usage(stats, tuple_counter / mem_sample_freq);
		#endif

		stream_tuple in = s->next_input();

		int order_id = get<1>(in);
		string action = get<2>(in);

		double price = get<4>(in);
		double volume = get<3>(in);

		#ifdef LOG_INPUT
		cout << "Tuple: order " << order_id << ", " << action
			<< " t=" << get<0>(in) << " p=" << get<4>(in)
			<< " v=" << get<3>(in) << endl;
		#endif

		if (action == "B")
		{
			// Insert bids

			#ifdef BULK
			tuple<double, double> pv = make_tuple(price, volume);
			if ( bulk && varying )
			{
				bids_batch.push_back(pv);
				if ( bids_batch.size() == batch_threshold ) {
					result = bulk_update_vwap_varying2(
						bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);
					bids_batch.clear();
					bulk_order_ids.clear();
				}
				else {
					bulk_order_ids.insert(order_id);
				}
			}
			else if ( bulk )
			{
				if ( (bid_counter % 5) == 0 )
				{
					result = bulk_update_vwap_5(
						bids_batch5, stv_bids, sv1_pbids, spv_bids, cvc_bids);

					bids_batch5.clear();
					bulk5_order_ids.clear();

					bids_batch5.reserve(5);
					bids_batch5[0] = pv;
					++bid_counter;
				}
				else {
					bids_batch5[bid_counter] = pv;
					++bid_counter;
				}

				bulk5_order_ids.insert(order_id);
			}
			else {
			#endif

				result = new_update_vwap2(price, volume);

			#ifdef BULK
			}
			#endif
		}

		else if (action == "S")
		{
			// Insert asks
			result = new_update_vwap2(price, volume);
		}

		else if (action == "J")
		{
			// Partial execution based from bid order book.

			// HACK -- special file format for EB
			// -- price = get<1>(in)
			// -- old volume = get<3>(in);
			// -- new volume = get<4>(in);

			price =  get<1>(in);
			volume =  get<3>(in);
			double new_volume = get<4>(in);

			// For now, we handle updates as delete, insert

			#ifdef BULK
			tuple<double, double> old_pv = make_tuple(price, volume);
			tuple<double, double> new_pv = make_tuple(price, new_volume);

			if ( bulk && varying )
			{
				// Apply the batch and the delete partial order.
				list<tuple<double, double> >::iterator bulk_bid_found =
					find(bids_batch.begin(), bids_batch.end(), old_pv);

				result = bulk_update_vwap_varying2(
					bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

				bids_batch.clear();
				bulk_order_ids.clear();
			}
			else if ( bulk )
			{
				// Apply the batch and the delete partial order.
				// Serially execute the batch given there may be less than 5 orders.
				for (unsigned int i = 0; i < bids_batch5.size(); ++i)
				{
					result = new_update_vwap2(
						get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));

				}

				bids_batch5.clear();
				bids_batch5.reserve(5);
				bulk5_order_ids.clear();
			}
			#endif

			result = new_update_vwap2(price, volume, false);

			if ( new_volume > 0 )
			{
				result = new_update_vwap2(price, new_volume);
			}
		}

		else if ( action == "K" )
		{
			// Partial execution based from bid order book.

			// HACK -- special file format for EB
			// -- price = get<1>(in)
			// -- old volume = get<3>(in);
			// -- new volume = get<4>(in);

			price =  get<1>(in);
			volume =  get<3>(in);
			double new_volume = get<4>(in);

			// For now, we handle updates as delete, insert

			result = new_update_vwap2(price, volume, false);

			if ( new_volume > 0 )
			{
				result = new_update_vwap2(price, new_volume);
			}
		}

		else if (action == "H")
		{
			// Order executed in full

			#ifdef BULK
			if ( bulk && varying )
			{
				result = bulk_update_vwap_varying2(
					bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

				bids_batch.clear();
				bulk_order_ids.clear();
			}
			else if ( bulk )
			{
				// Serially execute the batch.
				for (unsigned int i = 0; i < bids_batch5.size(); ++i)
				{
					result = new_update_vwap2(
						get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));
				}

				bids_batch5.clear();
				bids_batch5.reserve(5);
				bulk5_order_ids.clear();
			}
			#endif

			result = new_update_vwap2(price, volume, false);
		}
		else if (action == "I")
		{
			// Order executed in full
			result = new_update_vwap2(price, volume, false);
		}

		else if (action == "A")
		{
			// Delete from bid order book.

			#ifdef BULK
			if ( bulk && varying )
			{
				result = bulk_update_vwap_varying2(
					bids_batch, stv_bids, sv1_pbids, spv_bids, cvc_bids);

				bids_batch.clear();
				bulk_order_ids.clear();
			}
			else if ( bulk )
			{
				// Serially execute the batch.
				for (unsigned int i = 0; i < bids_batch5.size(); ++i)
				{
					result = new_update_vwap2(
						get<0>(bids_batch5[i]), get<1>(bids_batch5[i]));
				}

				bids_batch5.clear();
				bids_batch5.reserve(5);
				bulk5_order_ids.clear();
			}
			#endif

			result = new_update_vwap2(price, volume, false);
		}
		else if (action == "G")
		{
			// Delete from sell order book.
			result = new_update_vwap2(price, volume, false);
		}

		**
		else if (action == "X") {
			// Ignore X for now.
		}
		else if (action == "C") {
			// Unclear for now...
		}
		else if (action == "T") {
			// Unclear for now...
		}
		**

		#ifdef OUTPUT
		(*results) << result << endl;
		#endif
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP toasted QP", log, false);

	double bulk_total = 0.0;
	double bulk_avg_size = 0.0;
	list<unsigned long>::iterator be_it = bulk_executions.begin();
	list<unsigned long>::iterator be_end = bulk_executions.end();

	for (double i = 1.0; be_it != be_end; ++be_it, ++i) {
		bulk_total += static_cast<double>(*be_it);
		bulk_avg_size += ((static_cast<double>(*be_it) - bulk_avg_size) / i);
	}

	cout << "Bulk executions: " << bulk_executions.size()
		<< " avg: " << bulk_avg_size << ", "
		<< (bulk_total / bulk_executions.size()) << endl;

	cout << "Bulk[5] executions: " << bulk5_counter << endl;
}
*/


void ub_test()
{
	map<tuple<int, int>, int> test_map;

	cout << (test_map.begin() == test_map.end()) << endl;

	for (int i = 0; i < 10; ++i) {
		test_map[make_tuple(i, i)] = i;
		test_map[make_tuple(i, i+1)] = i;
		test_map[make_tuple(i, i-1)] = i;
	}

	map<tuple<int, int>, int>::iterator lb =
		test_map.upper_bound(make_tuple(5,2));

	cout << get<0>(lb->first) << ", " << get<1>(lb->first) << endl;

	map<tuple<int, int>, int>::iterator bbegin = test_map.begin();
	--bbegin;

	cout << (bbegin != test_map.end()) << endl;

}


void print_usage()
{
	cerr << "Usage: vwap <app mode> <data dir> <query freq>"
		<< "<in file> <out file> <result file> <stats file> [matviews file]" << endl;
}

int main(int argc, char* argv[])
{
	//ub_test();

	if ( argc < 8 ) {
		print_usage();
		exit(1);
	}

	string app_mode(argv[1]);
	string directory(argv[2]);
	long query_freq = atol(argv[3]);
	string input_file_name(argv[4]);
	string log_file_name(argv[5]);
	string results_file_name(argv[6]);
	string stats_file_name(argv[7]);
	string script_file_name = argc > 8? argv[8] : "";
	long copy_freq = argc > 9? atol(argv[9]) : 1500000;

	ofstream* log = new ofstream(log_file_name.c_str());
	ofstream* out = new ofstream(results_file_name.c_str());
	ofstream* stats = new ofstream(stats_file_name.c_str());

	file_stream f(input_file_name, 15000000);
	f.init_stream();
	cout << "Initialized input stream..." << endl;

	if ( app_mode == "toasted" )
		vwap_stream(&f, out, log, stats);

	else if ( app_mode == "ecpg" )
		vwap_snapshot_ecpg(directory, query_freq, &f, out, log);

	else if ( app_mode == "matviews" ) {
		assert ( argc > 9 );
		vwap_matviews(script_file_name, directory, query_freq, copy_freq, &f, out, log);
	}

	else if ( app_mode == "triggers" )
		vwap_triggers(script_file_name, directory, query_freq, &f, out, log);

	else {
		bool single_shot = true;
		vwap_snapshot(&f, out, log, query_freq, single_shot);
	}

	log->flush();
	out->flush();
	stats->flush();

	log->close();
	out->close();
	stats->close();

    return 0;
}
