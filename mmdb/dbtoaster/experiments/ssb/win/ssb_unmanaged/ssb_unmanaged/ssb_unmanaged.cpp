// ssb_unmanaged.cpp : Defines the entry point for the console application.
//

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <vector>

#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include <time.h>
#include <windows.h>
#include <psapi.h>

#include <boost/any.hpp>
#include <boost/function.hpp>

#include "tpch.h"

using namespace boost;

#define PENNIES 100
#define MEMORY 1

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
  #define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
  #define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif
 
struct timezone 
{
  int  tz_minuteswest; /* minutes W of Greenwich */
  int  tz_dsttime;     /* type of dst correction */
};
 
int gettimeofday(struct timeval *tv, struct timezone *tz)
{
  FILETIME ft;
  unsigned __int64 tmpres = 0;
  static int tzflag;
 
  if (NULL != tv)
  {
    GetSystemTimeAsFileTime(&ft);
 
    tmpres |= ft.dwHighDateTime;
    tmpres <<= 32;
    tmpres |= ft.dwLowDateTime;
 
    /*converting file time to unix epoch*/
    tmpres /= 10;  /*convert into microseconds*/
    tmpres -= DELTA_EPOCH_IN_MICROSECS; 
    tv->tv_sec = (long)(tmpres / 1000000UL);
    tv->tv_usec = (long)(tmpres % 1000000UL);
  }
 
  if (NULL != tz)
  {
    if (!tzflag)
    {
      _tzset();
      tzflag++;
    }
    tz->tz_minuteswest = _timezone / 60;
    tz->tz_dsttime = _daylight;
  }
 
  return 0;
}

inline void print_result(
	long sec, long usec, const char *text,
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

        if ( log != NULL ) {
        	(*log) << duration << "," << text << endl;
    	}

        //usleep(200000);
}


void get_memory_usage(DWORD processId)
{
	// Open current process
	HANDLE hProcess = ::OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, processId);
	if(hProcess)
	{
		PROCESS_MEMORY_COUNTERS ProcessMemoryCounters;

		memset(&ProcessMemoryCounters, 0, sizeof(ProcessMemoryCounters));

		// Set size of structure
		ProcessMemoryCounters.cb = sizeof(ProcessMemoryCounters);

		// Get memory usage
		if(::GetProcessMemoryInfo(hProcess,
								  &ProcessMemoryCounters,
								  sizeof(ProcessMemoryCounters))
			== TRUE)
		{
			std::cout << std::setfill('0') << std::hex
                << "PageFaultCount: 0x" << std::setw(8)
                << ProcessMemoryCounters.PageFaultCount << std::endl
                << "PeakWorkingSetSize: 0x" << std::setw(8)
                << ProcessMemoryCounters.PeakWorkingSetSize << std::endl
                << "WorkingSetSize: 0x" << std::setw(8)
                << ProcessMemoryCounters.WorkingSetSize << std::endl
                << "QuotaPeakPagedPoolUsage: 0x" << std::setw(8)
                << ProcessMemoryCounters.QuotaPeakPagedPoolUsage << std::endl
                << "QuotaPagedPoolUsage: 0x" << std::setw(8)
                << ProcessMemoryCounters.QuotaPagedPoolUsage << std::endl
                << "QuotaPeakNonPagedPoolUsage: 0x" << std::setw(8)
                << ProcessMemoryCounters.QuotaPeakNonPagedPoolUsage << std::endl
                << "QuotaNonPagedPoolUsage: 0x" << std::setw(8)
                << ProcessMemoryCounters.QuotaNonPagedPoolUsage << std::endl
                << "PagefileUsage: 0x" << std::setw(8)
                << ProcessMemoryCounters.PagefileUsage << std::endl
                << "PeakPagefileUsage: 0x" << std::setw(8)
				<< ProcessMemoryCounters.PeakPagefileUsage << std::dec << std::endl;
		}
		else {
			std::cout << "Could not get memory usage (Error: "
				<< ::GetLastError() << ")" << std::endl;
		}

		// Close process
		::CloseHandle(hProcess);
	}
	else {
		std::cout << "Could not open process (Error "
			<< ::GetLastError() << ")" << std::endl;
	}
}

//
//
// Field parsers

inline void parse_lineitem_field(
	int field, unsigned long line, lineitem& r, char* data)
{
	//cout << "Parse: " << string(data) << endl;

	switch(field) {
	case 0:
		r.orderkey = _atoi64(data);
		break;
	case 1:
		r.partkey = _atoi64(data);
		break;
	case 2:
		r.suppkey = _atoi64(data);
		break;
	case 3:
		r.linenumber = atoi(data);
		break;
	case 4:
		r.quantity = atof(data);
		break;
	case 5:
		r.extendedprice = atof(data);
		break;
	case 6:
		r.discount = atof(data);
		break;
	case 7:
		r.tax = atof(data);
		break;
	case 8:
		r.returnflag = string(data);
		break;
	case 9:
		r.linestatus = string(data);
		break;
	case 10:
		r.shipdate = date(data);
		break;
	case 11:
		r.commitdate = date(data);
		break;
	case 12:
		r.receiptdate = date(data);
		break;
	case 13:
		r.shipinstruct = string(data);
		break;
	case 14:
		r.shipmode = string(data);
		break;
	case 15:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid lineitem field id " << field
			<< " at line " << line << endl;
		break;
	}

	//cout << "Parsed " << r.as_string() << endl;
}

inline void parse_order_field(int field, unsigned long line, order& r, char* data)
{
	switch(field) {
	case 0:
		r.orderkey = _atoi64(data);
		break;
	case 1:
		r.custkey = _atoi64(data);
		break;
	case 2:
		r.orderstatus = string(data);
		break;
	case 3:
		r.totalprice = atof(data);
		break;
	case 4:
		r.orderdate = date(data);
		break;
	case 5:
		r.orderpriority = string(data);
		break;
	case 6:
		r.clerk = string(data);
		break;
	case 7:
		r.shippriority = atoi(data);
		break;
	case 8:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}

inline void parse_part_field(int field, unsigned long line, part& r, char* data)
{
	switch(field) {
	case 0:
		r.partkey = _atoi64(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.mfgr = string(data);
		break;
	case 3:
		r.brand= string(data);
		break;
	case 4:
		r.type = string(data);
		break;
	case 5:
		r.size = atoi(data);
		break;
	case 6:
		r.container = string(data);
		break;
	case 7:
		r.retailprice = atof(data);
		break;
	case 8:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}

inline void parse_customer_field(int field, unsigned long line, customer& r, char* data)
{
	switch(field) {
	case 0:
		r.custkey = _atoi64(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.address = string(data);
		break;
	case 3:
		r.nationkey = _atoi64(data);
		break;
	case 4:
		r.phone = string(data);
		break;
	case 5:
		r.acctbal = atof(data);
		break;
	case 6:
		r.mktsegment = string(data);
		break;
	case 7:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}

inline void parse_supplier_field(int field, unsigned long line, supplier& r, char* data)
{
	switch(field) {
	case 0:
		r.suppkey = _atoi64(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.address = string(data);
		break;
	case 3:
		r.nationkey = _atoi64(data);
		break;
	case 4:
		r.phone = string(data);
		break;
	case 5:
		r.acctbal = atof(data);
		break;
	case 6:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}

inline void parse_nation_field(int field, unsigned long line, nation& r, char* data)
{
	switch(field) {
	case 0:
		r.nationkey = _atoi64(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.regionkey = _atoi64(data);
		break;
	case 3:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}

inline void parse_region_field(int field, unsigned long line, region& r, char* data)
{
	switch(field) {
	case 0:
		r.regionkey = _atoi64(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.comment = string(data);
		break;
	default:
		cerr << "Invalid order field id " << field
			<< " at line " << line << endl;
		break;
	}
}


//
//
// Generic file I/O

typedef struct {
	int type;
	boost::any data;
} stream_tuple;

struct stream {
	virtual bool stream_has_inputs() = 0;
	virtual stream_tuple next_input() = 0;
	virtual unsigned int get_buffer_size() = 0;
};

template<typename T>
struct file_stream : public stream
{
	typedef boost::function<void (int, int, T&, char*)> parse_field_fn;
	typedef list<T> stream_buffer;

	parse_field_fn field_parser;

	string file_name;
	ifstream* input_file;
	stream_buffer buffer;
	unsigned long line;

	static const char delimiter = '|';

	int field_count;

	unsigned int buffer_count;
	unsigned int buffer_size;
	unsigned long threshold;
	bool finished_reading;

	file_stream(string fn, parse_field_fn parse_fn, int fields, unsigned int c)
		: field_parser(parse_fn), file_name(fn),
		  line(0), field_count(fields), buffer_count(c),
		  buffer_size(0), finished_reading(false)
	{
		assert ( field_count > 0 );

		input_file = new ifstream(file_name.c_str());
		if ( !(input_file->good()) )
			cerr << "Failed to open file " << file_name << endl;

		threshold =
			static_cast<long>(ceil(static_cast<double>(c) / 10));
	}

	~file_stream() {
		input_file->close();
		delete input_file;
	}

	tuple<bool, T> read_tuple()
	{
		char buf[512];
		input_file->getline(buf, 512);
		++line;

		char* start = &(buf[0]);
		char* end = start;

		//cout << "Data: " << string(buf) << endl;

		T r;
		for (int i = 0; i < field_count; ++i)
		{
			while ( *end && *end != delimiter ) ++end;
			if ( start == end ) {
				cerr << "Invalid field " << file_name
					<< ": " << line << " " << i << endl;
				return make_tuple(false, T());
			}
			if ( *end == '\0' && i != (field_count - 1) ) {
				cerr << "Invalid field " << file_name
					<< ": " << line << " " << i << endl;
				return make_tuple(false, T());
			}
			*end = '\0';
			field_parser(i, line, r, start);
			start = ++end;
		}

		//cout << "Read: " << r.as_string() << endl;
		if ( (line % 50000) == 0 ) {
			cout << "Read " << line << " tuples from " << file_name << endl;
		}

		return make_tuple(true, r);
	}

	void buffer_stream()
	{
		buffer_size = buffer.size();
		while ( buffer_size < buffer_count && input_file->good() )
		{
			tuple<bool, T> valid_input = read_tuple();
			if ( !get<0>(valid_input) )
				break;

			buffer.push_back(get<1>(valid_input));
			++buffer_size;
		}

		if ( !input_file->good() ) finished_reading = true;
	}

	void init_stream() { buffer_stream(); }

	bool stream_has_inputs() {
		return !buffer.empty();
	}

	int get_tuple_type(T& next)
	{
		int r;
		if ( typeid(next) == typeid(lineitem) )
			r = 0;
		else if ( typeid(next) == typeid(order) )
			r = 1;
		else if ( typeid(next) == typeid(part) )
			r = 2;
		else if ( typeid(next) == typeid(customer) )
			r = 3;
		else if ( typeid(next) == typeid(supplier) )
			r = 4;
		else if ( typeid(next) == typeid(nation) )
			r = 5;
		else if ( typeid(next) == typeid(region) )
			r = 6;
		else {
			r = -1;
		}

		return r;
	}

	stream_tuple next_input()
	{
		if ( !finished_reading && buffer_size < threshold )
			buffer_stream();

		T next = buffer.front();
		stream_tuple r;
		r.type = get_tuple_type(next);
		r.data = next;
		buffer.pop_front();
		--buffer_size;
		return r;
	}

	unsigned int get_buffer_size() { return buffer_size; }
};

//
//
// File I/O typedefs

typedef file_stream<lineitem>  lineitem_stream;
typedef file_stream<order>     order_stream;
typedef file_stream<part>      part_stream;
typedef file_stream<customer>  customer_stream;
typedef file_stream<supplier>  supplier_stream;
typedef file_stream<nation>    nation_stream;
typedef file_stream<region>    region_stream;

struct multiplexer : public stream
{
	vector<stream*> inputs;
	int num_streams;

	/*
	typedef boost::lagged_fibonacci607 rng_type;
	typedef boost::uniform_int<> rng_dist;
	typedef boost::variate_generator<rng_type&, rng_dist> rng;

	rng_type rng_t;
	rng_dist dist;
	rng generator;
	*/

	multiplexer(vector<stream*>& ins, int seed)
		: inputs(ins), num_streams(ins.size())
		  //rng_t(seed), dist(0, ins.size() -1), generator(rng_t, dist)
	{}


	bool stream_has_inputs()
	{
		return !(inputs.empty());
	}

	stream_tuple next_input()
	{
		int next_stream = (int) (num_streams * (rand() / (RAND_MAX + 1.0)));
		stream_tuple r = inputs[next_stream]->next_input();
		if ( !inputs[next_stream]->stream_has_inputs() )
		{
			cout << "Done with stream " << next_stream << endl;
			inputs.erase(inputs.begin()+next_stream);
			--num_streams;

		}

		return r;
	}

	unsigned int get_buffer_size()
	{
		vector<stream*>::iterator ins_it = inputs.begin();
		vector<stream*>::iterator ins_end = inputs.end();

		unsigned int r = 0;
		for (; ins_it != ins_end; ++ins_it)
			r += (*ins_it)->get_buffer_size();

		return r;
	}
};

////////////////////////////
//
// Query results

typedef unordered_map<date, unordered_map<string, double> > query_results;
query_results qr;

void print_query_results(query_results& qr, ofstream* results)
{
	query_results::iterator qr_it = qr.begin();
	query_results::iterator qr_end = qr.end();

	for (; qr_it != qr_end; ++qr_it)
	{
		unordered_map<string, double>::iterator nt_it = qr_it->second.begin();
		unordered_map<string, double>::iterator nt_end = qr_it->second.end();

		for (; nt_it != nt_end; ++nt_it)
		{
			(*results) << qr_it->first
				<< "," << nt_it->first << "," << nt_it->second << endl;
		}
	}
}

///////////////////////////////////////
//
// Snapshot processing.

typedef unordered_map<identifier, unordered_map<int, lineitem> > t_lineitem;
typedef unordered_map<identifier, order> t_orders;
typedef unordered_map<identifier, part> t_part;
typedef unordered_map<identifier, customer> t_customer;
typedef unordered_map<identifier, supplier> t_supplier;
typedef unordered_map<identifier, nation> t_nations;
typedef unordered_map<identifier, region> t_regions;
typedef unordered_map<date, string> t_date;

t_lineitem t_li;
t_orders t_ord;
t_part t_pt;
t_customer t_cs;
t_supplier t_sup;
t_nations t_nt;
t_regions t_rg;
t_date t_odyr;

void snapshot_query(ofstream* log, query_results& qr)
{
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	t_customer t_cs_pruned;
	t_supplier t_sup_pruned;
	t_part t_pt_pruned;

	// Filter customers and suppliers by region, and parts by manufacturer
	t_customer::iterator cs_it = t_cs.begin();
	t_customer::iterator cs_end = t_cs.end();

	for (; cs_it != cs_end; ++cs_it)
	{
		identifier cs_nt = cs_it->second.nationkey;
		t_nations::iterator nt_found = t_nt.find(cs_nt);
		if ( nt_found != t_nt.end() )
		{
			t_regions::iterator rg_found =
				t_rg.find(nt_found->second.regionkey);

			if ( rg_found != t_rg.end() &&
					rg_found->second.name == "AMERICA")
			{
				t_cs_pruned[cs_it->first] = cs_it->second;
			}
		}
	}

	t_supplier::iterator sup_it = t_sup.begin();
	t_supplier::iterator sup_end = t_sup.end();

	for (; sup_it != sup_end; ++sup_it)
	{
		identifier sup_nt = sup_it->second.nationkey;
		t_nations::iterator nt_found = t_nt.find(sup_nt);
		if ( nt_found != t_nt.end() )
		{
			t_regions::iterator rg_found =
				t_rg.find(nt_found->second.regionkey);

			if ( rg_found != t_rg.end() &&
					rg_found->second.name == "AMERICA")
			{
				t_sup_pruned[sup_it->first] = sup_it->second;
			}
		}
	}

	t_part::iterator pt_it = t_pt.begin();
	t_part::iterator pt_end = t_pt.end();

	for (; pt_it != pt_end; ++pt_it)
	{
		if ( (pt_it->second.mfgr == "Manufacturer#1") ||
				(pt_it->second.mfgr == "Manufacturer#2") )
		{
			t_pt_pruned[pt_it->first] = pt_it->second;
		}
	}

	// Loop over lineitem indexing into relations.
	t_lineitem::iterator li_it = t_li.begin();
	t_lineitem::iterator li_end = t_li.end();

	qr.clear();

	for (; li_it != li_end; ++li_it)
	{
		unordered_map<int, lineitem>::iterator line_it = li_it->second.begin();
		unordered_map<int, lineitem>::iterator line_end = li_it->second.end();

		for (; line_it != line_end; ++line_it)
		{
			lineitem& li = line_it->second;

			t_orders::iterator ord_found = t_ord.find(li.orderkey);

			if ( ord_found != t_ord.end() )
			{
				order& ord = ord_found->second;

				t_customer::iterator cs_found = t_cs_pruned.find(ord.custkey);
				t_supplier::iterator sup_found = t_sup_pruned.find(li.suppkey);
				t_part::iterator pt_found = t_pt_pruned.find(li.partkey);
				t_date::iterator date_found = t_odyr.find(ord.orderdate);

				if ( !((cs_found == t_cs_pruned.end()) ||
						(sup_found == t_sup_pruned.end()) ||
						(pt_found == t_pt_pruned.end()) ||
						(date_found == t_odyr.end())) )
				{
					t_nations::iterator cs_nt_found = t_nt.find(cs_found->second.nationkey);
					assert ( cs_nt_found != t_nt.end() );

					string d_year = date_found->second;
					string c_nation = cs_nt_found->second.name;

					double revenue = (li.extendedprice*(100-li.discount))/PENNIES;

					double rprice = 90000;
					rprice += (li.partkey/10) % 20001;        /* limit contribution to $200 */
					rprice += (li.partkey % 1000) * 100;
					double supplycost = 6*rprice/10;

					qr[d_year][c_nation] += (revenue - supplycost);
				}
			}
		}
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"TPCH QP iter", log, false);

}

inline void delta_snapshot_lineitem(boost::any& data)
{
	lineitem& li = boost::any_cast<lineitem&>(data);
	t_li[li.orderkey][li.linenumber] = li;

	#ifdef DEBUG
	cout << "Tuple li: " << li.as_string() << endl;
	#endif
}

inline void delta_snapshot_order(boost::any& data)
{
	order& ord = boost::any_cast<order&>(data);
	t_ord[ord.orderkey] = ord;

	// Build up date->year
	string year;
	size_t dash_pos = ord.orderdate.find_first_of('-');
	if ( dash_pos != string::npos )
		year = ord.orderdate.substr(0, dash_pos);
	else
		year = "";

	t_odyr[ord.orderdate] = year;

	#ifdef DEBUG
	cout << "Tuple ord: " << ord.as_string() << endl;)
	#endif
}

inline void delta_snapshot_part(boost::any& data)
{
	part& pt = boost::any_cast<part&>(data);
	t_pt[pt.partkey] = pt;

	#ifdef DEBUG
	cout << "Tuple pt: " << pt.as_string() << endl;
	#endif
}

inline void delta_snapshot_customer(boost::any& data)
{
	customer& cs = boost::any_cast<customer&>(data);
	t_cs[cs.custkey] = cs;

	#ifdef DEBUG
	cout << "Tuple cs: " << cs.as_string() << endl;
	#endif
}

inline void delta_snapshot_supplier(boost::any& data)
{
	supplier& sp = boost::any_cast<supplier&>(data);
	t_sup[sp.suppkey] = sp;

	#ifdef DEBUG
	cout << "Tuple sp: " << sp.as_string() << endl;
	#endif
}

inline void delta_snapshot_nation(boost::any& data)
{
	nation& nt = boost::any_cast<nation&>(data);
	t_nt[nt.nationkey] = nt;

	#ifdef DEBUG
	cout << "Tuple nt: " << nt.as_string() << endl;
	#endif
}

inline void delta_snapshot_region(boost::any& data)
{
	region& rg = boost::any_cast<region&>(data);
	t_rg[rg.regionkey] = rg;

	#ifdef DEBUG
	cout << "Tuple rg: " << rg.as_string() << endl;
	#endif
}


void ssb_snapshot(stream* s, ofstream* log, ofstream* results,
	long query_freq, bool single_shot = false)
{
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	long tuple_counter = 0;

	string year = "";

	while ( s->stream_has_inputs() )
	{
		stream_tuple in = s->next_input();

		try {
			switch (in.type) {
			case 0:
				delta_snapshot_lineitem(in.data);
				break;

			case 1:
				delta_snapshot_order(in.data);
				break;

			case 2:
				delta_snapshot_part(in.data);
				break;

			case 3:
				delta_snapshot_customer(in.data);
				break;

			case 4:
				delta_snapshot_supplier(in.data);
				break;

			case 5:
				delta_snapshot_nation(in.data);
				break;

			case 6:
				delta_snapshot_region(in.data);
				break;

			default:
				cerr << "Unknown tuple type " << in.type
					<< " (typeid '" << (in.data.type().name()) << "')" << endl;
				break;
			}
		}
		catch (const boost::bad_any_cast&)
		{
			cerr << "Failed to cast tuple data!" << endl;
		}

		++tuple_counter;

		if ( (tuple_counter % 10000) == 0 )
			cout << "Processed " << tuple_counter << " tuples." << endl;

		if ( !single_shot && (tuple_counter % query_freq) == 0 )
			snapshot_query(log, qr);
	}

	snapshot_query(log, qr);

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"TPCH QP", log, false);

	print_query_results(qr, results);
}

stream* load_snapshot_streams(string dir, string fileset)
{
	string li_file = dir + "/lineitem.tbl" + (fileset.empty()? "" : "." + fileset);
	string ord_file = dir + "/orders.tbl" + (fileset.empty()? "" : "." + fileset);
	string pt_file = dir + "/part.tbl" + (fileset.empty()? "" : "." + fileset);
	string cs_file = dir + "/customer.tbl" + (fileset.empty()? "" : "." + fileset);
	string sp_file = dir + "/supplier.tbl" + (fileset.empty()? "" : "." + fileset);
	string nt_file = dir + "/nation.tbl";
	string rg_file = dir + "/region.tbl";

	boost::function<void (int, unsigned long, lineitem&, char*)> li_parser = parse_lineitem_field;
	boost::function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;
	boost::function<void (int, unsigned long, part&, char*)> pt_parser = parse_part_field;
	boost::function<void (int, unsigned long, customer&, char*)> cs_parser = parse_customer_field;
	boost::function<void (int, unsigned long, supplier&, char*)> sp_parser = parse_supplier_field;
	boost::function<void (int, unsigned long, nation&, char*)> nt_parser = parse_nation_field;
	boost::function<void (int, unsigned long, region&, char*)> rg_parser = parse_region_field;

	lineitem_stream* li_s =
		new lineitem_stream(li_file, parse_lineitem_field, 16, 65000000);

	order_stream* ord_s =
		new order_stream(ord_file, parse_order_field, 9, 17000000);

	part_stream* pt_s =
		new part_stream(pt_file, parse_part_field, 9, 2100000);

	customer_stream* cs_s =
		new customer_stream(cs_file, parse_customer_field, 8, 1600000);

	supplier_stream* sp_s =
		new supplier_stream(sp_file, parse_supplier_field, 7, 110000);

	nation_stream* nt_s =
		new nation_stream(nt_file, parse_nation_field, 3, 100);

	region_stream* rg_s =
		new region_stream(rg_file, parse_region_field, 3, 100);

	li_s->init_stream();
	ord_s->init_stream();
	pt_s->init_stream();
	cs_s->init_stream();
	sp_s->init_stream();
	rg_s->init_stream();
	nt_s->init_stream();

	vector<stream*> inputs(7);
	inputs[0] = li_s; inputs[1] = ord_s;
	inputs[2] = pt_s; inputs[3] = cs_s;
	inputs[4] = sp_s;
	inputs[5] = nt_s; inputs[6] = rg_s;

	multiplexer* r = new multiplexer(inputs, 12345);
	return r;
}

///////////////////////////////////////
//
// Delta processing.

//
// Data structures
typedef unordered_map<date, date> orderdate_year;
typedef unordered_map<identifier, string> customer_nation;
typedef unordered_set<identifier> valid_suppliers;
typedef unordered_set<identifier> valid_parts;

orderdate_year odyr;
customer_nation csnt;
valid_suppliers sup;
valid_parts parts;

///////////
// Level 1

// 1. <d_yr, d_dk>
// d_dk ->
// c_nt ->
//   select c_nt, sum(lo_rev - lo_supc) from c,s,p,l
//   where ... and lo_od = d_dk
// ++ foreach c_nt in sum[d_dk]: d_yr, c_nt, sum[c_nt]

// 2. <c_nt, c_ck, c_rg>
// c_ck ->
// d_yr ->
//   select d_yr, sum(lo_rev - lo_supc) from d,s,p,l
//   where ... and lo_custkey = c_ck and c_rg = 'AMERICA'
// ++ if c_rg = 'AMERICA' then
//      foreach d_yr in sum[c_ck]: d_yr, c_nt, sum[d_yr]

// 3. <s_sk, s_rg>
// s_sk ->
// d_yr, c_nt ->
//   select d_yr, c_nt, sum(lo_rev - lo_supc) from d,c,p,l
//   where ... and lo_sk = s_ck and s_rg = 'AMERICA'
// ++ if s_rg = 'AMERICA' then
//      foreach d_yr, c_nt in sum[s_sk]: d_yr, c_nt, sum[d_yr, c_nt]

// 4. <p_pk, p_mfgr>
// p_pk ->
// d_yr, c_nt ->
//   select d_yr, c_nt, sum(lo_rev - lo_supc) from d,c,s,l
//   where ... and lo_pk = p_pk and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ if (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') then
//      foreach d_yr, c_nt in sum[p_pk]: d_yr, c_nt, sum[d_yr, c_nt]

// 5. <lo_ck, lo_sk, lo_pk, lo_od, lo_rev, lo_sc>
// lo_ck, lo_sk, lo_pk, lo_od ->
// d_dk -> {|d_yr|}
// c_ck -> {|c_nt|}
// s_sk -> count from s where s_rg = 'AMERICA'
// p_pk -> count from p where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ foreach d_yr in d_dk[lo_od]:
//      foreach c_nt in c_ck[lo_ck]:
//        d_yr, c_nt, (lo_rev - lo_supc)*(s_sk[lo_sk])*(p_pk[lo_pk])


inline void delta_l_l1(identifier& lo_ck, identifier& lo_sk,
	identifier& lo_pk, date& lo_od, double& lo_rev, double& lo_supc,
	query_results& running_results, ofstream* results)
{
	orderdate_year::iterator d_yr_found = odyr.find(lo_od);

	if ( d_yr_found == odyr.end() ) {
		cerr << "Failed to find year for d_dk " << lo_od << endl;
	}

	assert ( d_yr_found != odyr.end() );

	customer_nation::iterator c_nt_found = csnt.find(lo_ck);

	if ( c_nt_found != csnt.end() )
	{
		valid_suppliers::iterator sk_found = sup.find(lo_sk);
		valid_parts::iterator pk_found = parts.find(lo_pk);

		if ( !(sk_found == sup.end() || pk_found != parts.end()) )
		{
			date d_yr = d_yr_found->second;
			string c_nation = c_nt_found->second;

			running_results[d_yr][c_nation] += (lo_rev - lo_supc);

			(*results) << d_yr << "," << c_nation << ","
				<< running_results[d_yr][c_nation] << endl;
		}
	}
}

///////////
// Level 2

// 1/D.C <c_nt, c_ck, c_rg>
// c_ck ->
//   sum(lo_rev - lo_supc) from s, p, l
//   where ... and lo_ck = c_ck and c_rg = 'AMERICA'
// ++ if c_rg = 'AMERICA' then 1[c_nt] += sum[c_ck]

// 1/D.S <s_sk, s_rg>
// s_sk ->
// c_cnt ->
//   select c_nt, sum(lo_rev - lo_supc) from c,p,l
//   where ... and lo_sk = s_sk and s_rg = 'AMERICA'
// ++ if s_rg = 'AMERICA' then
//      foreach c_nt in sum[s_sk]: 1[c_nt] += sum[s_sk, c_nt]

// 1/D.P <p_pk, p_mfgr>
// p_pk ->
// c_nt ->
//   select c_nt, sum(lo_rev - lo_supc) from c,s,l
//   where ... and lo_pl = p_pk
// ++ if (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') then
//      foreach c_nt in sum[p_pk]: 1[c_nt] += sum[p_pk, c_nt]

// 1/D.L <lo_ck, lo_sk, lo_pk, lo_od>
// c_ck -> {|c_nt|}
// s_sk -> count from s where s_rg = 'AMERICA'
// p_pk -> count from p where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ foreach c_nt in c_nt[lo_ck]:
//    1[lo_od][c_nt] += (lo_rev - lo_supc)*(s_sk[lo_sk])*(p_pk[lo_pk])


//---------
// 2/C.D <d_yr, d_dk>
// d_dk ->
//   sum(lo_rev - lo_supc) from s, p, l
//   where ... and l_od = d_dk
// ++ 2[d_yr] += sum[d_dk]

// 2/C.S <s_sk, s_rg>
// s_sk ->
// d_yr ->
//   select d_yr, sum(lo_rev - lo_supc) from d, p, l
//   where ... and lo_sk = s_sk and s_rg = 'AMERICA'
// ++ if s_rg = 'AMERICA' then
//      foreach d_yr in sum[s_sk]: 2[d_yr] += sum[s_sk, d_yr]

// 2/C.P <p_pk, p_mfgr>
// p_pk ->
// d_yr ->
//   select d_yr, sum(lo_rev - lo_supc) from d, s, l
//    where ... and lo_pk = p_pk and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ if (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') then
//      foreach d_yr in sum[p_pk]: 2[d_yr] += sum[p_pk, d_yr]

// 2/C.L <lo_ck, lo_sk, lo_pk, lo_od>
// d_dk -> {|d_yr|}
// s_sk -> count from s where s_rg = 'AMERICA'
// p_pk -> count from s where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ foreach d_yr in d_dk[lo_od]:
//      2[lo_ck][d_yr] += (lo_rev - lo_supc)*(s_sk[lo_sk])*(p_pk[lo_pk])


//----------
// 3/S.D <d_yr, d_dk>
// d_yr ->
// c_nt ->
//   select c_nt, sum(lo_rev - lo_supc) from c, p, l
//   where ... and lo_od = d_dk
// ++ foreach c_nt in sum[d_yr]: 3[d_yr, c_nt] += sum[c_nt]

// 3/S.C <c_ck, c_nt, c_rg>
// c_nt ->
// d_yr ->
//    select d_yr, sum(lo_rev - lo_supc) from d, p, l
//    where ... and lo_ck = c_ck and c_rg = 'AMERICA'
// ++ if c_rg = 'AMERICA' then
//      foreach d_yr in sum[c_nt]: 3[d_yr, c_nt] += sum[d_yr]

// 3/S.P <p_pk, p_mfgr>
// d_yr, c_nt ->
//    select d_yr, c_nt, sum(lo_rev - lo_supc) from d, c, l
//    where ... and lo_pk = p_pk and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ if (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') then
//      foreach d_yr, c_nt in sum[]: 3[d_yr, c_nt] += sum[d_yr, c_nt]

// 3/S.L <lo_ck, lo_sk, lo_pk, lo_od>
// d_dk -> {|d_yr|}
// c_ck -> {|c_nt|}
// p_pk -> count from p where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
// ++ foreach d_yr in d_dk[lo_od]:
//      foreach c_nt in c_ck[lo_ck]:
//        3[lo_sk][d_yr, c_nt] += (lo_rev - lo_supc)*(p_pk[lo_pk])

//----------
// 4/P.D <d_yr, d_dk>
// d_yr ->
// c_nt ->
//    select c_nt, sum(lo_rev - lo_supc) from c, s, l
//    where ... and lo_od = d_dk
// ++ foreach c_nt in sum[d_yr]: 4[d_yr, c_nt] += sum[c_nt]

// 4/P.C <c_ck, c_nt, c_rg>
// c_nt ->
// d_yr ->
//    select d_yr, sum(lo_rev - lo_supc) from d, s, l
//    where ... and lo_ck = c_ck and c_rg = 'AMERICA'
// ++ if c_rg = 'AMERICA' then
//      foreach d_yr in sum[c_nt]: 4[d_yr, c_nt] += sum[d_yr]

// 4/P.S <s_sk, s_rg>
// d_yr, c_nt ->
//    select d_yr, c_nt, sum(lo_rev - lo_supc) from d, c, l
//    where ... and lo_sk = s_sk and s_rg = 'AMERICA'
// ++ if s_rg = 'AMERICA' then
//      foreach d_yr, c_nt in sum[]: 4[d_yr, c_nt] += sum[d_yr, c_nt]

// 4/P.L <lo_ck, lo_sk, lo_pk, lo_od>
// d_dk -> {|d_yr|}
// c_ck -> {|c_nt|}
// s_sk -> count from s where s_rg = 'AMERICA'
// ++ foreach d_yr in d_dk[lo_od]:
//      foreach c_nt in c_ck[lo_ck]:
//        4[lo_pk][d_yr, c_nt] += (lo_rev - lo_supc)*(s_sk[lo_sk])


//----------
// 5/L.D <d_dk, d_yr>
// ++ 5[d_dk] = d_yr::5[d_dk]
// -- TODO: where is this called from? Dates should be preloaded...
inline void delta_ld_l2(date& d_dk, string& d_yr)
{
	odyr[d_dk] = d_yr;
}

// 5/L.C <c_ck, c_nt>
// ++ 5[c_nt] = c_nt::5[c_ck]
inline void delta_lc_l2(identifier& c_ck, string& c_nt, string& c_rg)
{
	if ( c_rg == "AMERICA" ) {
		csnt[c_ck] = c_nt;
	}
}

// 5/L.S <s_sk, s_rg>
// ++ 5[s_sk] = (s_rg == 'AMERICA')
inline void delta_ls_l2(identifier& s_sk, string& s_rg)
{
	if ( s_rg == "AMERICA" )
		sup.insert(s_sk);
}

// 5/L.P <p_pk, p_mfgr>
// ++ 5[p_pk] = (p_mfgr == 'MFGR#1' or p_mfgr == 'MFGR#2')
inline void delta_lp_l2(identifier& p_pk, string& p_mfgr)
{
	if ( (p_mfgr == "MFGR#1") || (p_mfgr == "MFGR#2") )
		parts.insert(p_pk);
}


/////////////////
// Level 3


/////////////////
// Level 4


/////////////////
// Level 5


//////////////////////
//
// Data integration
typedef unordered_map<identifier, double> lineitem_totalprices;
typedef unordered_map<identifier, lineitem> lineitems_index;
typedef unordered_map<identifier, order> orders_index;
typedef unordered_map<identifier, tuple<identifier, string> > nations_index;
typedef unordered_map<identifier, string> regions_index;

lineitem_totalprices li_tp;
lineitems_index li_idx;
orders_index ord_idx;
nations_index nations;
regions_index regions;

/////////////////////
//
// Helpers

void print_nations()
{
	nations_index::iterator n_it = nations.begin();
	nations_index::iterator n_end = nations.end();

	for (; n_it != n_end; ++n_it) {
		cout << n_it->first << ", " << get<0>(n_it->second)
			<< ", " << get<1>(n_it->second) << endl;
	}
}

inline string get_nation(identifier& nk)
{
	nations_index::iterator nt_found = nations.find(nk);

	assert ( nt_found != nations.end() );
	return get<1>(nt_found->second);
}

inline string get_region(identifier& nk)
{
	nations_index::iterator nt_found = nations.find(nk);

	if ( nt_found == nations.end() ) {
		cerr << "Failed to find nation for " << nk << endl;
		print_nations();
	}
	assert ( nt_found != nations.end() );
	identifier rk = get<0>(nt_found->second);

	regions_index::iterator rg_found = regions.find(rk);
	assert ( rg_found != regions.end() );
	return rg_found->second;
}

inline tuple<bool, string> get_year(date& d)
{
	size_t dash_pos = d.find_first_of('-');
	if ( dash_pos != string::npos ) {
		return make_tuple(true, d.substr(0, dash_pos));
	}

	return make_tuple(false, "");
}

//////////////////////
//
// Top-level deltas

inline void delta_lineitem(boost::any& data, ofstream* results)
{
	lineitem* li = boost::any_cast<lineitem>(&data);

	#ifdef DEBUG
	cout << "Tuple li: " << li->as_string() << endl;
	#endif

	// select sum((ep*(100-dis)/PENNIES)*((100+tax)/PENNIES)) as total_price from L gb ok
	li_tp[li->orderkey] +=
		((li->extendedprice*(100-li->discount))/PENNIES) *
		((100+li->tax)/PENNIES);

	// select ... from {lineitem}, orders, TP where L.ok = O.ok and O.ok = TP.ok
	orders_index::iterator ord_found = ord_idx.find(li->orderkey);
	if ( ord_found != ord_idx.end() )
	{
		identifier ck = ord_found->second.custkey;
		date od = ord_found->second.orderdate;

		double rev = (li->extendedprice*(100-li->discount))/PENNIES;

		double rprice = 90000;
		rprice += (li->partkey/10) % 20001;        /* limit contribution to $200 */
        rprice += (li->partkey % 1000) * 100;
		double supc = 6*rprice/10;

		delta_l_l1(ck, li->suppkey, li->partkey, od, rev, supc, qr, results);
	}
	else {
		// Keep around for a future order...
		li_idx[li->orderkey] = *li;
	}
}

inline void delta_order(boost::any& data, ofstream* results)
{
	order* ord = boost::any_cast<order>(&data);

	#ifdef DEBUG
	cout << "Tuple ord: " << ord->as_string() << endl;)
	#endif

	lineitems_index::iterator li_found = li_idx.find(ord->orderkey);
	if ( li_found != li_idx.end() )
	{
		identifier sk = li_found->second.suppkey;
		identifier pk = li_found->second.partkey;

		double rev = (li_found->second.extendedprice*
			(100-li_found->second.discount))/PENNIES;

		double rprice = 90000;
		rprice += (li_found->second.partkey/10) % 20001;        /* limit contribution to $200 */
        rprice += (li_found->second.partkey % 1000) * 100;
		double supc = 6*rprice/10;

		delta_l_l1(ord->custkey, sk, pk, ord->orderdate, rev, supc, qr, results);
	}
	else {
		// Keep around for a future lineitem...
		ord_idx[ord->orderkey] = *ord;
	}
}

inline void delta_part(boost::any& data)
{
	part* pt = boost::any_cast<part>(&data);
	#ifdef DEBUG
	cout << "Tuple pt: " << pt->as_string() << endl;
	#endif

	delta_lp_l2(pt->partkey, pt->mfgr);
}

inline void delta_customer(boost::any& data)
{
	customer* cs = boost::any_cast<customer>(&data);

	#ifdef DEBUG
	cout << "Tuple cs: " << cs->as_string() << endl;
	#endif

	string nation = get_nation(cs->nationkey);
	string region = get_region(cs->nationkey);
	delta_lc_l2(cs->custkey, nation, region);
}

inline void delta_supplier(boost::any& data)
{
	supplier* sp = boost::any_cast<supplier>(&data);

	#ifdef DEBUG
	cout << "Tuple sp: " << sp->as_string() << endl;
	#endif

	string region = get_region(sp->nationkey);
	delta_ls_l2(sp->suppkey, region);
}

inline void delta_nation(boost::any& data)
{
	nation* nt = boost::any_cast<nation>(&data);

	#ifdef DEBUG
	cout << "Tuple nt: " << nt->as_string() << endl;
	#endif

	nations[nt->nationkey] = make_tuple(nt->regionkey, nt->name);
}

inline void delta_region(boost::any& data)
{
	region* rg = boost::any_cast<region>(&data);

	#ifdef DEBUG
	cout << "Tuple rg: " << rg->as_string() << endl;
	#endif

	regions[rg->regionkey] = rg->name;
}

void ssb_stream(stream* s, ofstream* log, ofstream* results, ofstream* stats)
{
	unsigned long tuple_counter = 0;

	double tup_sum = 0.0;

	struct timeval tvs, tve;
	gettimeofday(&tvs, NULL);

	while  ( s->stream_has_inputs() )
	{
		struct timeval tups, tupe;

		gettimeofday(&tups, NULL);

		++tuple_counter;

		stream_tuple in = s->next_input();

		try {
			switch (in.type) {
			case 0:
				delta_lineitem(in.data, results);
				break;

			case 1:
				delta_order(in.data, results);
				break;

			case 2:
				delta_part(in.data);
				break;

			case 3:
				delta_customer(in.data);
				break;

			case 4:
				delta_supplier(in.data);
				break;

			case 5:
				delta_nation(in.data);
				break;

			case 6:
				delta_region(in.data);
				break;

			default:
				cerr << "Unknown tuple type " << in.type
					<< " (typeid '" << (in.data.type().name()) << "')" << endl;
				break;
			}
		}
		catch (const boost::bad_any_cast&)
		{
			cerr << "Failed to cast tuple data!" << endl;
		}

		gettimeofday(&tupe, NULL);
		long sec = tupe.tv_sec - tups.tv_sec;
		long usec = tupe.tv_usec - tups.tv_usec;
		if ( usec < 0 ) { --sec; usec += 1000000; }
		tup_sum += (sec + (usec / 1000000.0));

		if ( (tuple_counter % 50000) == 0 ) {
				cout << "Processed " << tuple_counter << " tuples." << endl;
				cout << "Exec time " << (tup_sum / 50000.0) << endl;
				tup_sum = 0.0;
		}
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"TPCH toasted QP", log, false);
}

void preload_streams(string dir, string fileset)
{
	cout << "Preloading streams..." << endl;

	string ord_file = dir + "/orders.tbl" + (fileset.empty()? "" : "." + fileset);
	string nt_file = dir + "/nation.tbl";
	string rg_file = dir + "/region.tbl";

	boost::function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;
	boost::function<void (int, unsigned long, nation&, char*)> nt_parser = parse_nation_field;
	boost::function<void (int, unsigned long, region&, char*)> rg_parser = parse_region_field;

	// Build orderdate_year map
	order_stream* ord_s =
		new order_stream(ord_file, parse_order_field, 9, 100);

	ord_s->init_stream();

	while ( ord_s->stream_has_inputs() ) {
		stream_tuple next = ord_s->next_input();
		order& ord = boost::any_cast<order&>(next.data);
		tuple<bool, string> year_found = get_year(ord.orderdate);
		if ( get<0>(year_found) )
			delta_ld_l2(ord.orderdate, get<1>(year_found));
	}

	delete ord_s;

	// Build regions, nations
	nation_stream* nt_s =
		new nation_stream(nt_file, parse_nation_field, 4, 100);

	region_stream* rg_s =
		new region_stream(rg_file, parse_region_field, 3, 100);

	rg_s->init_stream();
	nt_s->init_stream();

	cout << rg_file << ": " << rg_s->get_buffer_size() << endl;
	cout << nt_file << ": " << nt_s->get_buffer_size() << endl;

	assert ( rg_s->get_buffer_size() > 0 && nt_s->get_buffer_size() > 0 );

	while ( rg_s->stream_has_inputs() ) {
		stream_tuple next = rg_s->next_input();
		delta_region(next.data);
	}

	while ( nt_s->stream_has_inputs() ) {
		stream_tuple next = nt_s->next_input();
		delta_nation(next.data);
	}

	delete rg_s;
	delete nt_s;

	// Preload customers, suppliers, parts.
	string pt_file = dir + "/part.tbl" + (fileset.empty()? "" : "." + fileset);
	string cs_file = dir + "/customer.tbl" + (fileset.empty()? "" : "." + fileset);
	string sp_file = dir + "/supplier.tbl" + (fileset.empty()? "" : "." + fileset);

	boost::function<void (int, unsigned long, part&, char*)> pt_parser = parse_part_field;
	boost::function<void (int, unsigned long, customer&, char*)> cs_parser = parse_customer_field;
	boost::function<void (int, unsigned long, supplier&, char*)> sp_parser = parse_supplier_field;

	part_stream* pt_s =
		new part_stream(pt_file, parse_part_field, 9, 2100000);

	customer_stream* cs_s =
		new customer_stream(cs_file, parse_customer_field, 8, 1600000);

	supplier_stream* sp_s =
		new supplier_stream(sp_file, parse_supplier_field, 7, 110000);

	pt_s->init_stream();
	cs_s->init_stream();
	sp_s->init_stream();

	cout << pt_file << ": " << pt_s->get_buffer_size() << endl;
	cout << cs_file << ": " << cs_s->get_buffer_size() << endl;
	cout << sp_file << ": " << sp_s->get_buffer_size() << endl;

	assert ( pt_s->get_buffer_size() > 0 &&
				cs_s->get_buffer_size() > 0 &&
				sp_s->get_buffer_size() > 0);

	while ( pt_s->stream_has_inputs() ) {
		stream_tuple next = pt_s->next_input();
		delta_part(next.data);
	}

	while ( cs_s->stream_has_inputs() ) {
		stream_tuple next = cs_s->next_input();
		delta_customer(next.data);
	}

	while ( sp_s->stream_has_inputs() ) {
		stream_tuple next = sp_s->next_input();
		delta_supplier(next.data);
	}

	delete pt_s;
	delete cs_s;
	delete sp_s;
}

stream* load_streams(string dir, string fileset)
{
	string li_file = dir + "/lineitem.tbl" + (fileset.empty()? "" : "." + fileset);
	//string ord_file = dir + "/orders.tbl" + (fileset.empty()? "" : "." + fileset);

	boost::function<void (int, unsigned long, lineitem&, char*)> li_parser = parse_lineitem_field;
	//boost::function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;

	lineitem_stream* li_s =
		new lineitem_stream(li_file, parse_lineitem_field, 16, 65000000);

	//order_stream* ord_s =
	//	new order_stream(ord_file, parse_order_field, 9, 17000000);

	li_s->init_stream();
	//ord_s->init_stream();

	// Get memory used by loading lineitem stream.
	get_memory_usage(::GetCurrentProcessId());

	// Preload date, nations, regions, customers, suppliers, parts
	preload_streams(dir, fileset);

	vector<stream*> inputs(1);
	inputs[0] = li_s;
	//inputs[1] = ord_s;
	//inputs[2] = pt_s; inputs[3] = cs_s;
	//inputs[4] = sp_s;
	//inputs[5] = nt_s; inputs[6] = rg_s;

	multiplexer* r = new multiplexer(inputs, 12345);
	return r;
}

void print_usage()
{
	cout << "Usage: ssb <app mode> <query frequency> "
		<< "<log file> <results file> <stats file> "
		<< "<tpc-h data directory> [tpc-h file set] [mv script file]" << endl;
}


int main(int argc, char* argv[])
{
	if ( argc < 7 ) {
		print_usage();
		exit(1);
	}

	string app_mode(argv[1]);
	long query_freq = atol(argv[2]);
	string log_file(argv[3]);
	string results_file(argv[4]);
	string stats_file(argv[5]);
	string directory(argv[6]);
	string fileset = argc > 7? string(argv[7]) : "";
	string mv_script_file = (fileset.empty()?
		(argc > 7? string(argv[7]) : "") : (argc > 8? string(argv[8]) : ""));

	ofstream* log_f = new ofstream(log_file.c_str());
	ofstream* results_f = new ofstream(results_file.c_str());
	ofstream* stats_f = new ofstream(stats_file.c_str());

	if ( app_mode == "toasted" ) {
		stream* multiplexed_stream = load_streams(directory, fileset);
		ssb_stream(multiplexed_stream, log_f, results_f, stats_f);
	}
	else
	{
		stream* multiplexed_stream = load_snapshot_streams(directory, fileset);
		ssb_snapshot(multiplexed_stream, log_f, results_f, query_freq);
	}

    return 0;
}

