#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <fstream>
#include <limits>
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

#include <boost/any.hpp>
#include <boost/function.hpp>

/*
#include <boost/random/lagged_fibonacci.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>
*/

#include "tpch.h"

using namespace boost;

exec sql include sqlca;

exec sql whenever sqlerror sqlprint;
exec sql whenever not found sqlprint;

#define PENNIES 100
#define MEMORY 1

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

        if ( db ) exec sql vacuum;

        if ( log != NULL ) {
        	(*log) << duration << "," << text << endl;
    	}

        //usleep(200000);
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
		r.orderkey = atoll(data);
		break;
	case 1:
		r.partkey = atoll(data);
		break;
	case 2:
		r.suppkey = atoll(data);
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
		r.orderkey = atoll(data);
		break;
	case 1:
		r.custkey = atoll(data);
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
		r.partkey = atoll(data);
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
		r.custkey = atoll(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.address = string(data);
		break;
	case 3:
		r.nationkey = atoll(data);
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
		r.suppkey = atoll(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.address = string(data);
		break;
	case 3:
		r.nationkey = atoll(data);
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
		r.nationkey = atoll(data);
		break;
	case 1:
		r.name = string(data);
		break;
	case 2:
		r.regionkey = atoll(data);
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
		r.regionkey = atoll(data);
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
	typedef function<void (int, int, T&, char*)> parse_field_fn;
	typedef list<T> stream_buffer;

	parse_field_fn field_parser;

	string file_name;
	ifstream* input_file;
	stream_buffer buffer;
	unsigned long line;

	static const char delimiter = '|';

	unsigned int field_count;

	unsigned int buffer_count;
	unsigned int buffer_size;
	unsigned long threshold;
	unsigned int line_size;
	bool finished_reading;

	file_stream(string fn, parse_field_fn parse_fn,
		unsigned int fields, unsigned int c, unsigned int ls)
		: field_parser(parse_fn), file_name(fn),
		  line(0), field_count(fields), buffer_count(c),
		  buffer_size(0), line_size(ls),
		  finished_reading(false)
	{
		assert ( field_count > 0 );
		assert ( line_size > 32 );

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
		char buf[line_size];
		input_file->getline(buf, line_size);
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


///////////////////////////////////////
//
// Postgres

typedef map<tuple<double, string>, double> ecpg_query_results;

bool check_table_sizes()
{
	exec sql begin declare section;
	int li_size, li_size_ind;
	int ord_size, ord_size_ind;
	int pt_size, pt_size_ind;
	int cs_size, cs_size_ind;
	int sup_size, sup_size_ind;
	int nt_size, nt_size_ind;
	int rg_size, rg_size_ind;
	exec sql end declare section;


	exec sql begin transaction;

	exec sql select count(*) into :li_size  :li_size_ind from lineitem;
	exec sql select count(*) into :ord_size :ord_size_ind from orders;
	exec sql select count(*) into :pt_size  :pt_size_ind from part;
	exec sql select count(*) into :cs_size  :cs_size_ind from customer;
	exec sql select count(*) into :sup_size :sup_size_ind from supplier;
	exec sql select count(*) into :nt_size  :nt_size_ind from nation;
	exec sql select count(*) into :rg_size  :rg_size_ind from region;

	exec sql commit;

	if ( (li_size_ind < 0) || (ord_size_ind < 0) ||
			(pt_size_ind < 0) || (cs_size_ind < 0) ||
			(sup_size_ind < 0) || (nt_size_ind < 0) ||
			(rg_size_ind < 0) )
	{
		cerr << "Found null count value for a table!" << endl;
		return false;
	}

	if ( (li_size == 0) || (ord_size == 0) ||
			(pt_size == 0) || (cs_size == 0) ||
			(sup_size == 0) || (nt_size == 0) ||
			(rg_size == 0) )
	{
		cerr << "Found empty table!" << endl;
		return false;
	}

	return true;
}

bool load_ssb_tables(ofstream* log, string directory, string fileset)
{
	bool valid = check_table_sizes();
	if ( !valid )
	{
		struct timeval tvs, tve;

		gettimeofday(&tvs, NULL);

		cout << "Loading TPC-H tables..." << endl;

		// Clear out all tables and start again.
		string drop_cmd = string() +
			"drop table if exists lineitem;" +
			"drop table if exists orders;" +
			"drop table if exists part;" +
			"drop table if exists customer;" +
			"drop table if exists supplier;" +
			"drop table if exists region;" +
			"drop table if exists nation;";


		string create_li = string() + "create table lineitem (" +
			"orderkey bigint, partkey bigint, suppkey bigint," +
			"linenumber integer," +
			"quantity decimal, extendedprice decimal," +
			"discount decimal, tax decimal," +
			"returnflag char(1), linestatus char(1)," +
			"shipdate date, commitdate date, receiptdate date," +
			"shipinstruct char(25), shipmode char(10)," +
			"comment varchar(44)," +
			"primary key (orderkey, linenumber)"+ ");";

		string create_ord = string() + "create table orders (" +
			"orderkey bigint, custkey bigint," +
			"orderstatus char(1), totalprice decimal," +
			"orderdate date, orderpriority char(15)," +
			"clerk char(15), shippriority integer," +
			"comment varchar(79)," +
			"primary key (orderkey)" + ");";

		string create_pt = string() + "create table part (" +
			"partkey bigint, name varchar(55)," +
			"mfgr char(25), brand char(10)," +
			"type varchar(25), size integer," +
			"container char(10), retailprice decimal," +
			"comment varchar(25)," +
			"primary key (partkey)" + ");";

		string create_cs = string() + "create table customer (" +
			"custkey bigint, name varchar(25)," +
			"address varchar(40), nationkey bigint," +
			"phone char(15), acctbal decimal," +
			"mktsegment char(10), comment varchar(117)," +
			"primary key (custkey)" + ");";

		string create_sup = string() + "create table supplier (" +
			"suppkey bigint, name char(25)," +
			"address varchar(40), nationkey bigint," +
			"phone char(15), acctbal decimal," +
			"comment varchar(101)," +
			"primary key (suppkey)" + ");";

		string create_rg = string() + "create table region (" +
			"regionkey bigint, name char(25), comment varchar(125)," +
			"primary key (regionkey)" + ");";

		string create_nt = string() + "create table nation (" +
			"nationkey bigint, name char(25)," +
			"regionkey bigint, comment varchar(152)," +
			"primary key (nationkey)" + ");";

		exec sql begin declare section;
		const char* drop_cmd_str = drop_cmd.c_str();
		const char* create_li_str = create_li.c_str();
		const char* create_ord_str = create_ord.c_str();
		const char* create_pt_str = create_pt.c_str();
		const char* create_cs_str = create_cs.c_str();
		const char* create_sup_str = create_sup.c_str();
		const char* create_rg_str = create_rg.c_str();
		const char* create_nt_str = create_nt.c_str();
		exec sql end declare section;


		exec sql execute immediate :drop_cmd_str;
		exec sql execute immediate :create_li_str;
		exec sql execute immediate :create_ord_str;
		exec sql execute immediate :create_pt_str;
		exec sql execute immediate :create_cs_str;
		exec sql execute immediate :create_sup_str;
		exec sql execute immediate :create_rg_str;
		exec sql execute immediate :create_nt_str;


		string copy_li = "copy lineitem from '" + directory +
			"/lineitem.tbl" + (fileset.empty()? "" : "." + fileset) + "'" +
			" with delimiter '|'";

		string copy_ord = "copy orders from '" + directory +
			"/orders.tbl" + (fileset.empty()? "" : "." + fileset) + "'" +
			" with delimiter '|'";

		string copy_pt = "copy part from '" + directory +
			"/part.tbl" + (fileset.empty()? "" : "." + fileset) + "'" +
			" with delimiter '|'";

		string copy_cs = "copy customer from '" + directory +
			"/customer.tbl" + (fileset.empty()? "" : "." + fileset) + "'" +
			" with delimiter '|'";

		string copy_sup = "copy supplier from '" + directory +
			"/supplier.tbl" + (fileset.empty()? "" : "." + fileset) + "'" +
			" with delimiter '|'";

		string copy_rg = "copy region (regionkey, name, comment) from '" + directory +
			"/region.tbl" + "'" + " with delimiter '|'";

		string copy_nt = "copy nation (nationkey, name, regionkey, comment) from '" + directory +
			"/nation.tbl" + "'" + " with delimiter '|'";


		exec sql begin declare section;
		const char* copy_li_str = copy_li.c_str();
		const char* copy_ord_str = copy_ord.c_str();
		const char* copy_pt_str = copy_pt.c_str();
		const char* copy_cs_str = copy_cs.c_str();
		const char* copy_sup_str = copy_sup.c_str();
		const char* copy_rg_str = copy_rg.c_str();
		const char* copy_nt_str = copy_nt.c_str();
		exec sql end declare section;

		exec sql execute immediate :copy_li_str;
		exec sql execute immediate :copy_ord_str;
		exec sql execute immediate :copy_pt_str;
		exec sql execute immediate :copy_cs_str;
		exec sql execute immediate :copy_sup_str;
		exec sql execute immediate :copy_rg_str;
		exec sql execute immediate :copy_nt_str;

		if ( !check_table_sizes() ) {
			// Give up.
			cerr << "Failed loading tables, please load manually!" << endl;
			return false;
		}

		gettimeofday(&tve, NULL);

		print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
			"SSB LOAD TPCH", log, false);
	}

	return true;
}

void ssb_query(ecpg_query_results& qr, ofstream* log)
{
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	// Query and result ecpg declarations.
	exec sql begin declare section;
	const char* cleanup_cmd_str;
	double yr, profit;
	VARCHAR nation[25];
	int yr_ind, profit_ind, nation_ind;
	exec sql end declare section;

	// Create temporary tables
	// -- note this is ugly since it's full recomputation
	// -- we should really try this with a DBMS with efficient matviews

	/*
	// Create indices for temporary tables
	// -- build cost doesn't justify speedup
	string indices = string() +
		"create index dk_h_idx on ssb_date using hash (datekey);" +
		"create index dk_b_idx on ssb_date using btree (datekey);" +
		"create index dy_h_idx on ssb_date using hash (year);" +
		"create index ck_h_idx on ssb_customer using hash (custkey);" +
		"create index cr_h_idx on ssb_customer using hash (region);" +
		"create index cn_h_idx on ssb_customer using hash (nation);" +
		"create index cr_b_idx on ssb_customer using btree (region);" +
		"create index ckr_b_idx on ssb_customer using btree (custkey, region);" +
		"create index sk_h_idx on ssb_supplier using hash (suppkey);" +
		"create index sr_h_idx on ssb_supplier using hash (region);" +
		"create index sr_b_idx on ssb_supplier using btree (region);" +
		"create index skr_b_idx on ssb_supplier using btree (suppkey, region);" +
		"create index pk_h_idx on ssb_part using hash (partkey);" +
		"create index pm_h_idx on ssb_part using hash (mfgr);" +
		"create index pm_b_idx on ssb_part using btree (mfgr);" +
		"create index pkm_b_idx on ssb_part using btree (partkey, mfgr);" +
		"create index lcspo_b_idx on ssb_lineorder " +
			"using btree (custkey, suppkey, partkey, orderdate);";
	*/

	string cleanup_cmd = string() +
		"drop table if exists ssb_date;" +
		"drop table if exists ssb_customer;" +
		"drop table if exists ssb_supplier;" +
		"drop table if exists ssb_part;" +
		"drop table if exists ssb_lineorder;" +
		"drop table if exists ssb_totalprices;";

	cleanup_cmd_str = cleanup_cmd.c_str();

	exec sql execute immediate :cleanup_cmd_str;

	exec sql begin transaction;

	exec sql select
			orderdate as datekey,
			extract(year from orderdate) as year
		into ssb_date
		from orders;

	exec sql select
			custkey, customer.name, address,
			(substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
			nation.name as nation,
			region.name as region,
			phone, mktsegment
		into ssb_customer
		from customer, nation, region
		where customer.nationkey = nation.nationkey
		and nation.regionkey = region.regionkey;

	exec sql select suppkey, supplier.name, address,
			(substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
			nation.name as nation,
			region.name as region,
			phone
		into ssb_supplier
		from supplier, nation, region
		where supplier.nationkey = nation.nationkey
		and nation.regionkey = region.regionkey;

	exec sql select partkey, name, mfgr,
			brand as category,
			(brand || (cast (round(random()*25) as text))) as brand1,
			substring(name from 0 for position(' ' in name)) as color,
			type, size, container
		into ssb_part
		from part;

	exec sql select orderkey,
			sum((extendedprice*(100-discount)*(100+tax))/100) as totalprice
		into ssb_totalprices
		from lineitem group by orderkey;

	exec sql select
			lineitem.orderkey, linenumber,
			custkey, partkey, suppkey,
			orderdate, orderpriority,
			shippriority, quantity, extendedprice,
			ssb_totalprices.totalprice as ordtotalprice, discount,
			(extendedprice * (100-discount)/100) as revenue,
			(90000 + ((partkey/10) % 20001) + ((partkey % 1000)*100)) as supplycost,
			tax, commitdate, shipmode
		into ssb_lineorder
		from lineitem, orders, ssb_totalprices
		where lineitem.orderkey = orders.orderkey
		and orders.orderkey = ssb_totalprices.orderkey;

	exec sql declare ssb_cursor cursor for
		select d.year, c.nation,
			sum(lo.revenue - lo.supplycost) as profit
		from
			ssb_date as d,
			ssb_customer as c,
			ssb_supplier as s,
			ssb_part as p,
			ssb_lineorder as lo
		where lo.custkey = c.custkey
		and lo.suppkey = s.suppkey
		and lo.partkey = p.partkey
		and lo.orderdate = d.datekey
		and c.region = 'AMERICA'
		and s.region = 'AMERICA'
		and (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2')
		group by d.year, c.nation;


	exec sql open ssb_cursor;

	do {
		exec sql fetch next from ssb_cursor into
			:yr :yr_ind, :nation :nation_ind, :profit :profit_ind;

		if ( sqlca.sqlcode == ECPG_NOT_FOUND )
			break;

		if ( yr_ind == 0 && nation_ind == 0 && profit_ind == 0 ) {
			qr[make_tuple(yr, nation.arr)] = profit;
		}
	}
	while ( yr_ind == 0 && nation_ind == 0 && profit_ind == 0 );

	exec sql close ssb_cursor;

	exec sql commit;

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"SSB PG iter", log, true);

}

void print_results(ecpg_query_results& qr, ofstream* results)
{
	ecpg_query_results::iterator qr_it = qr.begin();
	ecpg_query_results::iterator qr_end = qr.end();

	for (; qr_it != qr_end; ++qr_it)
	{
		(*results) << get<0>(qr_it->first) << ","
			<< get<1>(qr_it->first) << "," << qr_it->second << endl;
	}
}

void ssb_ecpg(ofstream* log, ofstream* results,
	string directory, string fileset, long query_freq,
	bool single_shot = true, stream* s = NULL)
{
	ecpg_query_results qr;

	// TODO: grow lineitem
	/*
	string lineitem_file_name = directory+"/copy_lineitem";
	ofstream* li_file = new ofstream(lineitem_file_name.c_str());

	string li_copy_stmt =
		"copy lineitem from '" + lineitem_file_name + "' with delimiter ','";
	*/

	exec sql connect to tpch;

	exec sql set autocommit to on;

	if ( !load_ssb_tables(log, directory, fileset) ) {
		cerr << "Failed to load ssb tables, aborting..." << endl;
		return;
	}

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	if ( single_shot )
	{
		ssb_query(qr, log);
		print_results(qr, results);
	}
	else
	{
		assert ( s );

		long tuple_counter = 0;

		while ( s->stream_has_inputs() )
		{
			++tuple_counter;

			stream_tuple next = s->next_input();

			// TODO: insert tuple...
			// -- assume tables are already loaded for now.

			if ( (tuple_counter % query_freq) == 0 ) {
				// Run queries after update.
				ssb_query(qr, log);
				print_results(qr, results);
				qr.clear();
			}
		}
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"SSB PG TPCH", log, true);

	exec sql set autocommit to off;

	exec sql disconnect;

}

////////////////////////////
//
// Query results

typedef unordered_map<double, unordered_map<string, double> > mv_query_results;
mv_query_results mvqr;

typedef unordered_map<date, unordered_map<string, double> > query_results;
query_results qr;

void print_mv_query_results(mv_query_results& qr, ofstream* results)
{
	mv_query_results::iterator qr_it = qr.begin();
	mv_query_results::iterator qr_end = qr.end();

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

/////////////////////////////////////////
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
		"SSB matview setup", log, false);
}

void matview_query(mv_query_results& mvqr, ofstream* log, string& query_type)
{
	exec sql begin declare section;
	const char* cleanup_cmd_str;
	double yr, profit;
	VARCHAR nation[25];
	int yr_ind, profit_ind, nation_ind;
	exec sql end declare section;

	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	exec sql declare ssb_mv_cursor cursor for
		select year, nation, profit from query_result;

	exec sql open ssb_mv_cursor;

	do {
		exec sql fetch next from ssb_mv_cursor into
			:yr :yr_ind, :nation :nation_ind, :profit :profit_ind;

		if ( sqlca.sqlcode == ECPG_NOT_FOUND )
			break;

		if ( yr_ind == 0 && nation_ind == 0 && profit_ind == 0 )
			mvqr[yr][string(nation.arr)] = profit;
	}
	while ( yr_ind == 0 && nation_ind == 0 && profit_ind == 0 );

	exec sql close ssb_mv_cursor;

	exec sql commit;

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		query_type.c_str(), log, true);

}

void ssb_matviews(string mv_script_file,
	string directory, string fileset, long query_freq,
	stream* s, ofstream* log, ofstream* results, ofstream* stats)
{
	exec sql connect to tpch;

	exec sql set autocommit to on;

	string mvqt = "SSB matview iter";
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	set_up_matviews_and_triggers(mv_script_file, log);

	assert ( s );

	long tuple_counter = 0;

	// Dummy loop to simulate refreshes.
	while ( s->stream_has_inputs() )
	{
		++tuple_counter;

		stream_tuple next = s->next_input();

		// TODO: insert tuple...
		// -- assume tables are already loaded for now.

		if ( (tuple_counter % query_freq) == 0 ) {
			// Run queries after update.
			matview_query(mvqr, log, mvqt);
			print_mv_query_results(mvqr, results);
			mvqr.clear();
		}
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"SSB matview", log, true);

	exec sql set autocommit to off;

	exec sql disconnect;

}

void ssb_triggers(string mv_script_file,
	string directory, string fileset, long query_freq,
	stream* s, ofstream* log, ofstream* results, ofstream* stats)
{
	exec sql connect to tpch;

	exec sql set autocommit to on;

	lineitem* li;
	order* ord;

	exec sql begin declare section;
	long long int orderkey;
	long long int partkey;
	long long int suppkey;
	int linenumber;
	double quantity;
	double extendedprice;
	double discount;
	double tax;
	const char* returnflag;
	const char* linestatus;
	const char* shipdate;
	const char* commitdate;
	const char* receiptdate;
	const char* shipinstruct;
	const char* shipmode;
	const char* comment;

	long long int custkey;
	const char* orderstatus;
	double totalprice;
	const char* orderdate;
	const char* orderpriority;
	const char* clerk;
	int shippriority;
	exec sql end declare section;

	string tgqt = "SSB trigger iter";
	struct timeval tvs, tve;

	gettimeofday(&tvs, NULL);

	set_up_matviews_and_triggers(mv_script_file, log);

	assert ( s );

	long tuple_counter = 0;

	while ( s->stream_has_inputs() )
	{
		++tuple_counter;

		stream_tuple in = s->next_input();

		switch (in.type) {
		case 0:
			li = boost::any_cast<lineitem>(&(in.data));

			orderkey = li->orderkey;
			partkey = li->partkey;
			suppkey = li->suppkey;
			linenumber = li->linenumber;
			quantity = li->quantity;
			extendedprice = li->extendedprice;
			discount = li->discount;
			tax = li->tax;
			returnflag = li->returnflag.c_str();
			linestatus = li->linestatus.c_str();
			shipdate = li->shipdate.c_str();
			commitdate = li->commitdate.c_str();
			receiptdate = li->receiptdate.c_str();
			shipinstruct = li->shipinstruct.c_str();
			shipmode = li->shipmode.c_str();
			comment = li->comment.c_str();

			exec sql insert into lineitem values (
				orderkey, partkey, suppkey,
				linenumber, quantity, extendedprice,
				discount, tax, returnflag, linestatus, shipdate,
				commitdate, receiptdate, shipinstruct, shipmode, comment);
			break;

		case 1:
			ord = boost::any_cast<order>(&(in.data));

			orderkey = ord->orderkey;
			custkey = ord->custkey;
			orderstatus = ord->orderstatus.c_str();
			totalprice = ord->totalprice;
			orderdate = ord->orderdate.c_str();
			orderpriority = ord->orderpriority.c_str();
			clerk = ord->clerk.c_str();
			shippriority = ord->shippriority;
			comment = ord->comment.c_str();

			exec sql insert into orders values (
				orderkey, custkey, orderstatus, totalprice,
				orderdate, orderpriority, clerk, shippriority);
			break;

		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		default:
			cerr << "Invalid tuple type " << in.type
				<< " (typeid '" << (in.data.type().name()) << "')" << endl;
			break;
		}

		if ( (tuple_counter % query_freq) == 0 ) {
			// Run queries after update.
			matview_query(mvqr, log, tgqt);
			print_mv_query_results(mvqr, results);
			mvqr.clear();
		}
	}

	gettimeofday(&tve, NULL);

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"SSB triggers", log, true);

	exec sql set autocommit to off;

	exec sql disconnect;

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
		catch (const boost::bad_any_cast& e)
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

	function<void (int, unsigned long, lineitem&, char*)> li_parser = parse_lineitem_field;
	function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;
	function<void (int, unsigned long, part&, char*)> pt_parser = parse_part_field;
	function<void (int, unsigned long, customer&, char*)> cs_parser = parse_customer_field;
	function<void (int, unsigned long, supplier&, char*)> sp_parser = parse_supplier_field;
	function<void (int, unsigned long, nation&, char*)> nt_parser = parse_nation_field;
	function<void (int, unsigned long, region&, char*)> rg_parser = parse_region_field;

	lineitem_stream* li_s =
		new lineitem_stream(li_file, parse_lineitem_field, 16, 65000000, 512);

	order_stream* ord_s =
		new order_stream(ord_file, parse_order_field, 9, 17000000, 512);

	part_stream* pt_s =
		new part_stream(pt_file, parse_part_field, 9, 2100000, 512);

	customer_stream* cs_s =
		new customer_stream(cs_file, parse_customer_field, 8, 1600000, 512);

	supplier_stream* sp_s =
		new supplier_stream(sp_file, parse_supplier_field, 7, 110000, 512);

	nation_stream* nt_s =
		new nation_stream(nt_file, parse_nation_field, 3, 100, 512);

	region_stream* rg_s =
		new region_stream(rg_file, parse_region_field, 3, 100, 512);

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

#ifdef MEMORY
static const size_t li_tp_entry_sz = sizeof(identifier)+sizeof(double);
static const size_t li_idx_entry_sz = sizeof(identifier)+sizeof(lineitem);
static const size_t ord_idx_entry_sz = sizeof(identifier)+sizeof(order);
static const size_t nations_entry_sz = 2*sizeof(identifier)+15;
static const size_t regions_entry_sz = sizeof(identifier)+15;

static const size_t odyr_entry_sz = 14+(2*sizeof(date)); // xxxx-xx-xx date format, xxxx year
static const size_t csnt_entry_sz = sizeof(identifier)+10; // assume 10 chars nation name
static const size_t supp_entry_sz = sizeof(identifier);
static const size_t parts_entry_sz = sizeof(identifier);

void analyse_memory_usage(ofstream* stats_file, unsigned long counter)
{
	size_t li_tp_size = li_tp.size() * li_tp_entry_sz;
	size_t li_idx_size = li_idx.size() * li_idx_entry_sz;
	size_t ord_idx_size = ord_idx.size() * ord_idx_entry_sz;
	size_t nations_size = nations.size() * nations_entry_sz;
	size_t regions_size = regions.size() * regions_entry_sz;

	size_t odyr_size = odyr.size() * odyr_entry_sz;
	size_t csnt_size = csnt.size() * csnt_entry_sz;
	size_t sup_size = sup.size() * supp_entry_sz;
	size_t parts_size = parts.size() * parts_entry_sz;

	size_t total_size =
		li_tp_size + li_idx_size + ord_idx_size + nations_size + regions_size +
		odyr_size + csnt_size + sup_size + parts_size;

	(*stats_file) << counter << ","
		<< total_size << ","
		<< li_tp.size() << "," << li_tp_size << ","
		<< li_idx.size() << "," << li_idx_size << ","
		<< ord_idx.size() << "," << ord_idx_size << ","
		<< nations.size() << "," << nations_size << ","
		<< regions.size() << "," << regions_size << ","
		<< odyr.size() << "," << odyr_size << ","
		<< csnt.size() << "," << csnt_size << ","
		<< sup.size() << "," << sup_size << ","
		<< parts.size() << "," << parts_size;

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

void ssb_stream(stream* s, ofstream* log, ofstream* results, ofstream* stats)
{
	struct timeval tvs, tve;

	#ifdef MEMORY
	// Space analysis.
	unsigned long tuple_counter = 0;
	unsigned long mem_sample_freq = 1000;		// Sample every 1000 tuples.
	#endif

	gettimeofday(&tvs, NULL);

	while  ( s->stream_has_inputs() )
	{
		#ifdef MEMORY
		++tuple_counter;
		if ( (tuple_counter % mem_sample_freq) == 0 )
			analyse_memory_usage(stats, tuple_counter / mem_sample_freq);
		#endif

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
		catch (const boost::bad_any_cast& e)
		{
			cerr << "Failed to cast tuple data!" << endl;
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

	function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;
	function<void (int, unsigned long, nation&, char*)> nt_parser = parse_nation_field;
	function<void (int, unsigned long, region&, char*)> rg_parser = parse_region_field;

	// Build orderdate_year map
	order_stream* ord_s =
		new order_stream(ord_file, parse_order_field, 9, 100, 512);

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
		new nation_stream(nt_file, parse_nation_field, 3, 100, 512);

	region_stream* rg_s =
		new region_stream(rg_file, parse_region_field, 3, 100, 512);

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

	function<void (int, unsigned long, part&, char*)> pt_parser = parse_part_field;
	function<void (int, unsigned long, customer&, char*)> cs_parser = parse_customer_field;
	function<void (int, unsigned long, supplier&, char*)> sp_parser = parse_supplier_field;

	part_stream* pt_s =
		new part_stream(pt_file, parse_part_field, 9, 2100000, 512);

	customer_stream* cs_s =
		new customer_stream(cs_file, parse_customer_field, 8, 1600000, 512);

	supplier_stream* sp_s =
		new supplier_stream(sp_file, parse_supplier_field, 7, 110000, 512);

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
	string ord_file = dir + "/orders.tbl" + (fileset.empty()? "" : "." + fileset);

	function<void (int, unsigned long, lineitem&, char*)> li_parser = parse_lineitem_field;
	function<void (int, unsigned long, order&, char*)> ord_parser = parse_order_field;

	lineitem_stream* li_s =
		new lineitem_stream(li_file, parse_lineitem_field, 16, 65000000, 512);

	order_stream* ord_s =
		new order_stream(ord_file, parse_order_field, 9, 17000000, 512);

	li_s->init_stream();
	ord_s->init_stream();

	// Preload date, nations, regions, customers, suppliers, parts
	preload_streams(dir, fileset);

	vector<stream*> inputs(2);
	inputs[0] = li_s; inputs[1] = ord_s;
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
	else if ( app_mode == "ecpg" )
	{
		bool single_shot = false;
		stream* multiplexed_stream = load_streams(directory, fileset);
		ssb_ecpg(log_f, results_f, directory, fileset, query_freq,
			single_shot, multiplexed_stream);
	}
	else if ( app_mode == "matviews" )
	{
		assert ( argc > 8 );
		stream* multiplexed_stream = load_streams(directory, fileset);
		ssb_matviews(mv_script_file, directory, fileset, query_freq,
			multiplexed_stream, log_f, results_f, stats_f);
	}
	else if ( app_mode == "triggers" )
	{
		assert ( argc > 8 );
		stream* multiplexed_stream = load_streams(directory, fileset);
		ssb_triggers(mv_script_file, directory, fileset, query_freq,
			multiplexed_stream, log_f, results_f, stats_f);
	}
	else
	{
		stream* multiplexed_stream = load_snapshot_streams(directory, fileset);
		ssb_snapshot(multiplexed_stream, log_f, results_f, query_freq);
	}

    return 0;
}
