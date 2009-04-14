// ssb.cpp : main project file.

#include <cassert>
#include <cstdlib>

#include <hash_map>
#include <list>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cliext/hash_map>
#include <cliext/hash_set>
#include <cliext/list>
#include <cliext/vector>
#include <cliext/utility>

#include <boost/any.hpp>
#include <boost/function.hpp>

#include "tpch.h"

#define PENNIES 100

using namespace std;
using namespace tr1;

using namespace System;
using namespace System::Data;
using namespace System::Data::SqlClient;
using namespace System::Diagnostics;
using namespace System::IO;

//
//
// Field parsers
generic<typename T>
public delegate void parse_field_fn(Int32, UInt32, T%, String^);

inline void parse_lineitem_field(
	Int32 field, UInt32 line, Lineitem^% r, String^ data)
{
	switch(field) {
	case 0:
		r->orderkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->partkey = Convert::ToUInt64(data);
		break;
	case 2:
		r->suppkey = Convert::ToUInt64(data);
		break;
	case 3:
		r->linenumber = Convert::ToInt32(data);
		break;
	case 4:
		r->quantity = Convert::ToDouble(data);
		break;
	case 5:
		r->extendedprice = Convert::ToDouble(data);
		break;
	case 6:
		r->discount = Convert::ToDouble(data);
		break;
	case 7:
		r->tax = Convert::ToDouble(data);
		break;
	case 8:
		r->returnflag = gcnew String(data);
		break;
	case 9:
		r->linestatus = gcnew String(data);
		break;
	case 10:
		r->shipdate = gcnew String(data);
		break;
	case 11:
		r->commitdate = gcnew String(data);
		break;
	case 12:
		r->receiptdate = gcnew String(data);
		break;
	case 13:
		r->shipinstruct = gcnew String(data);
		break;
	case 14:
		r->shipmode = gcnew String(data);
		break;
	case 15:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid lineitem field id {0} {1}", field, line);
		break;
	}

	//Console::WriteLine("Parsed {0}", r->as_string());
}

inline void parse_order_field(Int32 field, UInt32 line, Order^% r, String^ data)
{
	switch(field) {
	case 0:
		r->orderkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->custkey = Convert::ToUInt64(data);
		break;
	case 2:
		r->orderstatus = gcnew String(data);
		break;
	case 3:
		r->totalprice = Convert::ToDouble(data);
		break;
	case 4:
		r->orderdate = gcnew String(data);
		break;
	case 5:
		r->orderpriority = gcnew String(data);
		break;
	case 6:
		r->clerk = gcnew String(data);
		break;
	case 7:
		r->shippriority = Convert::ToInt32(data);
		break;
	case 8:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid order field id {0} {1}", field, line);
		break;
	}
}

inline void parse_part_field(Int32 field, UInt32 line, Part^% r, String^ data)
{
	switch(field) {
	case 0:
		r->partkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->name = gcnew String(data);
		break;
	case 2:
		r->mfgr = gcnew String(data);
		break;
	case 3:
		r->brand= gcnew String(data);
		break;
	case 4:
		r->type = gcnew String(data);
		break;
	case 5:
		r->size = Convert::ToInt32(data);
		break;
	case 6:
		r->container = gcnew String(data);
		break;
	case 7:
		r->retailprice = Convert::ToDouble(data);
		break;
	case 8:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid part field id {0} {1}", field, line);
		break;
	}
}

inline void parse_customer_field(Int32 field, UInt32 line, Customer^% r, String^ data)
{
	switch(field) {
	case 0:
		r->custkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->name = gcnew String(data);
		break;
	case 2:
		r->address = gcnew String(data);
		break;
	case 3:
		r->nationkey = Convert::ToUInt64(data);
		break;
	case 4:
		r->phone = gcnew String(data);
		break;
	case 5:
		r->acctbal = Convert::ToDouble(data);
		break;
	case 6:
		r->mktsegment = gcnew String(data);
		break;
	case 7:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid customer field id {0} {1}", field, line);
		break;
	}
}

inline void parse_supplier_field(Int32 field, UInt32 line, Supplier^% r, String^ data)
{
	switch(field) {
	case 0:
		r->suppkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->name = gcnew String(data);
		break;
	case 2:
		r->address = gcnew String(data);
		break;
	case 3:
		r->nationkey = Convert::ToUInt64(data);
		break;
	case 4:
		r->phone = gcnew String(data);
		break;
	case 5:
		r->acctbal = Convert::ToDouble(data);
		break;
	case 6:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid supplier field id {0} {1}", field, line);
		break;
	}
}

inline void parse_nation_field(Int32 field, UInt32 line, Nation^% r, String^ data)
{
	switch(field) {
	case 0:
		r->nationkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->name = gcnew String(data);
		break;
	case 2:
		r->regionkey = Convert::ToUInt64(data);
		break;
	case 3:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid nation field id {0} {1}", field, line);
		break;
	}
}

inline void parse_region_field(Int32 field, UInt32 line, Region^% r, String^ data)
{
	switch(field) {
	case 0:
		r->regionkey = Convert::ToUInt64(data);
		break;
	case 1:
		r->name = gcnew String(data);
		break;
	case 2:
		r->comment = gcnew String(data);
		break;
	default:
		Console::WriteLine("Invalid region field id {0} {1}", field, line);
		break;
	}
}


//
//
// Generic file I/O

ref struct stream_tuple {
	int type;
	Object^ data;

	stream_tuple() : type(-1) {}

	stream_tuple(stream_tuple% o) {
		type = o.type;
		data = o.data;
	}

	stream_tuple(stream_tuple^ o) {
		type = o->type;
		data = o->data;
	}
};

ref struct stream abstract {
	virtual bool stream_has_inputs() = 0;
	virtual stream_tuple next_input() = 0;
	virtual unsigned int get_buffer_size() = 0;
};

template<typename T>
ref struct file_stream : public stream
{
private:
	typedef cliext::list<T> stream_buffer;
	parse_field_fn<T>^ field_parser;

	String^ file_name;
	StreamReader^ input_file;
	stream_buffer buffer;
	UInt32 line;

	static String^ delimiter = "|";

	UInt32 field_count;

	UInt32 buffer_count;
	UInt32 buffer_size;
	UInt32 threshold;
	UInt32 line_size;
	Boolean finished_reading;

	cliext::pair<bool, T> read_tuple()
	{
		T r;

		String^ buf = input_file->ReadLine();
		++line;

		if ( buf == nullptr )
			return cliext::make_pair(false, r);

		array<wchar_t>^ data_chars = buf->ToCharArray();

		Int32 start_idx = 0;
		Int32 end_idx = start_idx;

		//Console::WriteLine("Data: {0}", buf);

		for (UInt32 i = 0; i < field_count; ++i)
		{
			while ( end_idx < data_chars->Length &&
						data_chars[end_idx] != delimiter )
			{
				++end_idx;
			}

			if ( start_idx == end_idx )
			{
				Console::WriteLine("Invalid field {0}: {1} {2}",
					file_name, line, i);

				return cliext::make_pair(false, r);
			}

			if ( end_idx >= data_chars->Length && i != (field_count - 1) )
			{
				Console::WriteLine("Invalid field {0}: {1} {2}",
					file_name, line, i);

				return cliext::make_pair(false, r);
			}

			String^ field =
				gcnew String(data_chars, start_idx, (end_idx - start_idx));

			field_parser(i, line, r, field);
			start_idx = ++end_idx;
		}

		//Console::WriteLine("Read: {0}", r.as_string());

		return cliext::make_pair(true, r);
	}

	void buffer_stream()
	{
		buffer_size = buffer.size();
		while ( buffer_size < buffer_count && input_file != nullptr )
		{
			cliext::pair<bool, T> valid_input = read_tuple();
			if ( !valid_input.first ) {
				delete (IDisposable^) input_file;
				input_file = nullptr;
				break;
			}

			buffer.push_back(valid_input.second);
			++buffer_size;
		}

		if ( input_file == nullptr ) finished_reading = true;
	}

public:

	file_stream(String^ fn, parse_field_fn<T>^ parse_fn,
				UInt32 fields, UInt32 c, UInt32 ls)
		: field_parser(parse_fn), file_name(fn),
		  line(0), field_count(fields), buffer_count(c),
		  buffer_size(0), line_size(ls),
		  finished_reading(false)
	{
		assert ( field_count > 0 );
		assert ( line_size > 32 );

		input_file = gcnew StreamReader(file_name);

		threshold = Convert::ToUInt32(
			Math::Ceiling(Convert::ToDouble(c) / 10));
	}

	~file_stream() {
		input_file->Close();
		delete (IDisposable^)input_file;
	}

	void init_stream() { buffer_stream(); }

	virtual Boolean stream_has_inputs() override {
		return !buffer.empty();
	}

	int get_tuple_type(T% next)
	{
		int r;
		if ( T::typeid == Lineitem::typeid )
			r = 0;
		else if ( T::typeid == Order::typeid )
			r = 1;
		else if ( T::typeid == Part::typeid )
			r = 2;
		else if ( T::typeid == Customer::typeid )
			r = 3;
		else if ( T::typeid == Supplier::typeid )
			r = 4;
		else if ( T::typeid == Nation::typeid )
			r = 5;
		else if ( T::typeid == Region::typeid )
			r = 6;
		else {
			r = -1;
		}

		return r;
	}

	virtual stream_tuple next_input() override
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

	virtual unsigned int get_buffer_size() override {
		return buffer_size;
	}
};


//
//
// File I/O typedefs

typedef file_stream<Lineitem^>  lineitem_stream;
typedef file_stream<Order^>     order_stream;
typedef file_stream<Part^>      part_stream;
typedef file_stream<Customer^>  customer_stream;
typedef file_stream<Supplier^>  supplier_stream;
typedef file_stream<Nation^>    nation_stream;
typedef file_stream<Region^>    region_stream;

ref struct multiplexer : public stream
{
	cliext::vector<stream^> inputs;
	int num_streams;

	Random^ rng;

	multiplexer(cliext::vector<stream^>^ ins, Int32 seed)
		: inputs(*ins), num_streams(ins->size()),
		  rng(gcnew Random(seed))
	{}


	virtual Boolean stream_has_inputs () override
	{
		return !(inputs.empty());
	}

	virtual stream_tuple next_input () override
	{
		Int32 next_stream = rng->Next(num_streams);
		stream_tuple r = inputs[next_stream]->next_input();
		if ( !inputs[next_stream]->stream_has_inputs() )
		{
			Console::WriteLine("Done with stream {0}", next_stream);
			inputs.erase(inputs.begin()+next_stream);
			--num_streams;
		}

		return r;
	}

	virtual unsigned int get_buffer_size () override
	{
		cliext::vector<stream^>::iterator ins_it = inputs.begin();
		cliext::vector<stream^>::iterator ins_end = inputs.end();

		unsigned int r = 0;
		for (; ins_it != ins_end; ++ins_it)
			r += (*ins_it)->get_buffer_size();

		return r;
	}
};

///////////////////////////
//
// QP common.

ref class QueryProcessor
{
protected:
	//
	// Data structures
	typedef cliext::hash_map<Date, Date> orderdate_year;
	typedef cliext::hash_map<Identifier, String^> customer_nation;
	typedef cliext::hash_set<Identifier> valid_suppliers;
	typedef cliext::hash_set<Identifier> valid_parts;

	orderdate_year odyr;
	customer_nation csnt;
	valid_suppliers sup;
	valid_parts parts;

	//----------
	// 5/L.D <d_dk, d_yr>
	// ++ 5[d_dk] = d_yr::5[d_dk]
	void delta_ld_l2(Date d_dk, String^ d_yr)
	{
		odyr[d_dk] = d_yr;
	}

	// 5/L.C <c_ck, c_nt>
	// ++ 5[c_nt] = c_nt::5[c_ck]
	void delta_lc_l2(Identifier c_ck, String^ c_nt, String^ c_rg)
	{
		if ( c_rg == "AMERICA" ) {
			csnt[c_ck] = c_nt;
		}
	}

	// 5/L.S <s_sk, s_rg>
	// ++ 5[s_sk] = (s_rg == 'AMERICA')
	void delta_ls_l2(Identifier s_sk, String^ s_rg)
	{
		if ( s_rg == "AMERICA" )
			sup.insert(s_sk);
	}

	// 5/L.P <p_pk, p_mfgr>
	// ++ 5[p_pk] = (p_mfgr == 'MFGR#1' or p_mfgr == 'MFGR#2')
	void delta_lp_l2(Identifier p_pk, String^ p_mfgr)
	{
		if ( (p_mfgr == "MFGR#1") || (p_mfgr == "MFGR#2") )
			parts.insert(p_pk);
	}

	//////////////////////
	//
	// Data integration
	typedef cliext::hash_map<Identifier, Nation> nations_index;
	typedef cliext::hash_map<Identifier, String^> regions_index;

	nations_index nations;
	regions_index regions;

	// Helpers

	void print_nations()
	{
		nations_index::iterator n_it = nations.begin();
		nations_index::iterator n_end = nations.end();

		for (; n_it != n_end; ++n_it) {
			Console::WriteLine("{0}, {1}, {2}", n_it->first,
				n_it->second->regionkey, n_it->second->name);
		}
	}

	inline String^ get_nation(Identifier nk)
	{
		nations_index::iterator nt_found = nations.find(nk);

		assert ( nt_found != nations.end() );
		return nt_found->second->name;
	}

	inline String^ get_region(Identifier nk)
	{
		nations_index::iterator nt_found = nations.find(nk);

		if ( nt_found == nations.end() ) {
			Console::WriteLine("Failed to find nation for {0}", nk);
			print_nations();
		}
		assert ( nt_found != nations.end() );
		Identifier rk = nt_found->second->regionkey;

		regions_index::iterator rg_found = regions.find(rk);
		assert ( rg_found != regions.end() );
		return rg_found->second;
	}

	inline cliext::pair<Boolean, String^> get_year(Date d)
	{
		String^ r = "";
		Int32 dash_pos = d->IndexOf('-');
		if ( dash_pos >= 0 ) {
			return cliext::make_pair(true, d->Substring(0, dash_pos));
		}

		return cliext::make_pair(false, r);
	}

	// Deltas

	void delta_part(Part^ pt)
	{
		delta_lp_l2(pt->partkey, pt->mfgr);
	}

	void delta_customer(Customer^ cs)
	{
		String^ nation = get_nation(cs->nationkey);
		String^ region = get_region(cs->nationkey);
		delta_lc_l2(cs->custkey, nation, region);
	}

	void delta_supplier(Supplier^ sp)
	{
		String^ region = get_region(sp->nationkey);
		delta_ls_l2(sp->suppkey, region);
	}

	void delta_nation(Nation^ nt)
	{
		nations[nt->nationkey] = nt;
	}


	void delta_region(Region^ rg)
	{
		regions[rg->regionkey] = rg->name;
	}

public:

	void preload_streams(String^ dir, String^ fileset)
	{
		Console::WriteLine("Preloading streams...");

		String^ ord_file = dir + "/orders.tbl" + (fileset == nullptr? "" : "." + fileset);
		String^ nt_file = dir + "/nation.tbl";
		String^ rg_file = dir + "/region.tbl";

		parse_field_fn<Order^>^ ord_parser =
			gcnew parse_field_fn<Order^>(&parse_order_field);

		parse_field_fn<Nation^>^ nt_parser =
			gcnew parse_field_fn<Nation^>(&parse_nation_field);

		parse_field_fn<Region^>^ rg_parser =
			gcnew parse_field_fn<Region^>(&parse_region_field);

		// Build orderdate_year map
		order_stream^ ord_s =
			gcnew order_stream(ord_file, ord_parser, 9, 100, 512);

		ord_s->init_stream();

		while ( ord_s->stream_has_inputs() ) {
			stream_tuple next = ord_s->next_input();
			Order^ ord = safe_cast<Order^>(next.data);
			cliext::pair<Boolean, String^> year_found = get_year(ord->orderdate);
			if ( year_found.first )
				delta_ld_l2(ord->orderdate, year_found.second);
		}

		delete ord_s;

		// Build regions, nations
		nation_stream^ nt_s =
			gcnew nation_stream(nt_file, nt_parser, 3, 100, 512);

		region_stream^ rg_s =
			gcnew region_stream(rg_file, rg_parser, 3, 100, 512);

		rg_s->init_stream();
		nt_s->init_stream();

		Console::WriteLine("{0}: {1}", rg_file, rg_s->get_buffer_size());
		Console::WriteLine("{0}: {1}", nt_file, nt_s->get_buffer_size());

		assert ( rg_s->get_buffer_size() > 0 && nt_s->get_buffer_size() > 0 );

		while ( rg_s->stream_has_inputs() ) {
			stream_tuple next = rg_s->next_input();
			delta_region(safe_cast<Region^>(next.data));
		}

		while ( nt_s->stream_has_inputs() ) {
			stream_tuple next = nt_s->next_input();
			delta_nation(safe_cast<Nation^>(next.data));
		}

		delete rg_s;
		delete nt_s;

		// Preload customers, suppliers, parts.
		String^ pt_file = dir + "/part.tbl" + (fileset == nullptr? "" : "." + fileset);
		String^ cs_file = dir + "/customer.tbl" + (fileset == nullptr? "" : "." + fileset);
		String^ sp_file = dir + "/supplier.tbl" + (fileset == nullptr? "" : "." + fileset);

		parse_field_fn<Part^>^ pt_parser =
			gcnew parse_field_fn<Part^>(&parse_part_field);

		parse_field_fn<Customer^>^ cs_parser =
			gcnew parse_field_fn<Customer^>(&parse_customer_field);

		parse_field_fn<Supplier^>^ sp_parser =
			gcnew parse_field_fn<Supplier^>(&parse_supplier_field);

		part_stream^ pt_s =
			gcnew part_stream(pt_file, pt_parser, 9, 2100000, 512);

		customer_stream^ cs_s =
			gcnew customer_stream(cs_file, cs_parser, 8, 1600000, 512);

		supplier_stream^ sp_s =
			gcnew supplier_stream(sp_file, sp_parser, 7, 110000, 512);

		pt_s->init_stream();
		cs_s->init_stream();
		sp_s->init_stream();

		Console::WriteLine("{0}: {1}", pt_file, pt_s->get_buffer_size());
		Console::WriteLine("{0}: {1}", cs_file, cs_s->get_buffer_size());
		Console::WriteLine("{0}: {1}", sp_file, sp_s->get_buffer_size());

		assert ( pt_s->get_buffer_size() > 0 &&
					cs_s->get_buffer_size() > 0 &&
					sp_s->get_buffer_size() > 0);

		while ( pt_s->stream_has_inputs() ) {
			stream_tuple next = pt_s->next_input();
			delta_part(safe_cast<Part^>(next.data));
		}

		while ( cs_s->stream_has_inputs() ) {
			stream_tuple next = cs_s->next_input();
			delta_customer(safe_cast<Customer^>(next.data));
		}

		while ( sp_s->stream_has_inputs() ) {
			stream_tuple next = sp_s->next_input();
			delta_supplier(safe_cast<Supplier^>(next.data));
		}

		delete pt_s;
		delete cs_s;
		delete sp_s;
	}
};

/////////////////////////////////
//
// Trigger-based processing

typedef cliext::hash_map<Double, cliext::hash_map<String^, Double> > MVQueryResults;


void print_mv_query_results(MVQueryResults^ qr, StreamWriter^ results)
{
	MVQueryResults::iterator qr_it = qr->begin();
	MVQueryResults::iterator qr_end = qr->end();

	for (; qr_it != qr_end; ++qr_it)
	{
		cliext::hash_map<String^, Double>::iterator nt_it =
			qr_it->second->begin();

		cliext::hash_map<String^, Double>::iterator nt_end =
			qr_it->second->end();

		for (; nt_it != nt_end; ++nt_it)
		{
			results->WriteLine("{0}, {1}, {2}", qr_it->first,
				nt_it->first, nt_it->second);
		}
	}
}

void execute_setup_script(SqlConnection^ dbConn, String^ sql_file)
{
	StreamReader^ script_reader = gcnew StreamReader(sql_file);

	SqlCommand^ scriptCmd = gcnew SqlCommand(nullptr, dbConn);

	String^ batch = nullptr;
	String^ line = nullptr;

	Stopwatch^ scriptSw = Stopwatch::StartNew();

	try {
		while ( line = script_reader->ReadLine() ) {
			if ( line->StartsWith("GO") ) {
				scriptCmd->CommandText = batch;
				scriptCmd->ExecuteNonQuery();
			}
			else {
				batch = batch == nullptr?
					line : String::Concat(batch, line);
			}
		}

		if ( script_reader )
			delete (IDisposable^)script_reader;
	}
	catch ( Exception^ e ) {
		Console::WriteLine( "Error executing script file '{0}':", sql_file );
		Console::WriteLine( e->Message );
	}

	scriptSw->Stop();
	Console::WriteLine("{0} s for 'SSB trigger setup'",
		scriptSw->Elapsed.TotalSeconds);
}

void matview_query(SqlConnection^ dbConn, MVQueryResults^ mvqr,
				   StreamWriter^ log, String^ queryType)
{
	Stopwatch^ querySw = Stopwatch::StartNew();

	SqlCommand^ viewQuery = gcnew SqlCommand(nullptr, dbConn);
	viewQuery->CommandText = "SELECT year, nation, profit FROM query_result";
	SqlDataReader^ resultReader = viewQuery->ExecuteReader();
	while ( resultReader->Read() )
	{
		Double year = resultReader->GetDouble(0);
		String^ nation = resultReader->GetString(1);
		Double profit = resultReader->GetDouble(2);
		mvqr[year][nation] = profit;
	}

	querySw->Stop();
	Console::WriteLine("{0} s for '{1}'",
		querySw->Elapsed.TotalSeconds, queryType);
}

void ssb_triggers(
	String^ serverName, String^ dbName, String^ mvScriptFile,
	String^ directory, String^ fileset, UInt32 queryFreq,
	stream^ s, StreamWriter^ log, StreamWriter^ results, StreamWriter^ stats)
{
	String^ dbConnStr = String::Concat(
		"Integrated Security=true", ";Server=", serverName,
		";Initial Catalog=", dbName);

    SqlConnection^ dbConn = gcnew SqlConnection(dbConnStr);

    dbConn->Open();

	if (dbConn->State != ConnectionState::Open)
		Debug::WriteLine("Failed to connect to database engine [" + dbName + "].");

	Console::WriteLine(String::Concat(
		"SSB triggers using query frequency ",
		Convert::ToString(queryFreq)));

	// Execute setup script
	execute_setup_script(dbConn, mvScriptFile);

	// Compile SQL commands.
	SqlCommand^ insertLineitemCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ insertOrderCmd = gcnew SqlCommand(nullptr, dbConn);

	insertLineitemCmd->CommandText = 
		"insert into lineitem values (" +
			"@orderkey, @partkey, @suppkey," +
			"@linenumber, @quantity, @extendedprice," +
			"@discount, @tax, @returnflag, @linestatus, @shipdate," +
			"@commitdate, @receiptdate, @shipinstruct, @shipmode, @comment);";

	SqlParameter^ lokParam = gcnew SqlParameter("@orderkey", SqlDbType::BigInt, 0);
	SqlParameter^ lpkParam = gcnew SqlParameter("@partkey", SqlDbType::BigInt, 0);
	SqlParameter^ lskParam = gcnew SqlParameter("@suppkey", SqlDbType::BigInt, 0);
	SqlParameter^ llnParam = gcnew SqlParameter("@linenumber", SqlDbType::Int, 0);
	SqlParameter^ lqtParam = gcnew SqlParameter("@quantity", SqlDbType::Decimal, 0);
	SqlParameter^ lepParam = gcnew SqlParameter("@extendedprice", SqlDbType::Decimal, 0);
	SqlParameter^ ldsParam = gcnew SqlParameter("@discount", SqlDbType::Decimal, 0);
	SqlParameter^ ltxParam = gcnew SqlParameter("@tax", SqlDbType::Decimal, 0);
	SqlParameter^ lrfParam = gcnew SqlParameter("@returnflag", SqlDbType::Char, 1);
	SqlParameter^ llsParam = gcnew SqlParameter("@linestatus", SqlDbType::Char, 1);
	SqlParameter^ lsdParam = gcnew SqlParameter("@shipdate", SqlDbType::Date, 0);
	SqlParameter^ lcoParam = gcnew SqlParameter("@commitdate", SqlDbType::Date, 0);
	SqlParameter^ lrdParam = gcnew SqlParameter("@receiptdate", SqlDbType::Date, 0);
	SqlParameter^ lsiParam = gcnew SqlParameter("@shipinstruct", SqlDbType::Char, 25);
	SqlParameter^ lsmParam = gcnew SqlParameter("@shipmode", SqlDbType::Char, 10);
	SqlParameter^ lcmParam = gcnew SqlParameter("@comment", SqlDbType::VarChar, 44);

	lokParam->Value = gcnew Int64(0);
	lpkParam->Value = gcnew Int64(0);
	lskParam->Value = gcnew Int64(0);
	llnParam->Value = gcnew Int32(0);
	lqtParam->Value = 0.0; lqtParam->Precision = 18; lqtParam->Scale = 0;
	lepParam->Value = 0.0; lepParam->Precision = 18; lepParam->Scale = 0;
	ldsParam->Value = 0.0; ldsParam->Precision = 18; ldsParam->Scale = 0;
	ltxParam->Value = 0.0; ltxParam->Precision = 18; ltxParam->Scale = 0;
	lrfParam->Value = "";
	llsParam->Value = "";
	lsdParam->Value = DateTime::Now;
	lcoParam->Value = DateTime::Now;
	lrdParam->Value = DateTime::Now;
	lsiParam->Value = "";
	lsmParam->Value = "";
	lcmParam->Value = "";


	array<SqlParameter^>^ lineitemParams = {
		lokParam, lpkParam, lskParam, llnParam, lqtParam,
		lepParam, ldsParam, ltxParam,
		lrfParam, llsParam, lsdParam, lcoParam, lrdParam,
		lsiParam, lsmParam, lcmParam };

	insertLineitemCmd->Parameters->AddRange(lineitemParams);
	insertLineitemCmd->Prepare();

	insertOrderCmd->CommandText =
		"insert into orders values (" + 
			"@orderkey, @custkey, @orderstatus, @totalprice," +
			"@orderdate, @orderpriority, @clerk, @shippriority)";

	SqlParameter^ ookParam = gcnew SqlParameter("@orderkey", SqlDbType::BigInt, 0);
	SqlParameter^ ockParam = gcnew SqlParameter("@custkey", SqlDbType::BigInt, 0);
	SqlParameter^ oosParam = gcnew SqlParameter("@orderstatus", SqlDbType::Char, 1);
	SqlParameter^ otpParam = gcnew SqlParameter("@totalprice", SqlDbType::Decimal, 0);
	SqlParameter^ oodParam = gcnew SqlParameter("@orderdate", SqlDbType::Date, 0);
	SqlParameter^ oopParam = gcnew SqlParameter("@orderpriority", SqlDbType::Char, 15);
	SqlParameter^ oclParam = gcnew SqlParameter("@clerk", SqlDbType::Char, 15);
	SqlParameter^ ospParam = gcnew SqlParameter("@shippriority", SqlDbType::Int, 0);

	ookParam->Value = gcnew Int64(0);
	ockParam->Value = gcnew Int64(0);
	oosParam->Value = "";
	otpParam->Value = 0.0; otpParam->Precision = 18; otpParam->Scale = 0;
	oodParam->Value = DateTime::Now;
	oopParam->Value = "";
	oclParam->Value = "";
	ospParam->Value = gcnew Int32(0);

	array<SqlParameter^>^ orderParams = {
		ookParam, ockParam, oosParam, otpParam,
		oodParam, oopParam, oclParam, ospParam };

	insertOrderCmd->Parameters->AddRange(orderParams);
	insertOrderCmd->Prepare();

	// Stream loop
	Stopwatch^ triggerSw = Stopwatch::StartNew();

	Stopwatch^ tupleSw = gcnew Stopwatch();
	Double tup_sum = 0.0;

	Lineitem^ li = nullptr;
	Order^ ord = nullptr;

	MVQueryResults mvqr;
	String^ tgqt = "SSB trigger iter";

	Int32 paramCount = 0;
	UInt64 tuple_counter = 0;

	while ( s->stream_has_inputs() )
	{
		++tuple_counter;

		stream_tuple in = s->next_input();

		tupleSw->Reset();
		tupleSw->Start();

		switch (in.type) {
		case 0:
			li = safe_cast<Lineitem^>(in.data);
			paramCount = 0;
			insertLineitemCmd->Parameters[paramCount]->Value = li->orderkey; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->partkey; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->suppkey; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->linenumber; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->quantity; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->extendedprice; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->discount; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->tax; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->returnflag; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->linestatus; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->shipdate; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->commitdate; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->receiptdate; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->shipinstruct; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->shipmode; ++paramCount;
			insertLineitemCmd->Parameters[paramCount]->Value = li->comment; ++paramCount;

			insertLineitemCmd->ExecuteNonQuery();
			break;
		
		case 1:
			ord = safe_cast<Order^>(in.data);
			paramCount = 0;
			insertOrderCmd->Parameters[paramCount]->Value = ord->orderkey; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->custkey; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->orderstatus; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->totalprice; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->orderdate; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->orderpriority; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->clerk; ++paramCount;
			insertOrderCmd->Parameters[paramCount]->Value = ord->shippriority; ++paramCount;

			insertOrderCmd->ExecuteNonQuery();
			break;

		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		default:
			Console::WriteLine("Invalid tuple type {0} (typeid '{1}')",
				in.type, in.data->GetType()->Name);
			break;
		}

		tupleSw->Stop();
		tup_sum += tupleSw->Elapsed.TotalSeconds;

		if ( (tuple_counter % 50000) == 0 ) {
			Console::WriteLine("Processed {0} tuples.", tuple_counter);
			Console::WriteLine("Exec time {0}", (tup_sum / 50000.0));
			tup_sum = 0.0;
		}

		if ( (tuple_counter % queryFreq) == 0 ) {
			// Run queries after update.
			matview_query(dbConn, %mvqr, log, tgqt);
			print_mv_query_results(%mvqr, results);
			mvqr.clear();
		}
	}
}


//////////////////////////////////
//
// DBToaster

ref class ToastedQP : public QueryProcessor
{
	typedef cliext::hash_map<Date, cliext::hash_map<String^, Double> > QueryResults;
	QueryResults qr;

	typedef cliext::hash_map<Identifier, Double> lineitem_totalprices;
	typedef cliext::hash_map<Identifier, Lineitem> lineitems_index;
	typedef cliext::hash_map<Identifier, Order> orders_index;

	lineitem_totalprices li_tp;
	lineitems_index li_idx;
	orders_index ord_idx;

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


	inline void delta_l_l1(Identifier% lo_ck, Identifier% lo_sk,
		Identifier% lo_pk, Date lo_od, Double% lo_rev, Double% lo_supc,
		QueryResults% running_results, StreamWriter^ results)
	{
		orderdate_year::iterator d_yr_found = odyr.find(lo_od);

		if ( d_yr_found == odyr.end() ) {
			Console::WriteLine("Failed to find year for d_dk {0}", lo_od);
		}

		assert ( d_yr_found != odyr.end() );

		customer_nation::iterator c_nt_found = csnt.find(lo_ck);

		if ( c_nt_found != csnt.end() )
		{
			valid_suppliers::iterator sk_found = sup.find(lo_sk);
			valid_parts::iterator pk_found = parts.find(lo_pk);

			if ( !(sk_found == sup.end() || pk_found != parts.end()) )
			{
				Date d_yr = d_yr_found->second;
				String^ c_nation = c_nt_found->second;

				running_results[d_yr][c_nation] += (lo_rev - lo_supc);

				results->WriteLine("{0}, {1}, {2}", d_yr, c_nation,
					running_results[d_yr][c_nation]);
			}
		}
	}

public:

	void delta_lineitem(Lineitem^ li, StreamWriter^ results)
	{
		#ifdef DEBUG
		Console::WriteLine("Tuple li: {0}", li->as_string());
		#endif

		// select sum((ep*(100-dis)/PENNIES)*((100+tax)/PENNIES)) as total_price from L gb ok
		li_tp[li->orderkey] +=
			((li->extendedprice*(100-li->discount))/PENNIES) *
			((100+li->tax)/PENNIES);

		// select ... from {lineitem}, orders, TP where L.ok = O.ok and O.ok = TP.ok
		orders_index::iterator ord_found = ord_idx.find(li->orderkey);
		if ( ord_found != ord_idx.end() )
		{
			Identifier ck = ord_found->second->custkey;
			Date od = ord_found->second->orderdate;

			Double rev = (li->extendedprice*(100-li->discount))/PENNIES;

			Double rprice = 90000;
			rprice += (li->partkey/10) % 20001;        /* limit contribution to $200 */
			rprice += (li->partkey % 1000) * 100;
			Double supc = 6*rprice/10;

			delta_l_l1(ck, li->suppkey, li->partkey, od, rev, supc, qr, results);
		}
		else {
			// Keep around for a future order...
			li_idx[li->orderkey] = *li;
		}
	}

	inline void delta_order(Order^ ord, StreamWriter^ results)
	{
		#ifdef DEBUG
		Console::WriteLine("Tuple ord: {0}", ord->as_string());
		#endif

		lineitems_index::iterator li_found = li_idx.find(ord->orderkey);
		if ( li_found != li_idx.end() )
		{
			Identifier sk = li_found->second->suppkey;
			Identifier pk = li_found->second->partkey;

			Double rev = (li_found->second->extendedprice*
				(100-li_found->second->discount))/PENNIES;

			Double rprice = 90000;
			rprice += (li_found->second->partkey/10) % 20001;        /* limit contribution to $200 */
			rprice += (li_found->second->partkey % 1000) * 100;
			Double supc = 6*rprice/10;

			delta_l_l1(ord->custkey, sk, pk, ord->orderdate, rev, supc, qr, results);
		}
		else {
			// Keep around for a future lineitem...
			ord_idx[ord->orderkey] = *ord;
		}
	}

	void ssb_stream(stream^ s,
		StreamWriter^ log, StreamWriter^ results, StreamWriter^ stats)
	{
		Stopwatch^ streamSw = Stopwatch::StartNew();

		UInt64 tupleCounter = 0;

		Lineitem^ li = nullptr;
		Order^ ord = nullptr;
		Part^ pt = nullptr;
		Customer^ cs = nullptr;
		Supplier^ sp = nullptr;
		Nation^ nt = nullptr;
		Region^ rg = nullptr;

		while  ( s->stream_has_inputs() )
		{
			++tupleCounter;

			stream_tuple in = s->next_input();

			switch (in.type) {
			case 0:
				li = safe_cast<Lineitem^>(in.data);
				delta_lineitem(li , results);
				break;

			case 1:
				ord = safe_cast<Order^>(in.data);
				delta_order(ord, results);
				break;

			case 2:
				pt = safe_cast<Part^>(in.data);
				delta_part(pt);
				break;

			case 3:
				cs = safe_cast<Customer^>(in.data);
				delta_customer(cs);
				break;

			case 4:
				sp = safe_cast<Supplier^>(in.data);
				delta_supplier(sp);
				break;

			case 5:
				nt = safe_cast<Nation^>(in.data);
				delta_nation(nt);
				break;

			case 6:
				rg = safe_cast<Region^>(in.data);
				delta_region(rg);
				break;

			default:
				break;
			}
		}

		streamSw->Stop();

		Console::WriteLine("{0} s for 'SSB stream'",
			streamSw->Elapsed.TotalSeconds);
	}

};


/////////////////////////////////
//
// Top level

stream^ load_streams(QueryProcessor^ qp, String^ dir, String^ fileset)
{
	String^ li_file = dir + "/lineitem.tbl" + (fileset == nullptr? "" : "." + fileset);
	String^ ord_file = dir + "/orders.tbl" + (fileset == nullptr? "" : "." + fileset);

	parse_field_fn<Lineitem^>^ li_parser =
		gcnew parse_field_fn<Lineitem^>(&parse_lineitem_field);

	parse_field_fn<Order^>^ ord_parser =
		gcnew parse_field_fn<Order^>(&parse_order_field);

	lineitem_stream^ li_s =
		gcnew lineitem_stream(li_file, li_parser, 16, 65000000, 512);

	order_stream^ ord_s =
		gcnew order_stream(ord_file, ord_parser, 9, 17000000, 512);

	li_s->init_stream();
	ord_s->init_stream();

	// Preload date, nations, regions, customers, suppliers, parts
	qp->preload_streams(dir, fileset);

	cliext::vector<stream^> inputs(2);
	inputs[0] = li_s; inputs[1] = ord_s;
	//inputs[2] = pt_s; inputs[3] = cs_s;
	//inputs[4] = sp_s;
	//inputs[5] = nt_s; inputs[6] = rg_s;

	multiplexer^ r = gcnew multiplexer(%inputs, 12345);
	return r;
}

void print_usage()
{
	Console::WriteLine(String::Concat(
		"Usage: ssb <app mode> <query frequency> ",
		"<log file> <results file> <stats file> ",
		"<tpc-h data directory> [tpc-h file set] [mv script file]"));
}

int main(array<System::String ^> ^args)
{
	if ( args->Length < 7 ) {
		print_usage();
		exit(1);
	}

	Console::WriteLine(String::Concat("# args: ", Convert::ToString(args->Length)));

	String^ serverName = "DBSERVER\\DBTOASTER";
	String^ dbName = "tpch";

	String^ app_mode = args[0];
	UInt32 query_freq = Convert::ToUInt32(args[1]);
	String^ log_file = args[2];
	String^ results_file = args[3];
	String^ stats_file = args[4];
	String^ directory = args[5];
	String^ fileset = args->Length > 6? args[6] : nullptr;
	String^ mv_script_file = (fileset == nullptr?
		(args->Length > 6? args[6] : nullptr) :
		(args->Length > 7? args[7] : nullptr));

	StreamWriter^ log = gcnew StreamWriter(log_file);
	StreamWriter^ results = gcnew StreamWriter(results_file);
	StreamWriter^ stats = gcnew StreamWriter(stats_file);

	if ( app_mode == "toasted" ) {
		ToastedQP^ qp = gcnew ToastedQP();
		stream^ multiplexed_stream = load_streams(qp, directory, fileset);
		qp->ssb_stream(multiplexed_stream, log, results, stats);
	}
	else if ( app_mode == "triggers" ) {
		assert ( args->Length > 7 );
		QueryProcessor^ qp = gcnew QueryProcessor();
		stream^ multiplexed_stream = load_streams(qp, directory, fileset);
		ssb_triggers(serverName, dbName, mv_script_file, directory, fileset, query_freq,
			multiplexed_stream, log, results, stats);
	}

	log->Flush(); log->Close();
	results->Flush(); results->Close();
	stats->Flush(); stats->Close();

    return 0;
}