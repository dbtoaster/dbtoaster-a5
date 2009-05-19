// vwap.cpp : main project file.

#include <cassert>
#include <cstdlib>

#include <algorithm>
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
#include <cliext/utility>

#include <time.h>
#include <windows.h>

using namespace std;
using namespace tr1;

using namespace System;
using namespace System::Data;
using namespace System::Data::SqlClient;
using namespace System::Diagnostics;
using namespace System::IO;

#define DBT_HASH_MAP unordered_map
#define DBT_HASH_SET unordered_set

#define VWAP_K 0.25

// Order books
// -- note we need to keep the order ids around for applying actions.

// Timestamp, price, volume
ref struct input_tuple
{
	UInt64 ts;
	Double price;
	Double volume;

	input_tuple() {}

	input_tuple(UInt64 t, Double p, Double v)
		: ts(t), price(p), volume(v)
	{}

	input_tuple(input_tuple% o) {
		ts = o.ts;
		price = o.price;
		volume = o.volume;
	}
};

// order id -> order
typedef cliext::hash_map<Int32, input_tuple> order_book;


// DBToaster data structures for VWAP computation

// price * volume -> sum(vol), count(*) as rc from bids where bids.price > price
typedef map<tuple<double, double>, tuple<double, int> > bcv_index;

// price -> sum(vol), count(*) as rc from bids where bids.price > price
typedef DBT_HASH_MAP<double, tuple<double, int> > price_bcv_index;

// total_volume -> sum(price * volume), count(price * volume) from bcv where bcv.cumsum < total_volume
typedef DBT_HASH_MAP<double, tuple<double, double> > vwap_index;

// total_volume -> count(*), avg(rc) from bcv group by cumsum_volume
// -- note rc must be the same for all tuples in the group, i.e. avg(rc) = rc
//typedef map<double, tuple<int, int> > cvol_index;

// total_volume -> price, count(*), avg(rc) from bcv group by cumsum_volume, price
// -- note rc must be the same for all tuples in the group, i.e. avg(rc) = rc
typedef DBT_HASH_MAP<double, DBT_HASH_MAP<double, tuple<int, int> > > cvol_index;

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


// Timestamp, order id, action, volume, price
ref struct stream_tuple
{
	UInt64 ts;
	Int32 orderid;
	String^ action;
	Double volume;
	Double price;

	stream_tuple() {}

	stream_tuple(stream_tuple% o) {
		ts = o.ts;
		orderid = o.orderid;
		action = gcnew String(o.action);
		volume = o.volume;
		price = o.price;
	}

	stream_tuple(stream_tuple^ o) {
		ts = o->ts;
		orderid = o->orderid;
		action = gcnew String(o->action);
		volume = o->volume;
		price = o->price;
	}
};

// Input stream
typedef cliext::list<stream_tuple> stream_buffer;

ref struct stream abstract
{
	virtual bool stream_has_inputs() = 0;
	virtual stream_tuple next_input() = 0;
};

ref struct file_stream : public stream
{
	StreamReader^ input_file;
	stream_buffer buffer;
	UInt64 buffer_count;
	UInt64 line;
	UInt64 threshold;
	UInt64 buffer_size;

	file_stream(String^ file_name, UInt64 c)
		: buffer_count(c), line(0), buffer_size(0)
	{
		input_file = gcnew StreamReader(file_name);

		threshold = Convert::ToUInt64(
			Math::Ceiling(Convert::ToDouble(c) / 10));
	}

	inline bool parse_line(Int64 line, String^ data, stream_tuple% r)
	{
		array<wchar_t>^ data_chars = data->ToCharArray();

		Int32 start_idx = 0;
		Int32 end_idx = 0;

		wchar_t action;

		for (Int32 i = 0; i < 5; ++i)
		{
			while ( (end_idx < data_chars->Length) && data_chars[end_idx] != ',' )
				++end_idx;

			if ( start_idx == end_idx )
			{
				Debug::WriteLine(String::Concat("Invalid field ",
					Convert::ToString(i), " line ", Convert::ToString(line)));
				return false;
			}

			if ( (end_idx >= data_chars->Length) && i != 4 )
			{
				Debug::WriteLine(String::Concat("Invalid field ",
					Convert::ToString(i), " line ", Convert::ToString(line)));
				return false;
			}

			switch (i) {
			case 0:
				r.ts = Convert::ToUInt64(
					gcnew String(data_chars, start_idx, (end_idx - start_idx)));
				break;

			case 1:
				r.orderid = Convert::ToInt32(
					gcnew String(data_chars, start_idx, (end_idx - start_idx)));
				break;

			case 2:
				action = data_chars[start_idx];
				if ( !(action == 'B' || action == 'S' ||
						action == 'E' || action == 'F' ||
						action == 'D' || action == 'X' ||
						action == 'C' || action == 'T') )
				{
					Debug::WriteLine(String::Concat("Invalid action ",
						action, " field ", Convert::ToString(i), " line ",
						Convert::ToString(line)));
					return false;
				}

				r.action = gcnew String(action, 1);
				break;

			case 3:
				r.volume = Convert::ToDouble(
					gcnew String(data_chars, start_idx, (end_idx - start_idx)));
				break;

			case 4:
				r.price = Convert::ToDouble(
					gcnew String(data_chars, start_idx, (end_idx - start_idx)));
				break;

			default:
				Debug::WriteLine(String::Concat("Invalid field ",
					Convert::ToString(i), " line ", Convert::ToString(line)));
				break;
			}

			start_idx = ++end_idx;
		}

		return true;
	}

	cliext::pair<bool, stream_tuple> read_tuple()
	{
		stream_tuple r;
		String^ data = input_file->ReadLine();
		++line;

		if ( data == nullptr || !parse_line(line, data, r) )
		{
			Debug::WriteLine(String::Concat(
				"Failed to parse record at line ", Convert::ToString(line)));

			return cliext::make_pair(false, stream_tuple());
		}

		if ( (line % 10000) == 0 ) {
			Console::WriteLine("Read {0} tuples.", line);
		}

		return cliext::make_pair(true, r);
	}

	void buffer_stream()
	{
		while ( buffer_size < buffer_count && input_file != nullptr )
		{
			cliext::pair<bool, stream_tuple> valid_input = read_tuple();
			if ( !valid_input.first ) {
				delete (IDisposable^) input_file;
				input_file = nullptr;
				break;
			}

			buffer.push_back(valid_input.second);
			++buffer_size;
		}
	}

	void init_stream() { buffer_stream(); }

	virtual bool stream_has_inputs() override {
		return buffer_size > 0;
	}

	virtual stream_tuple next_input() override
	{
		if ( buffer_size < threshold && input_file != nullptr )
			buffer_stream();

		stream_tuple r = buffer.front();
		buffer.pop_front();
		--buffer_size;
		return r;
	}
};


/////////////////////////////////////////
//
// Trigger-based execution

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
	Console::WriteLine("{0} s for 'VWAP trigger setup'",
		scriptSw->Elapsed.TotalSeconds);
}

cliext::pair<Boolean, Double> trigger_query(SqlConnection^ dbConn, StreamWriter^ log)
{
	Stopwatch^ querySw = Stopwatch::StartNew();

	SqlCommand^ vwapCmd = gcnew SqlCommand(nullptr, dbConn);
	vwapCmd->CommandText = "SELECT avg(price * volume) FROM vwap";
	Object^ result = vwapCmd->ExecuteScalar();

	querySw->Stop();

	Console::WriteLine("{0} s for 'VWAP trigger iter'",
		querySw->Elapsed.TotalSeconds);

	cliext::pair<Boolean, Double> r;
	if ( result == nullptr )
		r = cliext::make_pair(false, 0.0);
	else
		r = cliext::make_pair(true, Convert::ToDouble(result));

	return r;
}

void vwap_triggers(String^ serverName, String^ dbName,
				   String^ sql_file, String^ directory,
				   Int32 query_freq, stream^ s,
				   StreamWriter^ results, StreamWriter^ log,
				   Boolean dump = false)
{
	String^ dbConnStr = String::Concat(
		"Integrated Security=true", ";Server=", serverName,
		";Initial Catalog=", dbName);

    SqlConnection^ dbConn = gcnew SqlConnection(dbConnStr);

    dbConn->Open();

	if (dbConn->State != ConnectionState::Open)
		Debug::WriteLine("Failed to connect to database engine [" + dbName + "].");

	Console::WriteLine(String::Concat(
		"VWAP triggers using query frequency ",
		Convert::ToString(query_freq)));

	// Execute setup script
	execute_setup_script(dbConn, sql_file);

	// Prepared statements for each type of orderbook action.
	SqlCommand^ insertBidsCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ insertAsksCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ updateBidsVolCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ updateAsksVolCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ deleteBidsCmd = gcnew SqlCommand(nullptr, dbConn);
	SqlCommand^ deleteAsksCmd = gcnew SqlCommand(nullptr, dbConn);

	// Inserts
	// Bids
	insertBidsCmd->CommandText = "INSERT INTO bids VALUES (@ts, @orderid, @p, @v)";

	SqlParameter^ ibtsParam = gcnew SqlParameter("@ts", SqlDbType::BigInt, 0);
	SqlParameter^ iboidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	SqlParameter^ ibpParam = gcnew SqlParameter("@p", SqlDbType::Decimal, 0);
	SqlParameter^ ibvParam = gcnew SqlParameter("@v", SqlDbType::Decimal, 0);

    ibtsParam->Value = gcnew Int64(0);
	iboidParam->Value = gcnew Int64(0);
	ibpParam->Value = 0.0; ibpParam->Precision = 18; ibpParam->Scale = 0;
	ibvParam->Value = 0.0; ibvParam->Precision = 18; ibvParam->Scale = 0;

	array<SqlParameter^>^ insertBidsParams = {ibtsParam, iboidParam, ibpParam, ibvParam};
	insertBidsCmd->Parameters->AddRange(insertBidsParams);
	insertBidsCmd->Prepare();

	// Asks
	insertAsksCmd->CommandText = "INSERT INTO asks VALUES (@ts, @orderid, @p, @v)";

	SqlParameter^ iatsParam = gcnew SqlParameter("@ts", SqlDbType::BigInt, 0);
	SqlParameter^ iaoidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	SqlParameter^ iapParam = gcnew SqlParameter("@p", SqlDbType::Decimal, 0);
	SqlParameter^ iavParam = gcnew SqlParameter("@v", SqlDbType::Decimal, 0);

    iatsParam->Value = gcnew Int64(0);
	iaoidParam->Value = gcnew Int64(0);
	iapParam->Value = 0.0; iapParam->Precision = 18; iapParam->Scale = 0;
	iavParam->Value = 0.0; iavParam->Precision = 18; iavParam->Scale = 0;

	array<SqlParameter^>^ insertAsksParams = {iatsParam, iaoidParam, iapParam, iavParam};
	insertAsksCmd->Parameters->AddRange(insertAsksParams);
	insertAsksCmd->Prepare();

	// Updates 
	// Bids
	updateBidsVolCmd->CommandText = "UPDATE bids SET volume = @new_volume WHERE id = @orderid";

	SqlParameter^ ubnvParam = gcnew SqlParameter("@new_volume", SqlDbType::Decimal, 0);
	SqlParameter^ uboidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	ubnvParam->Value = 0.0; ubnvParam->Precision = 18; ubnvParam->Scale = 0;
	uboidParam->Value = gcnew Int64(0);

	array<SqlParameter^>^ updateBidsParams = {ubnvParam, uboidParam};
	updateBidsVolCmd->Parameters->AddRange(updateBidsParams);
	updateBidsVolCmd->Prepare();

	// Asks
	updateAsksVolCmd->CommandText = "UPDATE asks SET volume = @new_volume WHERE id = @orderid";

	SqlParameter^ uanvParam = gcnew SqlParameter("@new_volume", SqlDbType::Decimal, 0);
	SqlParameter^ uaoidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	uanvParam->Value = 0.0; uanvParam->Precision = 18; uanvParam->Scale = 0;
	uaoidParam->Value = gcnew Int64(0);

	array<SqlParameter^>^ updateAsksParams = {uanvParam, uaoidParam};
	updateAsksVolCmd->Parameters->AddRange(updateAsksParams);
	updateAsksVolCmd->Prepare();

	// Deletes
	// Bids
	deleteBidsCmd->CommandText = "DELETE FROM bids WHERE id = @orderid";
	SqlParameter^ dboidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	dboidParam->Value = gcnew Int64(0);

	array<SqlParameter^>^ deleteBidsParams = {dboidParam};
	deleteBidsCmd->Parameters->AddRange(deleteBidsParams);
	deleteBidsCmd->Prepare();

	// Asks
	deleteAsksCmd->CommandText = "DELETE FROM asks WHERE id = @orderid";
	SqlParameter^ daoidParam = gcnew SqlParameter("@orderid", SqlDbType::BigInt, 0);
	daoidParam->Value = gcnew Int64(0);

	array<SqlParameter^>^ deleteAsksParams = {daoidParam};
	deleteAsksCmd->Parameters->AddRange(deleteAsksParams);
	deleteAsksCmd->Prepare();

	order_book bids;
	order_book asks;

	UInt64 ts;
	Int32 orderid;
	Double v;
	Double p;
	Double new_volume;
	
	Int64 tuple_counter = 0;

	Stopwatch^ triggerSw = Stopwatch::StartNew();

	Stopwatch^ tupleSw = gcnew Stopwatch();
	Double tup_sum = 0.0;

	Stopwatch^ t10kSw = gcnew Stopwatch();
	t10kSw->Start();

	while ( s->stream_has_inputs() )
	{
		stream_tuple in = s->next_input();

		tupleSw->Reset();
		tupleSw->Start();

		ts = in.ts;
		orderid = in.orderid;
		String^ action = in.action;
		v = in.volume;
		p = in.price;

		if (action == "B") {
			// Insert bids
			bids[orderid] = input_tuple(ts, p, v);

			insertBidsCmd->Parameters[0]->Value = ts;
			insertBidsCmd->Parameters[1]->Value = orderid;
			insertBidsCmd->Parameters[2]->Value = p;
			insertBidsCmd->Parameters[3]->Value = v;
			insertBidsCmd->ExecuteNonQuery();
		}

		else if (action == "S") {
			// Insert asks
			asks[orderid] = input_tuple(ts, p, v);

			insertAsksCmd->Parameters[0]->Value = ts;
			insertAsksCmd->Parameters[1]->Value = orderid;
			insertAsksCmd->Parameters[2]->Value = p;
			insertAsksCmd->Parameters[3]->Value = v;
			insertAsksCmd->ExecuteNonQuery();
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			order_book::iterator found = bids.find(orderid);
			if ( found != bids.end() ) {
				UInt64 order_ts = found->second->ts;
				Double order_p = found->second->price;
				Double order_v = found->second->volume;
				new_volume = order_v - v;

				bids[orderid] = input_tuple(order_ts, order_p, new_volume);

				updateBidsVolCmd->Parameters[0]->Value = new_volume;
				updateBidsVolCmd->Parameters[1]->Value = orderid;
				updateBidsVolCmd->ExecuteNonQuery();

				// TODO: should we remove volume from the top of the asks book?
			}
			else
			{
				found = asks.find(orderid);
				if ( found != asks.end() ) {
					UInt64 order_ts = found->second->ts;
					Double order_p = found->second->price;
					Double order_v = found->second->volume;
					new_volume = order_v - v;

					asks[orderid] = input_tuple(order_ts, order_p, new_volume);

					updateAsksVolCmd->Parameters[0]->Value = new_volume;
					updateAsksVolCmd->Parameters[1]->Value = orderid;
					updateAsksVolCmd->ExecuteNonQuery();

					// TODO: should we remove volume from top of the bids book?
				}
				else
					Console::WriteLine("Unmatched order execution {0}", orderid);
			}
		}

		else if (action == "F")
		{
			// Order executed in full
			order_book::iterator found = bids.find(orderid);
			if ( found != bids.end() )
			{
				deleteBidsCmd->Parameters[0]->Value = orderid;
				deleteBidsCmd->ExecuteNonQuery();
				bids.erase(found);

				// TODO: should we remove volume from the top of the asks book?
			}
			else {
				found = asks.find(orderid);
				if ( found != asks.end() )
				{
					deleteAsksCmd->Parameters[0]->Value = orderid;
					deleteAsksCmd->ExecuteNonQuery();
					asks.erase(found);

					// TODO: should we remove volume from the top of the asks book?
				}
			}
		}

		else if (action == "D")
		{
			// Delete from relevant order book.
			order_book::iterator found = bids.find(orderid);
			if ( found != bids.end() )
			{
				deleteBidsCmd->Parameters[0]->Value = orderid;
				deleteBidsCmd->ExecuteNonQuery();
				bids.erase(found);
			}

			else {
				found = asks.find(orderid);
				if ( found != asks.end() )
				{
					deleteAsksCmd->Parameters[0]->Value = orderid;
					deleteAsksCmd->ExecuteNonQuery();
					asks.erase(found);
				}
				else
					Console::WriteLine("Unmatched order deletion {0}", orderid);
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
		
		tupleSw->Stop();
		tup_sum += tupleSw->Elapsed.TotalSeconds;

		if ( (tuple_counter % 10000) == 0 )
		{
			Double total = t10kSw->Elapsed.TotalSeconds;

			Console::WriteLine("Processed {0} tuples.", tuple_counter);
			Console::WriteLine("Exec time {0} {1}", (tup_sum/10000.0), total);

			tup_sum = 0.0;
		}

		if ( (tuple_counter % query_freq) == 0 )
		{
			// Do query
			cliext::pair<Boolean, Double> valid_result = trigger_query(dbConn, log);
			if ( valid_result.first )
				results->WriteLine(Convert::ToString(valid_result.second));

		}
	}

	tupleSw->Stop();
	t10kSw->Stop();

	triggerSw->Stop();
	Console::WriteLine("{0} s for 'VWAP triggers'",
		triggerSw->Elapsed.TotalSeconds);
}

///////////////////////////////////////////
//
// DBToaster

//
// Helpers
void print_bcv_index(bcv_index& sv1)
{
	bcv_index::iterator bcv_it = sv1.begin();
	bcv_index::iterator bcv_end = sv1.end();

	for (; bcv_it != bcv_end; ++bcv_it)
	{
		Console::WriteLine("{0}, {1}, {2}, {3}",
			get<0>(bcv_it->first), get<1>(bcv_it->first),
			get<0>(bcv_it->second), get<1>(bcv_it->second));
	}
}

void print_vwap_index(vwap_index& spv)
{
	vwap_index::iterator spv_it = spv.begin();
	vwap_index::iterator spv_end = spv.end();

	for (; spv_it != spv_end; ++spv_it)
	{
		Console::WriteLine("{0}, {1}, {2}", spv_it->first,
			get<0>(spv_it->second), get<1>(spv_it->second));
	}
}

void print_cvol_index(cvol_index& cvc)
{
	cvol_index::iterator cvc_it = cvc.begin();
	cvol_index::iterator cvc_end = cvc.end();

	for (; cvc_it != cvc_end; ++cvc_it) {
		DBT_HASH_MAP<double, tuple<int, int> >::iterator p_it = cvc_it->second.begin();
		DBT_HASH_MAP<double, tuple<int, int> >::iterator p_end = cvc_it->second.end();

		for (; p_it != p_end; ++p_it)
		{
			Console::WriteLine("{0}, {1}, {2}, {3}",
				cvc_it->first, p_it->first,
				get<0>(p_it->second), get<1>(p_it->second));
		}
	}
}

void debug_bcv_iterator(price_bcv_index::iterator it, price_bcv_index& bcv)
{
	Console::WriteLine("sv1_lb {0} {1} {2}",
		(it == bcv.begin()), (it == bcv.end()),
		(it == bcv.end()? -1.0 : it->first));
}

void print_price_bcv_index(price_bcv_index& bcv)
{
	price_bcv_index::iterator bcv_it = bcv.begin();
	price_bcv_index::iterator bcv_end = bcv.end();

	for (; bcv_it != bcv_end; ++bcv_it)
	{
		Console::WriteLine("{0}, {1}, {2}", bcv_it->first,
			get<0>(bcv_it->second), get<1>(bcv_it->second));
	}
}


//
// Tuple-processing functions.
inline double update_vwap(
	double price, double volume, double& stv,
	price_bcv_index& sv1, vwap_index& spv, cvol_index& cvc,
	bool insert = true)
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

	Console::WriteLine("Before insertions {0}", sv1.size());
	print_price_bcv_index(sv1);
	Console::WriteLine("------");
	#endif


	price_bcv_index new_sv1;
	for (; !(sv1_lb == sv1.begin() || sv1_lb == sv1.end()); --sv1_lb) {
		double sv1_val = get<0>(sv1_lb->second);
		int ref_count = get<1>(sv1_lb->second);

		#ifdef DEBUG
		Console::WriteLine("{0}, {1}, {2}", sv1_lb->first,
			(insert? sv1_val+(ref_count*volume) : sv1_val-(ref_count*volume)),
			ref_count);
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
		Console::WriteLine("{0}, {1}, {2}", sv1_lb->first,
			(insert? final_sv1_val+(ref_count*volume) : final_sv1_val-(ref_count*volume)),
			ref_count);
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
	Console::WriteLine("After insertions {0}", sv1.size());
	print_price_bcv_index(sv1);
	Console::WriteLine(" ----- ");
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
	Console::WriteLine("Before vwap {0}", sv1.size());
	print_price_bcv_index(sv1);
	Console::WriteLine(" ----- ");
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
	Console::WriteLine("Testing shift tracking ";
	for (; shift_it != shift_end; ++shift_it)
		Console::WriteLine("{0}", (*shift_it));
	Console::WriteLine(" ----- ");
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
				Console::WriteLine("RC: {0}", shift_ref_count);

				Console::WriteLine("Final shift rc: {0}, {1}",
					shift_ref_count, old_sv1);
				#endif

				double new_sv1 = (insert? old_sv1 + (shift_ref_count*volume) :
					old_sv1 - (shift_ref_count*volume));
				double new_sum = (insert?
					get<0>(old_sum_cnt) + (shift_ref_count * price * volume) :
					get<0>(old_sum_cnt) - (shift_ref_count * price * volume));
				double new_cnt = (insert? get<1>(old_sum_cnt)+1 : get<1>(old_sum_cnt)-1);
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
	Console::WriteLine("Deferred {0}", new_spv.size());
	print_vwap_index(new_spv);
	Console::WriteLine(" ----- CVC -----");
	print_cvol_index(new_cvc);
	Console::WriteLine(" ----- ");
	#endif

	set<double>::iterator old_it = old_sv1s.begin();
	set<double>::iterator old_end = old_sv1s.end();
	for (; old_it != old_end; ++old_it) {
		spv.erase(*old_it);
		cvc.erase(*old_it);
	}

	#ifdef DEBUG
	Console::WriteLine("After erase {0}", spv.size());
	print_vwap_index(spv);
	Console::WriteLine(" ----- CVC -----");
	print_cvol_index(cvc);
	Console::WriteLine(" ----- ");
	#endif

	// Check if sv1 exists.
	vwap_index::iterator sv1_found = spv.find(sv1_val);

	// Get iterator at sv1-, i.e. spv[sv1[(first p+,v*)]
	// -- note because of duplicates, we should get the iterator at the first (p+,v*)
	price_bcv_index::iterator sv1_minus = sv1.upper_bound(price);

	#ifdef DEBUG
	Console::WriteLine("sv1_minus: {0}, {1}, {2}",
		(sv1_minus == sv1.begin()), (sv1_minus == sv1.end()),
		(sv1_minus == sv1.end()? -1.0 : sv1_minus->first));
	#endif

	while ( sv1_minus->first == price && sv1_minus != sv1.end() )
		++sv1_minus;

	#ifdef DEBUG
	Console::WriteLine("sv1_minus: {0} {1} {2}",
		(sv1_minus == sv1.begin()), (sv1_minus == sv1.end()),
		(sv1_minus == sv1.end()? -1.0 : sv1_minus->first));
	#endif

	vwap_index::iterator spv_lb = spv.end();

	if ( sv1_minus != sv1.end() )
	{
		spv_lb = spv.lower_bound(get<0>(sv1_minus->second));

		#ifdef DEBUG
		Console::WriteLine("sv1-: {0}", get<0>(sv1_minus->second));
		Console::WriteLine("spv-: {0}",
			(spv_lb == spv.end()? -1.0 : spv_lb->first));
		#endif
	}

	if ( insert )
	{
		if ( new_p )
		{
			#ifdef DEBUG
			Console::WriteLine("sv1_found: ", 
				(sv1_found == spv.begin()),
				(sv1_found == spv.end()),
				(sv1_found == spv.end()? -1.0 : sv1_found->first));
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

					if ( existing_rc != 1 )
					{
						Console::WriteLine(
							"Found inconsistent cvc rc {0}, expected 1 (gc {1})",
							existing_rc, existing_gc);

						print_vwap_index(spv);
						Console::WriteLine(" ----- CVC ----- ");
						print_cvol_index(cvc);
						Console::WriteLine(" ----- ");
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
				Console::WriteLine("Failed to find spv for {0}", old_sv1);
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
				Console::WriteLine("Failed to find spv for ", old_sv1);
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvc_found != cvc.end() );
			DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
				old_cvc_found->second.find(price);

			if ( old_cvcp_found == old_cvc_found->second.end() ) {
				Console::WriteLine(
					"Failed to find cvcp for {0}, {1}", old_sv1, price);
				print_price_bcv_index(sv1);
				print_vwap_index(spv);
				print_cvol_index(cvc);
			}

			assert ( old_cvcp_found != old_cvc_found->second.end() );
			check_count = get<0>(old_cvcp_found->second);

			#ifdef DEBUG
			Console::WriteLine("check_count {0}, {1}",
				check_count, get<1>(old_sum_cnt));
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
					Console::WriteLine(
						"Found inconsistent cvc rc {0}, expected 1 (gc {1})",
						existing_rc, existing_gc);
					print_vwap_index(spv);
					Console::WriteLine(" ----- CVC ----- ");
					print_cvol_index(cvc);
					Console::WriteLine(" ----- ");
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
						Console::WriteLine(
							"Found inconsistent cvc rc {0}, expected {1} (gc {2})",
							existing_rc, sv1_ref_count, existing_gc);
						print_vwap_index(spv);
						Console::WriteLine(" ----- CVC ----- ");
						print_cvol_index(cvc);
						Console::WriteLine(" ----- ");
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
			Console::WriteLine("Failed to find spv for {0}", old_sv1);
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
		}

		// Check if we delete from spv
		assert ( spv.find(old_sv1) != spv.end() );
		tuple<double, double>& old_sum_cnt = spv[old_sv1];

		int check_count = 0;

		cvol_index::iterator old_cvc_found = cvc.find(old_sv1);
		if ( old_cvc_found == cvc.end() ) {
			Console::WriteLine("Failed to find spv for {0}", old_sv1);
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvc_found != cvc.end() );
		DBT_HASH_MAP<double, tuple<int, int> >::iterator old_cvcp_found =
			old_cvc_found->second.find(price);

		if ( old_cvcp_found == old_cvc_found->second.end() ) {
			Console::WriteLine("Failed to find cvcp for {0}, {1}", old_sv1, price);
			print_price_bcv_index(sv1);
			print_vwap_index(spv);
			print_cvol_index(cvc);
		}

		assert ( old_cvcp_found != old_cvc_found->second.end() );
		check_count = get<0>(old_cvcp_found->second);

		#ifdef DEBUG
		Console::WriteLine("check_count {0}, {1}", check_count, get<1>(old_sum_cnt));
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
				Console::WriteLine(
					"Found inconsistent cvc rc {0}, expected {1} (gc {2})",
					existing_rc, old_ref_count, existing_gc);
				print_vwap_index(spv);
				Console::WriteLine(" ----- CVC ----- ");
				print_cvol_index(cvc);
				Console::WriteLine(" ----- ");
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
						Console::WriteLine(
							"Found inconsistent cvc rc {0}, expected {1} (gc {2})",
							existing_rc, sv1_ref_count, existing_gc);
						print_vwap_index(spv);
						Console::WriteLine(" ----- CVC ----- ");
						print_cvol_index(cvc);
						Console::WriteLine(" ----- ");
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
	Console::WriteLine("Before merge {0}", spv.size());
	print_vwap_index(spv);
	Console::WriteLine(" ----- CVC ----- ");
	print_cvol_index(cvc);
	Console::WriteLine(" ----- ");
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

					if ( existing_rc != sv1_ref_count )
					{
						Console::WriteLine(
							String::Concat(
								"Found inconsistent cvc for sv1 {0} p {1} rc {2},",
								" expected {3} (gc {4})"),
							ncvc_it->first, ncvcp_it->first, existing_rc,
							get<1>(ncvcp_it->second), existing_gc);

						print_vwap_index(spv);
						Console::WriteLine(" ----- CVC ----- ");
						print_cvol_index(cvc);
						Console::WriteLine(" ----- ");
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
	Console::WriteLine("After merge {0}", spv.size());
	print_vwap_index(spv);
	Console::WriteLine(" ----- CVC ----- ");
	print_cvol_index(cvc);
	Console::WriteLine(" ----- ");
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
	Console::WriteLine("At return {0}", spv.size());
	print_vwap_index(spv);
	Console::WriteLine(" ----- CVC ----- ");
	print_cvol_index(cvc);
	Console::WriteLine(" ----- ");
	#endif

	return result;
}


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


void vwap_stream(stream^ s,
				 StreamWriter^ results, StreamWriter^ log,
				 StreamWriter^ stats, Boolean dump = false)
{
	typedef DBT_HASH_MAP<int, tuple<double, double> > order_ids;

	order_ids bid_orders;
	order_ids ask_orders;

	double result = 0.0;

	UInt64 tuple_counter = 0;

	Stopwatch^ streamSw = Stopwatch::StartNew();

	while ( s->stream_has_inputs() )
	{
		++tuple_counter;

		stream_tuple in = s->next_input();

		UInt64 ts = in.ts;
		Int32 order_id = in.orderid;
		String^ action = in.action;
		Double price = in.price;
		Double volume = in.volume;

		if (action == "B") {
			// Insert bids
			tuple<double, double> pv = make_tuple(price, volume);
			bid_orders[order_id] = pv;

			result = update_vwap(
				price, volume, stv_bids, sv1_pbids, spv_bids, cvc_bids);
		}

		else if (action == "S")
		{
			// Insert asks
			ask_orders[order_id] = make_tuple(price, volume);
			result = update_vwap(
				price, volume, stv_asks, sv1_pasks, spv_asks, cvc_asks);
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			double delta_volume = in.volume;

			order_ids::iterator bid_found = bid_orders.find(order_id);
			if ( bid_found != bid_orders.end() )
			{
				double order_price =  get<0>(bid_found->second);
				double order_volume =  get<1>(bid_found->second);
				double new_volume = order_volume - delta_volume;

				// For now, we handle updates as delete, insert
				tuple<double, double> old_pv = make_tuple(order_price, order_volume);
				tuple<double, double> new_pv = make_tuple(price, new_volume);

				bid_orders.erase(bid_found);

				result = update_vwap(order_price, order_volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);

				if ( new_volume > 0 )
				{
					bid_orders[order_id] = new_pv;
					result = update_vwap(price, new_volume,
						stv_bids, sv1_pbids, spv_bids, cvc_bids);
				}
			}
			else
			{
				order_ids::iterator ask_found = ask_orders.find(order_id);
				if ( ask_found != ask_orders.end() )
				{
					double order_price =  get<0>(ask_found->second);
					double order_volume =  get<1>(ask_found->second);
					double new_volume = order_volume - delta_volume;

					// For now, we handle updates as delete, insert
					ask_orders.erase(ask_found);

					result = update_vwap(order_price, order_volume,
						stv_asks, sv1_pasks, spv_asks, cvc_asks, false);

					if ( new_volume > 0 )
					{
						result = update_vwap(price, new_volume,
							stv_asks, sv1_pasks, spv_asks, cvc_asks);
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
				double order_price =  get<0>(bid_found->second);
				double order_volume =  get<1>(bid_found->second);

				bid_orders.erase(bid_found);
				result = update_vwap(order_price, order_volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);
			}
			else
			{
				order_ids::iterator ask_found = ask_orders.find(order_id);
				if ( ask_found != ask_orders.end() )
				{
					double order_price = get<0>(ask_found->second);
					double order_volume = get<1>(ask_found->second);
					ask_orders.erase(ask_found);
					result = update_vwap(order_price, order_volume,
						stv_asks, sv1_pasks, spv_asks, cvc_asks, false);
				}
			}
		}

		else if (action == "D")
		{
			// Delete from relevant order book.
			order_ids::iterator bid_found = bid_orders.find(order_id);
			if ( bid_found != bid_orders.end() )
			{
				double order_price =  get<0>(bid_found->second);
				double order_volume =  get<1>(bid_found->second);

				bid_orders.erase(bid_found);

				result = update_vwap(order_price, order_volume,
					stv_bids, sv1_pbids, spv_bids, cvc_bids, false);
			}
			else
			{
				order_ids::iterator ask_found = ask_orders.find(order_id);
				if ( ask_found != ask_orders.end() )
				{
					double order_price = get<0>(ask_found->second);
					double order_volume = get<1>(ask_found->second);

					ask_orders.erase(ask_found);
					result = update_vwap(order_price, order_volume,
						stv_asks, sv1_pasks, spv_asks, cvc_asks, false);
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
		results->WriteLine(Convert::ToString(result));
		#endif

		#ifdef DEBUG
		print_price_bcv_index(sv1_pbids);
		print_vwap_index(spv_bids);
		#endif

	}

	streamSw->Stop();

	Console::WriteLine("{0} s for 'VWAP toasted QP'",
		streamSw->Elapsed.TotalSeconds);
}


/////////////////////////////////////
//
// DBToaster exploiting monotonicity

void print_select_sv1_index(select_sv1_index& m)
{
	select_sv1_index::iterator m_it = m.begin();
	select_sv1_index::iterator m_end = m.end();

	for (; m_it != m_end; ++m_it)
		Console::WriteLine("{0} {1}", m_it->first, m_it->second);
}

void print_spv_index(spv_index& q)
{
	spv_index::iterator q_it = q.begin();
	spv_index::iterator q_end = q.end();

	for (; q_it != q_end; ++q_it)
		Console::WriteLine("{0} {1}", q_it->first, q_it->second);
}

inline double update_vwap_monotonic(double price, double volume,
	select_sv1_index& m, spv_index& q,
	tuple<double, double>& sv1_at_pmin,
	tuple<double, double>& spv_at_pmin,
	bool bids, bool insert = true)
{
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
			Console::WriteLine(
				"{0} no q[p_min] found for p_min = {1}",
				(bids? "Bids " : "Asks "), p_min_found->first);
			print_select_sv1_index(m);
		}
	}
	else {
		Console::WriteLine("{0} no m[p] > 0 found!", (bids? "Bids " : "Asks "));
		Console::WriteLine("New (p,v) {0}, {1}", price, volume);
		print_select_sv1_index(m);
	}

	return result;
}

void vwap_stream_monotonic(stream^ s,
				 StreamWriter^ results, StreamWriter^ log,
				 StreamWriter^ stats, Boolean dump = false)
{
	typedef DBT_HASH_MAP<int, tuple<double, double> > order_ids;

	order_ids bid_orders;
	order_ids ask_orders;

	Double result = 0.0;
	UInt64 tuple_counter = 0;

	Stopwatch^ streamSw = Stopwatch::StartNew();
	Stopwatch^ totalSw = Stopwatch::StartNew();
	Stopwatch^ tup10kSw = Stopwatch::StartNew();

	Console::WriteLine("Running stream...");

	while ( s->stream_has_inputs() )
	{

		stream_tuple in = s->next_input();

		UInt64 ts = in.ts;
		Int32 order_id = in.orderid;
		String^ action = in.action;
		Double price = in.price;
		Double volume = in.volume;

		if (action == "B") {
			// Insert bids
			tuple<double, double> pv = make_tuple(price, volume);
			bid_orders[order_id] = pv;

			result = update_vwap_monotonic(
				price, volume, m_bids, q_bids,
				sv1_at_pmin_bids, spv_at_pmin_bids, true);
		}

		else if (action == "S")
		{
			// Insert asks
			ask_orders[order_id] = make_tuple(price, volume);
			result = update_vwap_monotonic(
				price, volume, m_asks, q_asks,
				sv1_at_pmin_asks, spv_at_pmin_asks, false);
		}

		else if (action == "E")
		{
			// Partial execution based from order book according to order id.
			double delta_volume = in.volume;

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
					sv1_at_pmin_bids, spv_at_pmin_bids, true, false);

				if ( new_volume > 0 )
				{
					bid_orders[order_id] = new_pv;
					result = update_vwap_monotonic(
						price, new_volume, m_bids, q_bids,
						sv1_at_pmin_bids, spv_at_pmin_bids, true);
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
						sv1_at_pmin_asks, spv_at_pmin_asks, false, false);

					if ( new_volume > 0 ) {
						result = update_vwap_monotonic(
							price, new_volume, m_asks, q_asks,
							sv1_at_pmin_asks, spv_at_pmin_asks, false);
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

				result = update_vwap_monotonic(
					price, volume, m_bids, q_bids,
					sv1_at_pmin_bids, spv_at_pmin_bids, true, false);
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
						sv1_at_pmin_asks, spv_at_pmin_asks, false, false);
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

				result = update_vwap_monotonic(
					price, volume, m_bids, q_bids,
					sv1_at_pmin_bids, spv_at_pmin_bids, true, false);
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
						sv1_at_pmin_asks, spv_at_pmin_asks, false, false);
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

		++tuple_counter;

		if ( (tuple_counter % 10000) == 0 )
		{
			tup10kSw->Stop();
			

			Console::WriteLine("Processed {0} tuples.", tuple_counter);
			Console::WriteLine("Exec time {0} {1}",
				tup10kSw->Elapsed.TotalSeconds / 10000.0,
				totalSw->Elapsed.TotalSeconds);

			tup10kSw->Reset();
			tup10kSw->Start();
		}

		#ifdef OUTPUT
		results->WriteLine("{0}", result);
		#endif
	}

	streamSw->Stop();
	Console::WriteLine("{0} s for 'VWAP toasted QP (monotonic)'",
		streamSw->Elapsed.TotalSeconds);

}

//////////////////////////////////////
//
// Top level

void print_usage()
{
	Console::WriteLine(String::Concat(
		"Usage: vwap <app mode> <data dir> <query freq>",
		"<in file> <out file> <result file> <stats file> [matviews file]"));
}

int main(array<String ^> ^args)
{
	if ( args->Length < 8 ) {
		print_usage();
		exit(1);
	}

	Console::WriteLine(String::Concat("# args: ", Convert::ToString(args->Length)));

	String^ serverName = "DBSERVER\\DBTOASTER";
	String^ dbName = "orderbook";

	String^ app_mode = args[0];
	String^ directory = args[1];
	Int32 query_freq = System::Convert::ToInt32(args[2]);
	String^ input_file_name = args[3];
	String^ log_file_name = args[4];
	String^ results_file_name = args[5];
	String^ stats_file_name = args[6];
	String^ script_file_name = args->Length > 7? args[7] : "";
	Int32 copy_freq = args->Length > 8?
		Convert::ToInt32(args[8]) : 1500000;

	StreamWriter^ log = gcnew StreamWriter(log_file_name);
	StreamWriter^ out = gcnew StreamWriter(results_file_name);
	StreamWriter^ stats = gcnew StreamWriter(stats_file_name);

	file_stream^ f = gcnew file_stream(input_file_name, 15000000);
	f->init_stream();

	if ( app_mode == "toasted" )
		vwap_stream_monotonic(f, out, log, stats);

	else if ( app_mode == "triggers" )
	{
		vwap_triggers(serverName, dbName, script_file_name,
			directory, query_freq, f, out, log);
	}

	else {
		Console::WriteLine(
			String::Concat("Invalid app mode: '" , app_mode, "'"));
	}

	log->Flush(); log->Close();
	out->Flush(); out->Close();
	stats->Flush(); stats->Close();

    return 0;
}
