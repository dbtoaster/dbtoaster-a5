// vwap.cpp : main project file.

#include <stdlib.h>

#include <cliext/hash_map>
#include <cliext/hash_set>
#include <cliext/list>
#include <cliext/utility>

using namespace System;
using namespace System::Data;
using namespace System::Data::SqlClient;
using namespace System::Diagnostics;
using namespace System::IO;

#define DBT_HASH_MAP cliext::hash_map
#define DBT_HASH_SET cliext::hash_set

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

void vwap_stream(stream^ s,
				 StreamWriter^ results, StreamWriter^ log,
				 StreamWriter^ stats, Boolean dump = false)
{
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
		vwap_stream(f, out, log, stats);

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
