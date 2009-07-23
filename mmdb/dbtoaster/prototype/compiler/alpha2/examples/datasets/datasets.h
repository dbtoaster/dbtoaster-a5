#ifndef DEMO_DATASETS_H
#define DEMO_DATASETS_H

// STL headers
#include <iostream>
#include <fstream>
#include <list>
#include <vector>

// Unix headers
#include <cmath>

// TR1 headers
#include <tr1/tuple>

// Library headers
#include <boost/any.hpp>
#include <boost/function.hpp>

// User code headers
#include "tpch.h"
#include "linearroad.h"

namespace DBToaster
{
    using namespace std;
    using namespace tr1;
    using namespace boost;

    //
    // Basic stream interface

    template<typename tuple>
    struct stream
    {
        typedef list<tuple> stream_buffer;
        virtual bool stream_has_inputs() = 0;
        virtual tuple next_input() = 0;
	virtual unsigned int get_buffer_size() = 0;
    };

    //////////////////////////////
    //
    // VWAP data

    // Timestamp, order id, action, volume, price
    typedef tuple<double, int, string, double, double> vwap_tuple;

    struct vwap_file_stream : public stream<vwap_tuple>
    {
        ifstream* input_file;
        stream_buffer buffer;
        unsigned int buffer_count;
        unsigned long line;
        unsigned long threshold;
        unsigned long buffer_size;

        vwap_file_stream(string file_name, unsigned int c)
            : buffer_count(c), line(0), buffer_size(0)
        {
            input_file = new ifstream(file_name.c_str());
            if ( !(input_file->good()) )
                cerr << "Failed to open file " << file_name << endl;

            threshold =
                static_cast<long>(ceil(static_cast<double>(c) / 10));
        }

        inline bool parse_line(int line, char* data, vwap_tuple& r)
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

        tuple<bool, vwap_tuple> read_tuple()
        {
            char buf[256];
            input_file->getline(buf, sizeof(buf));
            ++line;

            vwap_tuple r;
            if ( !parse_line(line, buf, r) ) {
                cerr << "Failed to parse record at line " << line << endl;
                return make_tuple(false, vwap_tuple());
            }

            return make_tuple(true, r);
        }

        void buffer_stream()
        {
            while ( buffer_size < buffer_count && input_file->good() )
            {
                tuple<bool, vwap_tuple> valid_input = read_tuple();
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

        vwap_tuple next_input()
        {
            if ( buffer_size < threshold && input_file->good() )
                buffer_stream();

            vwap_tuple r = buffer.front();
            buffer.pop_front();
            --buffer_size;
            return r;
        }

	unsigned int get_buffer_size() { return buffer_size; }
    };


    //////////////////////////////
    //
    // SSB data

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


    typedef struct {
	int type;
        boost::any data;
    } ssb_tuple;

    template<typename T>
    struct ssb_file_stream : public stream<ssb_tuple>
    {
	typedef boost::function<void (int, int, T&, char*)> parse_field_fn;
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

        ssb_file_stream(string fn, parse_field_fn parse_fn,
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

	~ssb_file_stream() {
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

	ssb_tuple next_input()
	{
            if ( !finished_reading && buffer_size < threshold )
                buffer_stream();

            T next = buffer.front();
            ssb_tuple r;
            r.type = get_tuple_type(next);
            r.data = next;
            buffer.pop_front();
            --buffer_size;
            return r;
	}

	unsigned int get_buffer_size() { return buffer_size; }
    };

    //
    // File I/O typedefs

    typedef ssb_file_stream<lineitem>  lineitem_stream;
    typedef ssb_file_stream<order>     order_stream;
    typedef ssb_file_stream<part>      part_stream;
    typedef ssb_file_stream<customer>  customer_stream;
    typedef ssb_file_stream<supplier>  supplier_stream;
    typedef ssb_file_stream<nation>    nation_stream;
    typedef ssb_file_stream<region>    region_stream;


    //////////////////////////////
    //
    // LinearRoad data

    typedef LRData linear_road_tuple;

    struct linear_road_file_stream : public stream<linear_road_tuple>
    {
	ifstream* input_file;
	stream_buffer buffer;
	unsigned int buffer_count;
	unsigned long line;
	unsigned long threshold;

        unsigned int buffer_size;
        bool finished_reading;

        linear_road_file_stream(string file_name, unsigned int c)
            : buffer_count(c), line(0), buffer_size(0), finished_reading(false)
	{
            input_file = new ifstream(file_name.c_str());
            if ( !(input_file->good()) )
                cerr << "Failed to open file " << file_name << endl;

            threshold =
                static_cast<long>(ceil(static_cast<double>(c) / 10));
	}

	tuple<bool, linear_road_tuple> read_tuple()
	{
            char buf[256];
            input_file->getline(buf, sizeof(buf));
            ++line;

            //string row(buf);
            char* start = &(buf[0]);
            char* end = start;

            vector<int> lr_data_fields(15);
            for (int i = 0; i < 15; ++i)
            {
                while ( *end && *end != ',' ) ++end;
                if ( start == end ) {
                    cerr << "Invalid field " << line << " " << i << endl;
                    return make_tuple(false, linear_road_tuple());
                }
                if ( *end == '\0' && i != 14 ) {
                    cerr << "Invalid field " << line << " " << i << endl;
                    return make_tuple(false, linear_road_tuple());
                }
                *end = '\0';
                int f = atoi(start);
                start = ++end;
                lr_data_fields[i] = f;
            }

            linear_road_tuple r(lr_data_fields);
            return make_tuple(true, r);
	}

	void buffer_stream()
	{
            buffer_size = buffer.size();
            while ( buffer_size < buffer_count && input_file->good() )
            {
                tuple<bool, linear_road_tuple> valid_input = read_tuple();
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

	linear_road_tuple next_input()
	{
            if ( !finished_reading && buffer_size < threshold )
                buffer_stream();

            linear_road_tuple r = buffer.front();
            buffer.pop_front();
            --buffer_size;
            return r;
	}

        unsigned int get_buffer_size() { return buffer_size; }
    };
};

#endif

