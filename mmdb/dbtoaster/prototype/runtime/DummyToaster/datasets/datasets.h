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
    namespace DemoDatasets
    {
        using namespace std;
        using namespace tr1;
        using namespace boost;

        //
        // Basic stream interface

        template<typename tuple>
        struct Stream
        {
            typedef list<tuple> StreamBuffer;
            virtual void initStream() = 0;
            virtual bool streamHasInputs() = 0;
            virtual tuple nextInput() = 0;
            virtual unsigned int getBufferSize() = 0;
        };

        //////////////////////////////
        //
        // VWAP data

        // Timestamp, order id, action, volume, price
        struct VwapTuple {
            double t;
            int id;
            int c_id;
            string action;
            double volume;
            double price;
        };

        struct VwapFileStream : public Stream<VwapTuple>
        {
            ifstream* inputFile;
            StreamBuffer buffer;
            unsigned int bufferCount;
            unsigned long line;
            unsigned long threshold;
            unsigned long bufferSize;

            VwapFileStream(string fileName, unsigned int c)
                : bufferCount(c), line(0), bufferSize(0)
            {
                inputFile = new ifstream(fileName.c_str());
                if ( !(inputFile->good()) )
                    cerr << "Failed to open file " << fileName << endl;

                threshold =
                    static_cast<long>(ceil(static_cast<double>(c) / 10));
            }

            inline bool parseLine(int line, char* data, VwapTuple& r)
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
                        r.t = atof(start);
                        break;

                    case 1:
                        r.id = atoi(start);
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

                        r.action = action;
                        break;

                    case 3:
                        r.volume = atof(start);
                        break;

                    case 4:
                        r.price = atof(start);
                        break;

                    default:
                        cerr << "Invalid field " << i << " line " << line << endl;
                        break;
                    }

                    start = ++end;
                }

                return true;
            }

            tuple<bool, VwapTuple> read_tuple()
            {
                char buf[256];
                inputFile->getline(buf, sizeof(buf));
                ++line;

                VwapTuple r;
                if ( !parseLine(line, buf, r) ) {
                    cerr << "Failed to parse record at line " << line << endl;
                    return make_tuple(false, VwapTuple());
                }

                return make_tuple(true, r);
            }

            void bufferStream()
            {
                while ( bufferSize < bufferCount && inputFile->good() )
                {
                    tuple<bool, VwapTuple> validInput = read_tuple();
                    if ( !get<0>(validInput) )
                        break;

                    buffer.push_back(get<1>(validInput));
                    ++bufferSize;
                }
            }

            void initStream() { bufferStream(); }

            bool streamHasInputs() {
                return bufferSize > 0;
            }

            VwapTuple nextInput()
            {
                if ( bufferSize < threshold && inputFile->good() )
                    bufferStream();

                VwapTuple r = buffer.front();
                buffer.pop_front();
                --bufferSize;
                return r;
            }

            unsigned int getBufferSize() { return bufferSize; }
        };


        //////////////////////////////
        //
        // SSB data

        //
        //
        // Field parsers

        inline void parseLineitemField(
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

        inline void parseOrderField(int field, unsigned long line, order& r, char* data)
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

        inline void parsePartField(int field, unsigned long line, part& r, char* data)
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

        inline void parseCustomerField(int field, unsigned long line, customer& r, char* data)
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

        inline void parseSupplierField(int field, unsigned long line, supplier& r, char* data)
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

        inline void parseNationField(int field, unsigned long line, nation& r, char* data)
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

        inline void parseRegionField(int field, unsigned long line, region& r, char* data)
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


        template<typename T>
            struct SsbFileStream : public Stream<T>
        {
            typedef boost::function<void (int, int, T&, char*)> parseFieldFn;
            typedef list<T> StreamBuffer;

            parseFieldFn fieldParser;

            string fileName;
            ifstream* inputFile;
            StreamBuffer buffer;
            unsigned long line;

            static const char delimiter = '|';

            unsigned int fieldCount;

            unsigned int bufferCount;
            unsigned int bufferSize;
            unsigned long threshold;
            unsigned int lineSize;
            bool finishedReading;

            SsbFileStream(string fn, parseFieldFn parse_fn,
                      unsigned int fields, unsigned int c, unsigned int ls)
            : fieldParser(parse_fn), fileName(fn),
                line(0), fieldCount(fields), bufferCount(c),
                bufferSize(0), lineSize(ls),
                finishedReading(false)
                {
                    assert ( fieldCount > 0 );
                    assert ( lineSize > 32 );

                    inputFile = new ifstream(fileName.c_str());
                    if ( !(inputFile->good()) )
                        cerr << "Failed to open file " << fileName << endl;

                    threshold =
                        static_cast<long>(ceil(static_cast<double>(c) / 10));
                }

            ~SsbFileStream() {
                inputFile->close();
                delete inputFile;
            }

            tuple<bool, T> read_tuple()
            {
                char buf[lineSize];
                inputFile->getline(buf, lineSize);
                ++line;

                char* start = &(buf[0]);
                char* end = start;

                //cout << "Data: " << string(buf) << endl;

                T r;
                for (int i = 0; i < fieldCount; ++i)
                {
                    while ( *end && *end != delimiter ) ++end;
                    if ( start == end ) {
                        cerr << "Invalid field " << fileName
                             << ": " << line << " " << i << endl;
                        return make_tuple(false, T());
                    }
                    if ( *end == '\0' && i != (fieldCount - 1) ) {
                        cerr << "Invalid field " << fileName
                             << ": " << line << " " << i << endl;
                        return make_tuple(false, T());
                    }
                    *end = '\0';
                    fieldParser(i, line, r, start);
                    start = ++end;
                }

                //cout << "Read: " << r.as_string() << endl;

                return make_tuple(true, r);
            }

            void bufferStream()
            {
                bufferSize = buffer.size();
                while ( bufferSize < bufferCount && inputFile->good() )
                {
                    tuple<bool, T> validInput = read_tuple();
                    if ( !get<0>(validInput) )
                        break;

                    buffer.push_back(get<1>(validInput));
                    ++bufferSize;
                }

                if ( !inputFile->good() ) finishedReading = true;
            }

            void initStream() { bufferStream(); }

            bool streamHasInputs() {
                return !buffer.empty();
            }

            T nextInput()
            {
                if ( !finishedReading && bufferSize < threshold )
                    bufferStream();

                T r = buffer.front();
                buffer.pop_front();
                --bufferSize;
                return r;
            }

            unsigned int getBufferSize() { return bufferSize; }
        };

        //
        // File I/O typedefs

        typedef SsbFileStream<lineitem>  LineitemStream;
        typedef SsbFileStream<order>     OrderStream;
        typedef SsbFileStream<part>      PartStream;
        typedef SsbFileStream<customer>  CustomerStream;
        typedef SsbFileStream<supplier>  SupplierStream;
        typedef SsbFileStream<nation>    NationStream;
        typedef SsbFileStream<region>    RegionStream;


        //////////////////////////////
        //
        // LinearRoad data

        typedef LRData LinearRoadTuple;

        struct LinearRoadFileStream : public Stream<LinearRoadTuple>
        {
            ifstream* inputFile;
            StreamBuffer buffer;
            unsigned int bufferCount;
            unsigned long line;
            unsigned long threshold;

            unsigned int bufferSize;
            bool finishedReading;

            LinearRoadFileStream(string fileName, unsigned int c)
                : bufferCount(c), line(0), bufferSize(0), finishedReading(false)
            {
                inputFile = new ifstream(fileName.c_str());
                if ( !(inputFile->good()) )
                    cerr << "Failed to open file " << fileName << endl;

                threshold =
                    static_cast<long>(ceil(static_cast<double>(c) / 10));
            }

            tuple<bool, LinearRoadTuple> read_tuple()
            {
                char buf[256];
                inputFile->getline(buf, sizeof(buf));
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
                        return make_tuple(false, LinearRoadTuple());
                    }
                    if ( *end == '\0' && i != 14 ) {
                        cerr << "Invalid field " << line << " " << i << endl;
                        return make_tuple(false, LinearRoadTuple());
                    }
                    *end = '\0';
                    int f = atoi(start);
                    start = ++end;
                    lr_data_fields[i] = f;
                }

                LinearRoadTuple r(lr_data_fields);
                return make_tuple(true, r);
            }

            void bufferStream()
            {
                bufferSize = buffer.size();
                while ( bufferSize < bufferCount && inputFile->good() )
                {
                    tuple<bool, LinearRoadTuple> validInput = read_tuple();
                    if ( !get<0>(validInput) )
                        break;

                    buffer.push_back(get<1>(validInput));
                    ++bufferSize;
                }

                if ( !inputFile->good() ) finishedReading = true;
            }

            void initStream() { bufferStream(); }

            bool streamHasInputs() {
                return !buffer.empty();
            }

            LinearRoadTuple nextInput()
            {
                if ( !finishedReading && bufferSize < threshold )
                    bufferStream();

                LinearRoadTuple r = buffer.front();
                buffer.pop_front();
                --bufferSize;
                return r;
            }

            unsigned int getBufferSize() { return bufferSize; }
        };
    };
};

#endif

