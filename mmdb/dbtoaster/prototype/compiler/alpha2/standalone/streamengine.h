#ifndef DBTOASTER_COMMON_H
#define DBTOASTER_COMMON_H

#include <iostream>
#include <list>
#include <map>
#include <vector>

#include <tr1/tuple>

#include <boost/any.hpp>
#include <boost/function.hpp>

#include "datasets/datasets.h"

namespace DBToaster
{
    using namespace std;
    using namespace tr1;
    using namespace boost;

    enum dmltype { insertTuple, deleteTuple };

    typedef struct
    {
        dmltype type;
        int id;
        boost::any data;
    } dbtoaster_tuple;

    struct multiplexer
    {
        typedef stream<boost::any> input_stream;
        vector<input_stream*> inputs;
        map<input_stream*, int> input_ids;
        int stream_step;
        int num_streams;

        tuple<int, int> running_stream;

        multiplexer(int seed, int stepsize) : stream_step(stepsize) {}

        // Note: not thread-safe
        void add_stream(input_stream* s, int id)
        {
            inputs.push_back(s);
            input_ids[s] = id;
            num_streams = inputs.size();
        }

        void remove_stream(input_stream* s)
        {
            vector<input_stream*>::iterator s_found = find(inputs.begin(), inputs.end(), s);
            if ( s_found != inputs.end() ) {
                input_ids.erase(*s_found);
                inputs.erase(s_found);
            }

            num_streams = inputs.size();
        }

        bool stream_has_inputs()
        {
            return !(inputs.empty());
        }

        dbtoaster_tuple next_input()
        {
            assert ( stream_has_inputs() );

            int current_stream = get<0>(running_stream);
            int remaining_count = get<1>(running_stream);

            while ( remaining_count == 0 ) {
                current_stream = (int) (num_streams * (rand() / (RAND_MAX + 1.0)));
                remaining_count = (int) (stream_step * (rand() / (RAND_MAX + 1.0)));
            }

            input_stream* s = inputs[current_stream];
            dbtoaster_tuple r;
            r.type = insertTuple;
            r.id = input_ids[s];
            r.data = s->next_input();

            if ( !s->stream_has_inputs() )
            {
                cout << "Done with stream " << current_stream << endl;
                inputs.erase(inputs.begin()+current_stream);
                --num_streams;

                remaining_count = 0;
            }
            else
                --remaining_count;

            running_stream = make_tuple(current_stream, remaining_count);
            return r;
        }

        unsigned int get_buffer_size()
        {
            vector<input_stream*>::iterator ins_it = inputs.begin();
            vector<input_stream*>::iterator ins_end = inputs.end();

            unsigned int r = 0;
            for (; ins_it != ins_end; ++ins_it)
                r += (*ins_it)->get_buffer_size();

            return r;
        }
    };

    typedef struct 
    {
        typedef boost::function<void (boost::any)> handler;
        typedef tuple<int, int> key;
        typedef map<key, handler> handler_map;
        handler_map handlers;

        void add_handler(int streamid, dmltype type, handler h)
        {
            key k = make_tuple(streamid, type);
            if ( handlers.find(k) == handlers.end() )
                handlers[k] = h;
            else {
                cout << "Found existing " << (type == insertTuple? "insert" : "delete")
                     << " handler for stream id " << streamid << endl;
            }
        }

        void remove_handler(int streamid, dmltype type)
        {
            key k = make_tuple(streamid, type);
            handler_map::iterator h_it = handlers.find(k);
            if ( h_it != handlers.end() )
                handlers.erase(h_it);
            else {
                cout << "Could not find " << (type == insertTuple? "insert" : "delete")
                     << " handler for stream id " << streamid << endl;
            }
        }

        void dispatch(dbtoaster_tuple& tuple)
        {
            key k = make_tuple(tuple.id, tuple.type);
            handler_map::iterator h_it = handlers.find(k);
            if ( h_it != handlers.end() ) {
                (h_it->second)(tuple.data);
            }
        }

    } dispatcher;
};

#endif
