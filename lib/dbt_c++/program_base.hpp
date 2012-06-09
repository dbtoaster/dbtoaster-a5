/*
 * program_base.hpp
 *
 *  Created on: May 8, 2012
 *      Author: daniel
 */

#ifndef DBTOASTER_PROGRAM_BASE_H
#define DBTOASTER_PROGRAM_BASE_H


#include <fstream>
#include <map>
#include <utility>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/array.hpp>
#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>
#include <boost/fusion/tuple.hpp>
#include <boost/fusion/include/fold.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/lambda/bind.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/member.hpp>

#include "iprogram.hpp"
#include "util.hpp"
#include "streams.hpp"
#include "runtime.hpp"
#include "standard_adaptors.hpp"


using namespace ::std;
using namespace ::boost;
using namespace ::boost::chrono;
using namespace ::boost::filesystem;
using namespace ::boost::serialization;
using namespace ::boost::fusion;
using namespace ::boost::lambda;
using namespace ::boost::multi_index;
using namespace ::dbtoaster;
using namespace ::dbtoaster::adaptors;
using namespace ::dbtoaster::datasets;
using namespace ::dbtoaster::runtime;
using namespace ::dbtoaster::streams;
using namespace ::dbtoaster::util;

#ifdef DBT_PROFILE
#include "statistics.hpp"
using namespace ::dbtoaster::statistics;
#endif


#define BOOST_SERIALIZATION_NVP_OF_PTR( name )  \
	boost::serialization::make_nvp(BOOST_PP_STRINGIZE(name), *name)

namespace dbtoaster {

template<class TLQ_T>
class ProgramBase : public IProgram<TLQ_T> {
public:

	typedef boost::function<void (boost::archive::xml_oarchive&)> serialize_fn_t;
	struct serializer{
		template<class T>
		static
		boost::archive::xml_oarchive& fn(
				boost::archive::xml_oarchive& oa,
				const nvp<T>& t)
		{
			return (oa << t);
		}
	};

	struct map_t{
		serialize_fn_t serialize_fn;

		bool isOutput;
		bool isTraced;

		map_t(serialize_fn_t _serialize_fn) :
			serialize_fn(_serialize_fn)
			, isOutput(false)
			, isTraced(false)
		{}
	};
	typedef shared_ptr<map_t> map_ptr_t;


	typedef boost::function<void (const event_args&)> trigger_fn_t;

	struct logger_t{
		typedef stream<file_sink> file_stream_t;

		shared_ptr<file_stream_t> log_stream;
		bool log_stream_name;
		bool log_event_type;

		logger_t(const path& fp, bool ln = false, bool le = false) :
			log_stream(new file_stream_t(fp.c_str())),
			log_stream_name(ln),
			log_event_type(le)
		{
			if ( !log_stream ) {
				cerr << "failed to open file path " << fp << endl;
			} else {
				cout << "logging to " << fp << endl;
			}
		}

		void log(string& stream_name, stream_event& evt) {
			if( !log_stream )	return;

			if( log_stream_name )	(*log_stream) << stream_name << ",";
			if( log_event_type )	(*log_stream) << evt.type << ",";
			(*log_stream) << setprecision(15) << evt.data << endl;
		}
	};

	struct trigger_t {
		string name;
		trigger_fn_t fn;
		shared_ptr<logger_t> logger;

		trigger_t( string s_name, stream_event_type ev_type,
					trigger_fn_t t_fn, shared_ptr<logger_t> t_logger ) :
			name(string(stream_event_name[ev_type])+"_"+s_name),
			fn(t_fn),
			logger(t_logger)
		{}

		void log(string& stream_name, stream_event& evt) {
			if( !logger )	return;
			logger->log(stream_name, evt);
		}
	};

	struct stream_t {
		string name;
		stream_id_t id;

		shared_ptr<trigger_t> trigger[2];

		stream_t( string s_name, stream_id_t s_id,
				   trigger_fn_t ins_trigger_fn = 0,
				   trigger_fn_t del_trigger_fn = 0,
				   shared_ptr<logger_t> ins_logger = shared_ptr<logger_t>(),
				   shared_ptr<logger_t> del_logger = shared_ptr<logger_t>())
			: name(s_name), id(s_id)
		{
			trigger[insert_tuple] =	ins_trigger_fn ?
					shared_ptr<trigger_t>(new trigger_t( s_name, insert_tuple, ins_trigger_fn, ins_logger )) :
					shared_ptr<trigger_t>();
			trigger[delete_tuple] =	del_trigger_fn ?
					shared_ptr<trigger_t>(new trigger_t( s_name, delete_tuple, del_trigger_fn, del_logger )) :
					shared_ptr<trigger_t>();
		}
	};
	typedef shared_ptr<stream_t> stream_ptr_t;


    stream_id_t get_stream_id( string s_name )
    {
        typename map<string, shared_ptr<stream_t> >::iterator
            it = streams_by_name.find( s_name );
        return ( it != streams_by_name.end() ) ? it->second->id : -1;
    }

    string get_stream_name( stream_id_t s_id )
    {
        typename map<stream_id_t, shared_ptr<stream_t> >::iterator
            it = streams_by_id.find( s_id );
        return ( it != streams_by_id.end() ) ? it->second->name : "";
    }

	template<class T>
	void add_map( string m_name, T& t )
	{
		if( maps_by_name.find(m_name) != maps_by_name.end() ) {
			cerr << "Found existing map " << m_name << endl;
			return;
		}

		serialize_fn_t fn = boost::bind( &serializer::template fn<T>, ::boost::lambda::_1, make_nvp(m_name.c_str(),t) );
		map_ptr_t m = shared_ptr<map_t>(new map_t(fn));
		maps_by_name[m_name] = m;
		return;
	}

	void add_stream( string s_name, stream_id_t s_id = -1 )
	{
		if( streams_by_name.find(s_name) != streams_by_name.end() ) {
			cerr << "Found existing stream " << s_name << endl;
			return;
		}

		stream_id_t id = (s_id != -1) ? s_id : next_stream_id++;
		if( streams_by_id.find(s_id) != streams_by_id.end() ) {
			cerr << "Found existing stream " << s_name << " with id " << id << endl;
			return;
		}

		stream_ptr_t s = shared_ptr<stream_t>(new stream_t(s_name, id));
		streams_by_name[s_name] = s;
		streams_by_id[id] = s;
	}

	void add_trigger( string s_name, stream_event_type ev_type, trigger_fn_t fn )
	{
		typename map<string, stream_ptr_t >::iterator it = streams_by_name.find( s_name );
		if( it == streams_by_name.end() ) {
			cerr << "Stream not found: " << s_name << endl;
			return;
		}
		stream_ptr_t s = it->second;

		static shared_ptr<logger_t> g_log = shared_ptr<logger_t>();
		shared_ptr<logger_t> log = shared_ptr<logger_t>();
		if ( run_opts.global() )
		{
			if(!g_log) {
				path global_file = run_opts.get_log_file("", "Events", true);
				g_log = shared_ptr<logger_t>( new logger_t(global_file,true,true) );
			}
			log = g_log;
		}
		else if( run_opts.logged_streams.find(s_name) != run_opts.logged_streams.end() )
		{
			if( run_opts.unified() )
			{
				stream_event_type other_type = ev_type == insert_tuple ? delete_tuple : insert_tuple;
				shared_ptr<logger_t> other_log = s->trigger[other_type]->logger;

				if(other_log)	log = other_log;
				else
					log = shared_ptr<logger_t>( new logger_t(run_opts.get_log_file(s->name),false,true) );
			}
			else
			{
				log = shared_ptr<logger_t>( new logger_t(run_opts.get_log_file(s->name, ev_type), false, false) );
			}
		}

		s->trigger[ev_type] = shared_ptr<trigger_t>(new trigger_t( s->name, ev_type, fn, log ));
	}


	ProgramBase(int argc = 0, char* argv[] = 0) :
		run_opts(argc,argv)
		, stream_multiplexer(12345, 10)
        , table_multiplexer(12345, 10)
		, next_stream_id(0)
		, tuple_count(0)
		, log_count_every(run_opts.log_tuple_count_every)
#ifdef DBT_PROFILE
		, window_size( run_opts.get_stats_window_size() )
		, stats_period( run_opts.get_stats_period() )
		, stats_file( run_opts.get_stats_file() )
		, exec_stats(new trigger_exec_stats("exec", window_size, stats_period, stats_file))
		, ivc_stats(new trigger_exec_stats("ivc", window_size, stats_period, stats_file))
		, delta_stats(new delta_size_stats("delta_sz", window_size, stats_period, stats_file))
#endif // DBT_PROFILE
	{
		if ( run_opts.help() ) { exit(1); };
	}

    typedef boost::function<void (stream_event&)> process_fn_t;

    void run_multiplexer(source_multiplexer& multiplexer, process_fn_t process_fn) {
        while( multiplexer.has_inputs() ) {
            shared_ptr<std::list<stream_event> > events = multiplexer.next_inputs();

            if( !events ) { continue; }

            std::list<stream_event>::iterator ev_it = events->begin();
            for( ; ev_it != events->end(); ev_it++)
            {
                process_fn(*ev_it);
            }
        }
    }

	virtual int run() {
		IProgram<TLQ_T>::start_running();

		run_multiplexer(stream_multiplexer,
            boost::bind(&ProgramBase::process_event, this, ::boost::lambda::_1));

		IProgram<TLQ_T>::stop_running();


		trace(run_opts.get_output_file(), false);

#ifdef DBT_PROFILE
		exec_stats->save_now();
#endif // DBT_PROFILE

		return 0;
	}


protected:
	runtime_options run_opts;
	source_multiplexer stream_multiplexer;
	source_multiplexer table_multiplexer;

	map<string, map_ptr_t> maps_by_name;

	map<string, stream_ptr_t> streams_by_name;
	map<stream_id_t, stream_ptr_t > streams_by_id;
	int next_stream_id;

	unsigned int tuple_count;
	unsigned int log_count_every;


	void set_log_count_every(unsigned int _log_count_every){
		log_count_every = _log_count_every;
	}

	void process_event(stream_event& evt)
	{
		#ifdef DBT_TRACE
		trace(cout, false);
		#else
		if( run_opts.is_traced() )
			trace(run_opts.get_trace_file(), true);
		#endif // DBT_TRACE

		typename map<stream_id_t, shared_ptr<stream_t> >::iterator
			s_it = streams_by_id.find(evt.id);
		if( s_it != streams_by_id.end() && s_it->second->trigger[evt.type] ) {
			shared_ptr<trigger_t> trig = s_it->second->trigger[evt.type];

			try {
				#ifdef DBT_TRACE
				cout << trig->name << ": " << evt.data << endl;
				#endif // DBT_TRACE
				trig->log( s_it->second->name, evt );

				(trig->fn)(evt.data);
			} catch (boost::bad_any_cast& bc) {
				cout << "bad cast on " << trig->name << ": " << bc.what() << endl;
			}
		} else {
			cerr << "Could not find "
					<< stream_event_name[evt.type]
					<< " handler for stream " << evt.id << endl;
		}

		if(log_count_every && (tuple_count % log_count_every == 0)){
			struct timeval tp;
			gettimeofday(&tp, NULL);
			cout << tuple_count << " tuples processed at "
					<< tp.tv_sec << "s+" << tp.tv_usec << "us" << endl;
		}
		tuple_count += 1;

		IProgram<TLQ_T>::process_snapshot();
	}

	void trace(const path& trace_file, bool debug) {
		if(strcmp(trace_file.c_str(), "-")){
			std::ofstream ofs(trace_file.c_str());
			trace(ofs, debug);
		} else {
			trace(cout, debug);
		}
	}

	void trace(std::ostream &ofs, bool debug) {
		std::auto_ptr<boost::archive::xml_oarchive> oa;

		typename map<string, map_ptr_t>::iterator it = maps_by_name.begin();
		for( ;it != maps_by_name.end(); it++)
			if( (!debug && it->second->isOutput) || (debug && it->second->isTraced))
			{
				if( !oa.get() )
					oa = std::auto_ptr<boost::archive::xml_oarchive>( new boost::archive::xml_oarchive(ofs, 0));
				it->second->serialize_fn( *oa );
			}
	}


	#ifdef DBT_PROFILE
public:
	unsigned int window_size;
	unsigned int stats_period;
	string stats_file;

	shared_ptr<trigger_exec_stats>  exec_stats;
	shared_ptr<trigger_exec_stats>   ivc_stats;
	shared_ptr<delta_size_stats>   delta_stats;
	#endif // DBT_PROFILE


};
}

#endif /* DBTOASTER_DBT_PROGRAM_BASE_H */
