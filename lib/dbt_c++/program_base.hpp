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
#include <boost/serialization/map.hpp>

#include <boost/preprocessor/repetition/enum_params.hpp>

#include <boost/fusion/tuple.hpp>
#include <boost/fusion/include/fold.hpp>
#include <boost/fusion/include/for_each.hpp>
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
#include "standard_adaptors.hpp"

using namespace ::std;
using namespace ::boost;

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

/**
 * Class that provides common functionality for running a program as specified by
 * the sql input file.
 *
 * It implements the process_streams() and process_stream_event() virtual
 * functions of IProgram. Only take_snapshot() remains to be implemented in a
 * derived class in order to get a completely specified IProgram class.
 *
 * Configuration is performed through the following functions:
 *  - add_map : used for specifying the maps used by the program;
 *  - add_stream : used for specifying the streams that might generate events during the
 *  execution of the program;
 *  - add_trigger : used for specifying the trigger functions that need to be executed
 *  for handling different events;
 *  - add_table_source : used for specifying sources of events for static table relations;
 *  - add_stream_source : used for specifying sources of events for stream relations.
 *
 *  The 'TLQ_T' class parameter represents the data-structure used for storing the results
 * of the program.
 */
template<class TLQ_T>
class ProgramBase: public IProgram<TLQ_T> {
public:

	typedef boost::function<void(boost::archive::xml_oarchive&)> serialize_fn_t;
	struct serializer {
		template<class T>
		static boost::archive::xml_oarchive& fn(
				boost::archive::xml_oarchive& oa, const nvp<T>& t) {
			return (oa << t);
		}
	};

	struct map_t {
		serialize_fn_t serialize_fn;

		bool isOutput;
		bool isTraced;

		map_t(serialize_fn_t _serialize_fn) :
				serialize_fn(_serialize_fn), isOutput(false), isTraced(false) {
		}
	};
	typedef shared_ptr<map_t> map_ptr_t;

	typedef boost::function<void(const event_args_t&)> trigger_fn_t;

	struct logger_t {
		typedef stream<file_sink> file_stream_t;

		shared_ptr<file_stream_t> log_stream;
		bool log_relation_name;
		bool log_event_type;

		logger_t(const path& fp, bool ln = false, bool le = false) :
				log_stream(new file_stream_t(fp.c_str())), log_relation_name(ln), log_event_type(
						le) {
			if (!log_stream) {
				cerr << "failed to open file path " << fp << endl;
			} else {
				cout << "logging to " << fp << endl;
			}
		}

		void log(string& relation_name, event_t& evt) {
			if (!log_stream)
				return;

			if (log_relation_name)
				(*log_stream) << relation_name << ",";
			if (log_event_type)
				(*log_stream) << evt.type << ",";
			(*log_stream) << setprecision(15) << evt.data << endl;
		}
	};

	struct trigger_t {
		string name;
		trigger_fn_t fn;
		shared_ptr<logger_t> logger;

		trigger_t(string r_name, event_type ev_type, trigger_fn_t t_fn,
					shared_ptr<logger_t> t_logger) :
			name(string(event_name[ev_type]) + "_" + r_name),
			fn(t_fn),
			logger(t_logger)
		{}

		void log(string& relation_name, event_t& evt) {
			if (!logger)
				return;
			logger->log(relation_name, evt);
		}
	};

	struct relation_t {
		string name;
		bool is_table;
		relation_id_t id;

		shared_ptr<trigger_t> trigger[2];

		relation_t(string r_name, bool r_is_table, relation_id_t r_id,
				trigger_fn_t ins_trigger_fn = 0, trigger_fn_t del_trigger_fn = 0,
				shared_ptr<logger_t> ins_logger = shared_ptr<logger_t>(),
				shared_ptr<logger_t> del_logger = shared_ptr<logger_t>()) :
				name(r_name), is_table(r_is_table), id(r_id) {
			trigger[insert_tuple] =
					ins_trigger_fn ?
							shared_ptr < trigger_t
									> (new trigger_t(r_name, insert_tuple,
											ins_trigger_fn, ins_logger)) :
							shared_ptr<trigger_t>();
			trigger[delete_tuple] =
					del_trigger_fn ?
							shared_ptr < trigger_t
									> (new trigger_t(r_name, delete_tuple,
											del_trigger_fn, del_logger)) :
							shared_ptr<trigger_t>();
		}
	};
	typedef shared_ptr<relation_t> relation_ptr_t;

	relation_id_t get_relation_id(string r_name) {
		typename map<string, shared_ptr<relation_t> >::iterator it =
				relations_by_name.find(r_name);
		return (it != relations_by_name.end()) ? it->second->id : -1;
	}

	string get_relation_name(relation_id_t s_id) {
		typename map<relation_id_t, shared_ptr<relation_t> >::iterator it =
				relations_by_id.find(s_id);
		return (it != relations_by_id.end()) ? it->second->name : "";
	}

	template<class T>
	void add_map(string m_name, T& t) {
		if (maps_by_name.find(m_name) != maps_by_name.end()) {
			cerr << "Found existing map " << m_name << endl;
			return;
		}

		serialize_fn_t fn = boost::bind(&serializer::template fn<T>,
				::boost::lambda::_1, make_nvp(m_name.c_str(), t));
		map_ptr_t m = shared_ptr<map_t>(new map_t(fn));
		maps_by_name[m_name] = m;
		return;
	}

	void add_relation(string r_name, bool is_table = false, relation_id_t s_id = -1) {
		if (relations_by_name.find(r_name) != relations_by_name.end()) {
			cerr << "Found existing relation " << r_name << endl;
			return;
		}

		relation_id_t id = (s_id != -1) ? s_id : next_relation_id++;
		if (relations_by_id.find(id) != relations_by_id.end()) {
			cerr << "Found existing relation " << r_name << " with id " << id
					<< endl;
			return;
		}

		relation_ptr_t r = shared_ptr < relation_t > (new relation_t(r_name, is_table, id));
		relations_by_name[r_name] = r;
		relations_by_id[id] = r;
	}

	void add_trigger(string r_name, event_type ev_type, trigger_fn_t fn) {
		typename map<string, relation_ptr_t>::iterator it = relations_by_name.find(r_name);
		if (it == relations_by_name.end()) {
			cerr << "Relation not found: " << r_name << endl;
			return;
		}
		relation_ptr_t r = it->second;

		static shared_ptr<logger_t> g_log = shared_ptr<logger_t>();
		shared_ptr<logger_t> log = shared_ptr<logger_t>();
		if (run_opts.global()) {
			if (!g_log) {
				path global_file = run_opts.get_log_file("", "Events", true);
				g_log = shared_ptr<logger_t>(
						new logger_t(global_file, true, true));
			}
			log = g_log;
		} else if (run_opts.logged_streams.find(r_name)
				!= run_opts.logged_streams.end()) {
			if (run_opts.unified()) {
				event_type other_type =
						ev_type == insert_tuple ? delete_tuple : insert_tuple;
				shared_ptr<logger_t> other_log = r->trigger[other_type]->logger;

				if (other_log)
					log = other_log;
				else
					log = shared_ptr < logger_t
							> (new logger_t(run_opts.get_log_file(r->name),
									false, true));
			} else {
				log = shared_ptr < logger_t
						> (new logger_t(run_opts.get_log_file(r->name, ev_type),
								false, false));
			}
		}

		r->trigger[ev_type] = shared_ptr < trigger_t
				> (new trigger_t(r->name, ev_type, fn, log));
	}

	void add_source(shared_ptr<streams::source> source, bool is_table_source = false) {
		if( is_table_source )	table_multiplexer.add_source(source);
		else					stream_multiplexer.add_source(source);
	}

	ProgramBase(int argc = 0, char* argv[] = 0) :
		run_opts(argc, argv)
		, stream_multiplexer(12345, 10)
		, table_multiplexer(12345, 10)
		, next_relation_id(0)
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
		if (run_opts.help()) {
			exit(1);
		};
	}

	void process_streams() {

		while( stream_multiplexer.has_inputs() ) {
			shared_ptr<std::list<event_t> > events = stream_multiplexer.next_inputs();

			if( !events )	continue;

			std::list<event_t>::iterator ev_it = events->begin();
			for( ; ev_it != events->end(); ev_it++)
			{
				process_stream_event(*ev_it);
			}
		}

		trace(run_opts.get_output_file(), false);

#ifdef DBT_PROFILE
		exec_stats->save_now();
#endif // DBT_PROFILE
	}

	void process_tables() {
		while( table_multiplexer.has_inputs() ) {
			shared_ptr<std::list<event_t> > events = table_multiplexer.next_inputs();

			if( !events )	continue;

			std::list<event_t>::iterator ev_it = events->begin();
			for( ; ev_it != events->end(); ev_it++)
			{
				process_event(*ev_it, true);
			}
		}
	}

protected:
	runtime_options run_opts;
	source_multiplexer stream_multiplexer;
	source_multiplexer table_multiplexer;

	map<string, map_ptr_t> maps_by_name;

	map<string, relation_ptr_t> relations_by_name;
	map<relation_id_t, relation_ptr_t> relations_by_id;
	int next_relation_id;

	unsigned int tuple_count;
	unsigned int log_count_every;

	void set_log_count_every(unsigned int _log_count_every) {
		log_count_every = _log_count_every;
	}

	void process_event(event_t& evt, bool process_table) {

		typename map<relation_id_t, shared_ptr<relation_t> >::iterator r_it =
				relations_by_id.find(evt.id);
		if( r_it != relations_by_id.end() &&
			r_it->second->is_table == process_table &&
			r_it->second->trigger[evt.type] )
		{
			shared_ptr<trigger_t> trig = r_it->second->trigger[evt.type];

			try {
				#ifdef DBT_TRACE
				cout << trig->name << ": " << evt.data << endl;
				#endif // DBT_TRACE
				trig->log(r_it->second->name, evt);

				(trig->fn)(evt.data);
			} catch (boost::bad_any_cast& bc) {
				cout << "bad cast on " << trig->name << ": " << bc.what()
						<< endl;
			}
		} else {
			cerr << "Could not find " << event_name[evt.type]
					<< " handler for relation " << evt.id << endl;
		}
	}

	void process_stream_event(event_t& evt) {
		#ifdef DBT_TRACE
		trace(cout, false);
		#else
		if (run_opts.is_traced())
			trace(run_opts.get_trace_file(), true);
		#endif // DBT_TRACE

		process_event(evt, false);

		if (log_count_every && (tuple_count % log_count_every == 0)) {
			struct timeval tp;
			gettimeofday(&tp, NULL);
			cout << tuple_count << " tuples processed at " << tp.tv_sec << "s+"
					<< tp.tv_usec << "us" << endl;
		}
		tuple_count += 1;

		IProgram < TLQ_T > ::process_stream_event(evt);
	}



private:
	void trace(const path& trace_file, bool debug) {
		if (strcmp(trace_file.c_str(), "-")) {
			std::ofstream ofs(trace_file.c_str());
			trace(ofs, debug);
		} else {
			trace(cout, debug);
		}
	}

	void trace(std::ostream &ofs, bool debug) {
		std::auto_ptr<boost::archive::xml_oarchive> oa;

		typename map<string, map_ptr_t>::iterator it = maps_by_name.begin();
		for (; it != maps_by_name.end(); it++)
			if ((!debug && it->second->isOutput)
					|| (debug && it->second->isTraced)) {
				if (!oa.get())
					oa = std::auto_ptr<boost::archive::xml_oarchive>(
							new boost::archive::xml_oarchive(ofs, 0));
				it->second->serialize_fn(*oa);
			}
	}

#ifdef DBT_PROFILE
public:
	unsigned int window_size;
	unsigned int stats_period;
	string stats_file;

	shared_ptr<trigger_exec_stats> exec_stats;
	shared_ptr<trigger_exec_stats> ivc_stats;
	shared_ptr<delta_size_stats> delta_stats;
#endif // DBT_PROFILE

};

template<class entry_type>
struct increment_table_entry{
	void operator ()( entry_type& e )
	{
		e.__av++;
	}
};

}


namespace boost {namespace serialization {

template<class Archive>
struct serialize_tuple
{
	Archive& ar;

	serialize_tuple(Archive& _ar) : ar(_ar){}

    template<typename T>
    void operator()(T& t) const
    {
    	ar & BOOST_SERIALIZATION_NVP(t);
    }
};

template <class Archive, BOOST_PP_ENUM_PARAMS (FUSION_MAX_VECTOR_SIZE, typename T)>
inline void serialize (Archive& ar, boost::fusion::tuple <BOOST_PP_ENUM_PARAMS (FUSION_MAX_VECTOR_SIZE, T) >& p, const unsigned int/* file_version */)
{
    boost::fusion::for_each( p, serialize_tuple<Archive>(ar) );
}

}} //namespace serialization, namespace boost


#endif /* DBTOASTER_DBT_PROGRAM_BASE_H */
