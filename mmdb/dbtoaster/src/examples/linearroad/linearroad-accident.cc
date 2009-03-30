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

#include "linearroad.h"

using namespace std;
using namespace tr1;

//#define DEBUG 1

typedef LRData stream_tuple;

// Input stream
typedef list<stream_tuple> stream_buffer;

struct stream {
	virtual bool stream_has_inputs() = 0;
	virtual stream_tuple next_input() = 0;
        virtual unsigned int get_buffer_size() = 0;
};

void split(string str, string delim, vector<string>& results)
{
	int cutAt;
	while( (cutAt = str.find_first_of(delim)) != str.npos )
	{
		if(cutAt > 0)
			results.push_back(str.substr(0,cutAt));

		str = str.substr(cutAt+1);
	}

	if(str.length() > 0)
		results.push_back(str);
}

inline void print_result(
	long sec, long usec, const char *text, ofstream* log = NULL)
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

        usleep(200000);
}

struct file_stream : public stream
{
	ifstream* input_file;
	stream_buffer buffer;
	unsigned int buffer_count;
	unsigned long line;
	unsigned long threshold;

        unsigned int buffer_size;
        bool finished_reading;

	file_stream(string file_name, unsigned int c)
		: buffer_count(c), line(0), buffer_size(0), finished_reading(false)
	{
		input_file = new ifstream(file_name.c_str());
		if ( !(input_file->good()) )
			cerr << "Failed to open file " << file_name << endl;

		threshold =
			static_cast<long>(ceil(static_cast<double>(c) / 10));
	}

	tuple<bool, stream_tuple> read_tuple()
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
				return make_tuple(false, stream_tuple());
			}
			if ( *end == '\0' && i != 14 ) {
				cerr << "Invalid field " << line << " " << i << endl;
				return make_tuple(false, stream_tuple());
			}
			*end = '\0';
			int f = atoi(start);
			start = ++end;
			lr_data_fields[i] = f;
		}

                /*
		vector<string> tokens;
		split(row, ",", tokens);

		if ( tokens.size() != 15 ) {
			cerr << "Invalid record found at line " << line << endl;
			return make_tuple(false, stream_tuple());
		}

		vector<int> lr_data_fields(15);
		for (int i = 0; i < 15; ++i) {
			istringstream field_ss(tokens[i]);
			int f; field_ss >> f;
			if ( !field_ss ) {
				cerr << "Invalid field read at line "
					<< line << " field " << i << endl;
				return make_tuple(false, stream_tuple());
			}

			lr_data_fields[i] = f;
		}
                */

		stream_tuple r(lr_data_fields);
		return make_tuple(true, r);
	}

	void buffer_stream()
	{
		buffer_size = buffer.size();
		while ( buffer_size < buffer_count && input_file->good() )
		{
			tuple<bool, stream_tuple> valid_input = read_tuple();
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

	stream_tuple next_input()
	{
		if ( !finished_reading && buffer_size < threshold )
			buffer_stream();

		stream_tuple r = buffer.front();
		buffer.pop_front();
                --buffer_size;
		return r;
	}

        unsigned int get_buffer_size() { return buffer_size; }
};

// vid, xway, dir
typedef tuple<int, int, int> agg1_key;
typedef tuple<int, int, int, int> agg1_tuple;

// xway, seg, dir
typedef tuple<int, int, int> agg2_key;
typedef tuple<int, int, int, int> agg2_tuple;

// t -> min/max/count pos
//typedef map<int, tuple<int, int, int> > t_pos_index;

typedef tuple<int, int> t_pos_tuple;
typedef list<t_pos_tuple> t_pos_index;


// minutes -> min/max/count vid
//typedef map<int, tuple<int, int, int> > m_vid_index;

typedef tuple<int, int> m_vid_tuple;
typedef list<m_vid_tuple> m_vid_index;


struct less_window_t_pos :
	public binary_function<t_pos_tuple, t_pos_tuple, bool>
{
	int window_size;
	less_window_t_pos(int ws) : window_size(ws) {}

	bool operator()(t_pos_tuple l, t_pos_tuple r) {
		return get<0>(l) < (get<0>(r) - window_size);
	}
};

struct less_window_m_vid :
	public binary_function<m_vid_tuple, m_vid_tuple, bool>
{
	int window_size;
	less_window_m_vid(int ws) : window_size(ws) {}

	bool operator()(m_vid_tuple l, m_vid_tuple r) {
		return get<0>(l) < (get<0>(r) - window_size);
	}
};

void lracc_stream(stream* s,
	ofstream* results, ofstream* log, bool dump = false)
{

	less_window_t_pos t_pos_window_op(120);
	less_window_m_vid m_vid_window_op(1);

	// agg1_key -> t -> min/max pos
	map<agg1_key, t_pos_index> min_max_pos;

	// agg2_key -> t -> min/max vid
	map<agg2_key, m_vid_index> min_max_vid;

	struct timeval tvs, tve;

	double result = 0.0;

	cout << "Initial buffer size " << s->get_buffer_size() << endl;

	gettimeofday(&tvs, NULL);

	unsigned long counter = 0;

	while ( s->stream_has_inputs() )
	{
		stream_tuple in = s->next_input();

		++counter;

		int t = in.t;
		int vid = in.vid;
		int xway = in.xway;
		int dir = in.dir;
		int seg = in.seg;
		int pos = in.pos;

		#ifdef DEBUG
		cout << "Tuple " << t << ", " << vid << ", " << xway
			<< ", " << dir << ", " << seg << ", " << pos << endl;
		#endif

		// Process position reports.
		if ( in.rec_type != 0 ) continue;

		// Delta min/max pos [range 120]
		agg1_key k1 = make_tuple(vid, xway, dir);

		int new_min_pos = numeric_limits<int>::max();
		int new_max_pos = numeric_limits<int>::min();
		int new_pos_count = 0;

		t_pos_index& pos_index = min_max_pos[k1];
		t_pos_tuple t_pos_window_end = make_tuple(t, 0);

		t_pos_index::iterator pos_end = pos_index.end();
		t_pos_index::iterator pos_it = lower_bound(
			pos_index.begin(), pos_end, t_pos_window_end, t_pos_window_op);

		/*
		while ( (pos_it != pos_index.begin() && pos_it != pos_end) &&
				(get<0>(*pos_it) >= (t-120)) )
		{
			--pos_it;
		}
		*/

		if ( !((pos_it == pos_end) ||
				(pos_it == pos_index.begin() &&
					(get<0>(*pos_it) >= (t-120)))) )
		{
			#ifdef DEBUG
			cout << "Deleting AGG1 window ["
				<< get<0>(*(pos_index.begin()))
				<< ", " << get<0>(*pos_it) << "]" << endl;

			cout << "TP bef " << pos_index.size() << endl;
			#endif

			pos_index.erase(pos_index.begin(), pos_it);

			#ifdef DEBUG
			cout << "TP aft " << pos_index.size() << endl;
			#endif
		}

		t_pos_tuple new_t_pos = make_tuple(t, pos);
		pos_index.push_back(new_t_pos);

		pos_it = pos_index.begin(); pos_end = pos_index.end();
		for (; pos_it != pos_end; ++pos_it)
		{
			new_min_pos = min(new_min_pos, get<1>(*pos_it));
			new_max_pos = max(new_max_pos, get<1>(*pos_it));
			++new_pos_count;
		}

		agg1_tuple new_agg1 =
			make_tuple(t, new_min_pos, new_max_pos, new_pos_count);

		#ifdef DEBUG
		cout << "New agg1 " << t << ", "
			<< new_min_pos << ", "
			<< new_max_pos << ", "
			<< new_pos_count << endl;
		#endif

		// Old t_pos_index data structure
		/*
		t_pos_index::iterator pos_it = pos_index.lower_bound(t-120);
		t_pos_index::iterator pos_end = pos_index.end();

		unordered_set<int> t_deletions;

		while ( (pos_it != pos_index.begin() && pos_it != pos_index.end()) &&
				(pos_it->first >= (t-120)) )
		{
			--pos_it;
		}

		if ( !((pos_it == pos_index.begin() && (pos_it->first >= (t-120))) ||
				(pos_it == pos_index.end())) )
		{
			while ( pos_it != pos_index.begin() ) {
				t_deletions.insert(pos_it->first);
				--pos_it;
			}

			t_deletions.insert(pos_it->first);
		}

		unordered_set<int>::iterator del_it = t_deletions.begin();
		unordered_set<int>::iterator del_end = t_deletions.end();
		for (; del_it != del_end; ++del_it)
			pos_index.erase(*del_it);

		pos_it = pos_index.begin(); pos_end = pos_index.end();
		for (; pos_it != pos_end; ++pos_it)
		{
			new_min_pos = min(new_min_pos, get<0>(pos_it->second));
			new_max_pos = max(new_max_pos, get<1>(pos_it->second));
		}

		t_pos_index::iterator t_pos_found = pos_index.find(t);
		int prev_pos_count = t_pos_found == pos_index.end()?
			0 : get<2>(t_pos_found->second);

		tuple<int, int, int> new_agg1 = make_tuple(
			min(pos, new_min_pos), max(pos, new_max_pos),
			prev_pos_count+1);

		pos_index[t] = new_agg1;
		*/

		// Filter min pos = max pos, count = 4
		if ( (get<1>(new_agg1) == get<2>(new_agg1)) && (get<3>(new_agg1) > 4) )
		{
			int new_seg = get<1>(new_agg1)/5280;
			int minute = ((t+90)/60)+1;
			agg2_key k2 = make_tuple(xway, new_seg, dir);

			#ifdef DEBUG
			cout << "Found stopped m:" << minute
				<< " v:" << vid << " seg:" << new_seg << endl;
			#endif

			// Delta min/max vid
			int new_min_vid = numeric_limits<int>::max();
			int new_max_vid = numeric_limits<int>::min();
			int new_vid_count = 0;

			m_vid_index& vid_index = min_max_vid[k2];
			m_vid_tuple m_vid_window_end = make_tuple(minute, 0);

			m_vid_index::iterator vid_end = vid_index.end();
			m_vid_index::iterator vid_it = lower_bound(
				vid_index.begin(), vid_end, m_vid_window_end, m_vid_window_op);

			/*
			while ( (vid_it != vid_index.begin() && vid_it != vid_end) &&
					(get<0>(*vid_it) >= (minute-1)) )
			{
				--vid_it;
			}
			*/

			if ( !((vid_it == vid_end) ||
					(vid_it == vid_index.begin() &&
						(get<0>(*vid_it) >= (minute-1)))) )
			{
				#ifdef DEBUG
				cout << "Deleting AGG2 window ["
					<< get<0>(*(vid_index.begin()))
					<< ", " << get<0>(*vid_it) << "]" << endl;

				cout << "MV bef " << vid_index.size() << endl;
				#endif

				vid_index.erase(vid_index.begin(), vid_it);

				#ifdef DEBUG
				cout << "MV aft " << pos_index.size() << endl;
				#endif
			}

			m_vid_tuple new_m_vid = make_tuple(minute, vid);
			vid_index.push_back(new_m_vid);

			vid_it = vid_index.begin(); vid_end = vid_index.end();
			for (; vid_it != vid_end; ++vid_it)
			{
				new_min_vid = min(new_min_vid, get<1>(*vid_it));
				new_max_vid = max(new_max_vid, get<1>(*vid_it));
				++new_vid_count;
			}

			agg2_tuple new_agg2 =
				make_tuple(minute, new_min_vid, new_max_vid, new_vid_count);

			#ifdef DEBUG
			cout << "New agg2 " << minute << ", "
				<< new_min_vid << ", "
				<< new_max_vid << ", "
				<< new_vid_count << endl;
			#endif

			// Old vid_index data structure
			/*
			unordered_set<int> min_deletions;

			m_vid_index::iterator min_it = min_max_vid[k2].begin();
			m_vid_index::iterator min_end = min_max_vid[k2].end();

			for (; min_it != min_end; ++min_it)
			{
				if ( min_it->first < (minute-1) )
					min_deletions.insert(min_it->first);
				else
				{
					new_min_vid = min(new_min_vid, get<0>(min_it->second));
					new_max_vid = max(new_max_vid, get<1>(min_it->second));
				}
			}

			m_vid_index::iterator m_vid_found = min_max_vid[k2].find(minute);
			int prev_vid_count = m_vid_found == min_max_vid[k2].end()?
				0 : get<2>(m_vid_found->second);

			tuple<int, int, int> new_agg2 = make_tuple(
				min(vid, new_min_vid), max(vid, new_max_vid),
				prev_vid_count+1);

			min_max_vid[k2][minute] = new_agg2;

			unordered_set<int>::iterator mdel_it = min_deletions.begin();
			unordered_set<int>::iterator mdel_end = min_deletions.end();

			for (; mdel_it != mdel_end; ++mdel_it)
				min_max_vid[k2].erase(*mdel_it);
			*/

			// Filter min vid != max vid, count > 1
			if ( get<1>(new_agg2) != get<2>(new_agg2) &&
					(get<3>(new_agg2) > 1) )
			{
				#ifdef DEBUG
				cout << "Found accident "
					<< get<0>(k2) << "," << get<1>(k2) << ","
					<< get<2>(k2) << "," << minute << ","
					<< get<3>(new_agg2) << endl;
				#endif

				(*results) << get<0>(k2) << "," << get<1>(k2) << ","
					<< get<2>(k2) << "," << minute << ","
					<< get<3>(new_agg2) << endl;
			}
		}
	}

	gettimeofday(&tve, NULL);

	cout << "Processed " << counter << " tuples." << endl;

	print_result(tve.tv_sec - tvs.tv_sec, tve.tv_usec - tvs.tv_usec,
		"VWAP toasted QP", log);
}

/*
void lracc_snapshot(stream* s,
	ofstream* results, ofstream* log, bool dump = false)
{

}
*/

void print_usage()
{
	cout << "Usage: linearroad "
		<< "<app mode> <data file> <log file> "
		<< "<results file> [iters]" << endl;
}

int main(int argc, char* argv[])
{
	if ( argc < 4 ) {
		print_usage();
		exit(1);
	}

	string app_mode(argv[1]);
	string input_file_name(argv[2]);
	string log_file_name(argv[3]);
	string results_file_name(argv[4]);
	int iters = argc > 5? atoi(argv[5]) : 1;

	ofstream* log = new ofstream(log_file_name.c_str());
	ofstream* out = new ofstream(results_file_name.c_str());

	file_stream f(input_file_name, 15000000);
	f.init_stream();

	if ( app_mode == "toasted" )
		lracc_stream(&f, out, log);
	//else lracc_snapshot(&f, out, log);

	log->flush();
	out->flush();

	log->close();
	out->close();

    return 0;
}
