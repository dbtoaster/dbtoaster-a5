#ifndef GEN_STRUCTURES_H
#define GEN_STRUCTURES_H

#include <iostream>
#include <cstring>
#include <cassert>
#include <map>
#include <unordered_map>
#include <boost/variant.hpp>
using namespace std;

/* ONE MAP TYPE */
#define KEY_NAME Key1
#define HASH_NAME Hash1
#define COMPARE_NAME Compare1
#define MATCH_NAME Match1

class KEY_NAME {
public:
/* actual content depends on M3 map dimension size and type */
	int loc1;
	int loc2;
	
	friend ostream &operator<<(ostream &stream, KEY_NAME mk){
		stream << "[" << mk.loc1 << ", " << mk.loc2 << "]";
		return stream;
	}
};

class HASH_NAME {
public:
	/* maybe there are better implementations? */
	size_t operator() (const KEY_NAME& mk) const {
		size_t h = 0;
		h += mk.loc1;
		h += mk.loc2;
      return h; 
	}
};

class COMPARE_NAME {	
public:
	/* unordered_map has only == operator */
	bool operator() (const KEY_NAME& mk1, const KEY_NAME& mk2) const {
		return mk1.loc1 == mk2.loc1 && 
				 mk1.loc2 == mk2.loc2;
	}
};

class MATCH_NAME {
public:
	bool operator()(const pair<KEY_NAME, vector<int> >& pk, const KEY_NAME& key) const{
		const KEY_NAME& pattern_key = pk.first;
		vector<int> pattern = pk.second;
		int size = pattern.size();
		
		/* since we have loc1 and loc2 */
		assert(size == 2);
		
		if (pattern_key.loc1 != key.loc1){
			if (pattern[0] == 0){
				/* == 0 means we are NOT on * place */
				return false;
			}
		}
		
		if (pattern_key.loc2 != key.loc2){
			if (pattern[1] == 0){
				/* == 0 means we are NOT on * place */
				return false;
			}
		}
		
		return true;
	}
};


/* END of ONE MAP TYPE */


/* COMMON */

/* all possibilities of Map types has to be specified here,
	but operator << will work without any conversion! */
template<class Key, class Hash, class Compare, class Match>
class WriteStorage;
typedef boost::variant<WriteStorage<Key1, Hash1, Compare1, Match1> > Variant;
typedef unordered_map<string, Variant > AllMaps;

/* END of COMMON*/
#endif
