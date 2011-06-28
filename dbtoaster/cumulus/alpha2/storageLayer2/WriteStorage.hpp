#ifndef WRITESTORAGE_H
#define WRITESTORAGE_H

#include <iostream>
#include <cstring>
#include <sstream>
#include <vector>
#include <cassert>
#include <map>
#include <unordered_map>

#include "Common.hpp"
#include "MultiKey.hpp"

using namespace std;

struct Hash {
	/* maybe there are better implementations? */
	size_t operator() (const Key* mk) const {
      return mk->hashValue();
	}
};

struct CompareKey {
	/* unordered_map has only == operator */
	bool operator() (const Key* pk1, const Key* pk2) const {
		return pk1->compareKey(pk2);
	}
};

/* typedefs, note that for nested types typename has to be specified as well */
typedef unordered_map<const Key*, TSvalue, Hash, CompareKey> Storage;
typedef Storage::iterator StorageIter;
typedef Storage::const_iterator StorageConstIter;

typedef pair<int, double> PairTSvalue;
typedef unordered_map<const Key*, PairTSvalue, Hash, CompareKey> Slice;
typedef Slice::const_iterator SliceConstIter;
/* a keyword typename is necessary only within templates*/

/* For each registered pattern, one Secondary Storage */
typedef unordered_map<const Key*, const TSvalue*, Hash, CompareKey> SecStorage;
typedef SecStorage::const_iterator SecStConstIter;
	
/* PatternKey consists of
		- Key from template (Key from the map), i.e. [4, 0, 3] (zero have to be in place of *)
		- Pattern with elements 0 and 1,        i.e. [0, 1, 0]
	It represents 							          i.e  [4, *, 3] */
typedef pair<const Key*, vector<int>* > PatternKey;
	
/* since there will be only a few patterns, it is not big deal
		if all of them all inside one bucket */
class HashPattern {
public:
	size_t operator() (const PatternKey& pk) const {
		size_t h = 0;
     	return h;
	}
};
class ComparePattern {
public:
	/* In a pair, first is Key, then Pattern, as explained  */
	bool operator() (const PatternKey& patternKey1, const PatternKey& patternKey2) const {
							const Key* pk1 = patternKey1.first;
							const Key* pk2 = patternKey2.first;	
							vector<int>* v1 = patternKey1.second;
							vector<int>* v2 = patternKey2.second;
							CompareKey compare;
							return compare(pk1, pk2) &&
								 Comparison::compareVectors(v1, v2);
	}
};
	/* map could be employed as well, but then < operator should be provided for PatternKey .*/
typedef unordered_map<PatternKey, SecStorage, HashPattern, ComparePattern> Secondaries;
typedef Secondaries::iterator SecIter;
typedef Secondaries::const_iterator SecConstIter;

/* unordered_map has structure as key and another map as data.
	The another map has timestamp as key and value as data. 
	unordered_map distributed keys using Hash, 
	while the another map sorts entries using Compare */
	
class WriteStorage {
private:
	Storage storage;
	
		
	/* collection of all pattern-based secondary index structures. */
	Secondaries secondaries;
	

public:
	void setValue(const Key* key, int ts, double value);	
	double getValue(const Key* key, int ts);
	double getValueBefore(const Key* pkey, int ts);
	double getDelta(const Key* pkey, int ts);
		
	void garbageCollection(int TS);
	
	friend ostream &operator << (ostream &stream, const WriteStorage& ws);
	
	/* adding new pattern */
	void addSecStorage(const PatternKey& pk);
	
	/* adding key to some pattern(s) */
	void addToSecondaries(const Key* key, const TSvalue* pTSval);
	
	Slice getSlice(const PatternKey& pk, int ts)const;
	
	string printSlice(const Slice& slice)const;
	
	string printSecondaries()const;
};

#endif
