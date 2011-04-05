#ifndef WRITESTORAGE_H
#define WRITESTORAGE_H

#include <iostream>
#include <cstring>
#include <sstream>
#include <vector>
#include <map>
#include <unordered_map>

#include "Common.hpp"

using namespace std;
	
/* unordered_map has structure as key and another map as data.
	The another map has timestamp as key and value as data. 
	unordered_map distributed keys using Hash, 
	while the another map sorts entries using Compare */
	
template <class Key, class Hash, class Compare, class Match>
class WriteStorage {
	/* typedefs, note that for nested types typename has to be specified as well */
	typedef unordered_map<Key, TSvalue, Hash, Compare> Storage;
	typedef typename Storage::iterator StorageIter;
	typedef typename Storage::const_iterator StorageConstIter;
	
	typedef pair<int, double> PairTSvalue;
	typedef unordered_map<Key, PairTSvalue, Hash, Compare> Slice;
	typedef typename Slice::const_iterator SliceConstIter;
	
	/* For each registered pattern, one Secondary Storage */
	typedef unordered_map<Key, const TSvalue*, Hash, Compare> SecStorage;
	typedef typename SecStorage::const_iterator SecStConstIter;
	
	/* PatternKey consists of
			- Key from template (Key from the map), i.e. [4, 0, 3] (zero have to be in place of *)
			- Pattern with elements 0 and 1,        i.e. [0, 1, 0]
		It represents 							          i.e  [4, *, 3] */
	typedef pair<Key, vector<int> > PatternKey;
	
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
		bool operator() (const PatternKey& patternKey1, 
								const PatternKey& patternKey2) const {
								Compare compare;
								return compare(patternKey1.first, patternKey2.first) &&
											Comparison::compareVectors(patternKey1.second, patternKey2.second);
		}
	};
	/* map could be employed as well, but then < operator should be provided for PatternKey .*/
	typedef unordered_map<PatternKey, SecStorage, HashPattern, ComparePattern> Secondaries;
	typedef typename Secondaries::iterator SecIter;
	typedef typename Secondaries::const_iterator SecConstIter;

private:
	Storage storage;
	
	/* used in pointer methods, mapTSvalue is the reason why this visiting methods
		could not return iterator 
			- for checking map.end(), we have to know map we are visiting. */
	TSvalIter itTSvalGlobal;
	TSvalue* mapTSvalue;
	
	/* collection of all pattern-based secondary index structures. */
	Secondaries secondaries;
	

public:
	void setValue(const Key& key, int ts, double value);	
	double getValue(const Key& key, int ts);
	
	
	void setPointer(const Key& key, int ts);
	void pointerMoveNext();
	int getPointerTS()const;
	double getPointerValue()const;
	bool isPointerEnd()const;
	
	void truncate(int TS);
	
		/* Friend template method has to be inline. 
		Alternative: http://www.parashift.com/c++-faq-lite/templates.html#faq-35.16 */
	friend ostream &operator << (ostream &stream, const WriteStorage<Key, Hash, Compare, Match>& ws){
		StorageConstIter itSt;
		TSvalConstIter itTSval;
		for (itSt = ws.storage.begin(); itSt != ws.storage.end(); itSt++) {
			stream << "For mapkey " << itSt->first << ": " << endl;
			const TSvalue* tsValue = &(itSt->second);		 
			for(itTSval = tsValue->begin(); itTSval != tsValue->end(); itTSval++){
				stream << "[" << itTSval->first << ", " << itTSval->second << "]" << endl;
			}
			stream << endl;
		}	   
		return stream;
	}
	
	/* adding new pattern */
	void addSecStorage(const PatternKey& pk);
	
	/* adding key to some pattern(s) */
	void addToSecondaries(const Key& key, const TSvalue* pTSval);
	
	Slice getSlice(const PatternKey& pk, int ts)const;
	
	string printSlice(const Slice& slice)const;
	
	string printSecondaries()const;
};

/* all template methods definitions and declarations
		has to be inside same file. */
template <class Key, class Hash, class Compare, class Match>
void WriteStorage<Key, Hash, Compare, Match>::setValue(const Key& key, int ts, double value){
	StorageIter itSt;
	itSt = storage.find(key);
	if(itSt != storage.end()){
		/* in place modifications of TSvalue map */
		TSvalue* tsValue = &(itSt->second);
		(*tsValue)[ts] = value;
	}else{
		TSvalue tsValue;
		tsValue[ts] = value;
		storage[key] = tsValue;
		addToSecondaries(key, &(storage[key]));
	}
}

/* There is no exceptions here, since this method will return 0, if no entry exist. */
template <class Key, class Hash, class Compare, class Match>
double WriteStorage<Key, Hash, Compare, Match>::getValue(const Key& key, int ts) {
	return storage[key][ts];
	
	/* This is equivalent version, but enables us:
		- const method modificator
		- possibility to throw Exception, if wanted 
		
	StorageConstIter itSt = storage.find(location);
	if (itSt != storage.end()){
		const TSvalue* tsValue = &(itSt->second);
		TSvalConstIter itTSval = tsValue->find(ts);
		if(itTSval != tsValue->end()){
			return itTSval->second;
		}
	}
	return 0;
	*/
}

/* has to be on lower level, due to boost library approach used */
template <class Key, class Hash, class Compare, class Match>
void WriteStorage<Key, Hash, Compare, Match>::truncate(int TS) {
	StorageIter itSt;
	TSvalIter itTSval;
	/* iterate over maps */
	for (itSt = storage.begin(); itSt != storage.end(); itSt++) {
		TSvalue* tsValue = &(itSt->second);
		bool foundLess = false;	 
		/* iterate over values for single map */	
		for(itTSval = tsValue->begin(); itTSval != tsValue->end(); itTSval++){
			if(foundLess){
				/* delete from here up to the end, since TS are in descending order. */
				tsValue->erase(itTSval, tsValue->end());
				/* exit of inner loop */
				break;
			}
			if(itTSval->first < TS){
				/* from now on, following values can safely be deleted.
					We have to save one value before EPOCH. */
				foundLess = true;
			}	
		}
	}
}


/* only one client could iterate over this collection because of itTSvalGlobal is single for WriteStorage. */
	
/* this method will point to first ts/value pair which ts is less or equal to method's ts */
template <class Key, class Hash, class Compare, class Match>
void WriteStorage<Key, Hash, Compare, Match>::setPointer(const Key& key, int ts){
	StorageIter itSt;
	itSt = storage.find(key);
	if(itSt != storage.end()){
		mapTSvalue = &(itSt->second);
		itTSvalGlobal = mapTSvalue->find(ts);
		if(itTSvalGlobal == mapTSvalue->end()){
			//sequential search
			for(itTSvalGlobal = mapTSvalue->begin(); itTSvalGlobal != mapTSvalue->end(); itTSvalGlobal++){
				/* we locate on first smaller item, since there is no exact ts */
				if(itTSvalGlobal->first < ts){
					break;
				}
			}
		}
	}else{
		ostringstream os;
		os << "For WriteStorage " << this << endl << 
			" there is no key " << key << endl << "." << endl;
		throw MyException(os.str());
	}
}
	
template <class Key, class Hash, class Compare, class Match>
void WriteStorage<Key, Hash, Compare, Match>::pointerMoveNext(){
	/* do not increment if we are already at the end */
	if (itTSvalGlobal != mapTSvalue->end())
		itTSvalGlobal++;
}

template <class Key, class Hash, class Compare, class Match>
int WriteStorage<Key, Hash, Compare, Match>::getPointerTS()const{
	return itTSvalGlobal->first;
}

template <class Key, class Hash, class Compare, class Match>	
double WriteStorage<Key, Hash, Compare, Match>::getPointerValue()const{
	return itTSvalGlobal->second;
}

template <class Key, class Hash, class Compare, class Match>	
bool WriteStorage<Key, Hash, Compare, Match>::isPointerEnd()const{
	return itTSvalGlobal == mapTSvalue->end();
}


template <class Key, class Hash, class Compare, class Match>	
void WriteStorage<Key, Hash, Compare, Match>::addSecStorage(const PatternKey& pk){
		SecStorage ss;
		secondaries[pk]=ss;
}

/* Secondaries and primary share the same key copy, therefore it should not be changed from main program! */
template <class Key, class Hash, class Compare, class Match>	
void WriteStorage<Key, Hash, Compare, Match>::addToSecondaries(const Key& key, const TSvalue* pTSvalue){
		//typedef unordered_map<PatternKey, SecStorage, HashPattern, ComparePattern> Secondaries;
		SecIter itSec;
		Match match;
		for(itSec = secondaries.begin(); itSec != secondaries.end(); itSec++){
			const PatternKey& pk = itSec->first;
			SecStorage* ss = &(itSec->second);
			if (match(pk, key)){
				/* add a pointer in primary structure to entry in the secondary structure. */
				(*ss)[key] = pTSvalue;
			}
		}
}

template <class Key, class Hash, class Compare, class Match>	
unordered_map<Key, pair<int, double>, Hash, Compare>  /* Slice as return type */ 
WriteStorage<Key, Hash, Compare, Match>::getSlice(const PatternKey& pk, int ts)const{
		/* find pattern associated to this pattern key.
			search is performed on secondaries. */
		SecConstIter itSec = secondaries.find(pk);
		assert(itSec != secondaries.end());
 		const SecStorage* ss = &(itSec->second);
 	
 		Slice slice;
 		/* for each key in pattern-based secondary storage */
 		for(SecStConstIter itSecSt = ss->begin(); itSecSt != ss->end(); itSecSt++){
 			Key key = itSecSt->first;
 			const TSvalue* ptv = itSecSt->second;
 			TSvalConstIter itTSval;
 			
 			int readTS = 0;
 			double readValue = 0;
 			for(itTSval = ptv->begin(); itTSval != ptv->end(); itTSval++){
 				readTS = itTSval->first;
 				if(readTS < ts){
 					readValue = itTSval->second;
 					break;
 				}
 			}
 			PairTSvalue pairtv = pair<int, double>(readTS, readValue);
 			slice[key] = pairtv;
 		}
 		return slice;
}

template <class Key, class Hash, class Compare, class Match>	
string WriteStorage<Key, Hash, Compare, Match>::printSlice(const Slice& slice)const{
		ostringstream os;
		SliceConstIter itSlice;
		for(itSlice = slice.begin(); itSlice != slice.end(); itSlice++){
			os << "Key" << itSlice->first << endl;
			const PairTSvalue& pairTSvalue = itSlice->second;
			os << "TS: " << pairTSvalue.first << " Value: " << pairTSvalue.second << endl;
		}
		return os.str();
	}	
	
template <class Key, class Hash, class Compare, class Match>	
string WriteStorage<Key, Hash, Compare, Match>::printSecondaries()const{
	ostringstream os;
	os << "Same as primary, but only some keys fullfill patterns" << endl;
	for(SecConstIter itSec = secondaries.begin(); itSec != secondaries.end(); itSec++){
			const SecStorage* ss = &(itSec->second);
			SecStConstIter itSecSt;
			for (itSecSt = ss->begin(); itSecSt!= ss->end(); itSecSt++){
				os << "Key" << itSecSt->first << endl;
				const TSvalue* pTSval = itSecSt->second;
				TSvalConstIter itTSval;
				for (itTSval = pTSval->begin(); itTSval != pTSval->end(); itTSval++){
					os << "TS " << itTSval->first << " Value " << itTSval->second << endl;
				}
			}
	}
	return os.str();
}

#endif
