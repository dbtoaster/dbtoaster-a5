#include "WriteStorage.hpp"

/* TODO
WriteStorage should avoid using & in this method (TSvalue map should be stored as a pointer)
*/
void WriteStorage::setValue(const Key* pkey, int ts, double value){
	StorageIter itSt;
	itSt = storage.find(pkey);
	if(itSt != storage.end()){
		/* in place modifications of TSvalue map */
		TSvalue* tsValue = &(itSt->second);
		(*tsValue)[ts] = value;
	}else{
		TSvalue tsValue;
		tsValue[ts] = value;
		storage[pkey] = tsValue;
		addToSecondaries(pkey, &(storage[pkey]));
	}
}

/* There is no exceptions here, since this method will return 0, if no entry exist. */
double WriteStorage::getValue(const Key* pkey, int ts) {
	return storage[pkey][ts];
	
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
void WriteStorage::garbageCollection(int TS) {
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

/* return value which is most recent before TS */
double WriteStorage::getValueBefore(const Key* pkey, int ts){
	ts = ts - 1;
	StorageIter itSt;
	itSt = storage.find(pkey);
	
	if(itSt != storage.end()){
		TSvalue* mapTSvalue = &(itSt->second);
		TSvalIter itTSval = mapTSvalue->find(ts);
		if(itTSval == mapTSvalue->end()){
			//sequential search
			for(itTSval = mapTSvalue->begin(); itTSval != mapTSvalue->end(); itTSval++){
				/* we locate on first smaller item, since there is no exact ts */
				if(itTSval->first < ts){
					break;
				}
			}
		}
		if(itTSval != mapTSvalue->end()){
			return itTSval->second;
		}else{
			return 0;
		}
	}else{
		/* requested key do not exist */
		ostringstream os;
		os << "For WriteStorage " << this << endl << 
			" there is no key " << pkey->printKey() << endl << "." << endl;
		throw MyException(os.str());
	}
}

/* return value which is most recent before TS */
double WriteStorage::getDelta(const Key* pkey, int ts){
	StorageIter itSt;
	itSt = storage.find(pkey);
	
	if(itSt != storage.end()){
		TSvalue* mapTSvalue = &(itSt->second);
		TSvalIter itTSval = mapTSvalue->find(ts);
		if(itTSval == mapTSvalue->end()){
			//sequential search
			for(itTSval = mapTSvalue->begin(); itTSval != mapTSvalue->end(); itTSval++){
				/* we locate on first smaller item, since there is no exact ts */
				if(itTSval->first < ts){
					break;
				}
			}
		}
		double first=0;
		double second=0;
		if(itTSval != mapTSvalue->end()){
			first = itTSval->second;
			itTSval++;
			if(itTSval != mapTSvalue->end()){
				second = itTSval->second;
			}
		}
		return first - second;
	}else{
		/* requested key do not exist */
		ostringstream os;
		os << "For WriteStorage " << this << endl << 
			" there is no key " << pkey->printKey() << endl << "." << endl;
		throw MyException(os.str());
	}
}


void WriteStorage::addSecStorage(const PatternKey& pk){
		SecStorage ss;
		secondaries[pk]=ss;
}

/* Secondaries and primary share the same key copy, therefore it should not be changed from main program! */
void WriteStorage::addToSecondaries(const Key* pkey, const TSvalue* pTSvalue){
		//typedef unordered_map<PatternKey, SecStorage, HashPattern, ComparePattern> Secondaries;
		SecIter itSec;
		for(itSec = secondaries.begin(); itSec != secondaries.end(); itSec++){
			const PatternKey& pk = itSec->first;
			SecStorage* ss = &(itSec->second);
			if (pkey->matchPattern(pk)){
				/* add a pointer in primary structure to entry in the secondary structure. */
				(*ss)[pkey] = pTSvalue;
			}
		}
}

Slice WriteStorage::getSlice(const PatternKey& pk, int ts)const{
		/* find pattern associated to this pattern key.
			search is performed on secondaries. */
		SecConstIter itSec = secondaries.find(pk);
		assert(itSec != secondaries.end());
 		const SecStorage* ss = &(itSec->second);

 		Slice slice;
 		/* for each key in pattern-based secondary storage */
 		for(SecStConstIter itSecSt = ss->begin(); itSecSt != ss->end(); itSecSt++){
 			const Key* pkey = itSecSt->first;
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
 			slice[pkey] = pairtv;
 		}
 		return slice;
}

string WriteStorage::printSlice(const Slice& slice)const{
		ostringstream os;
		SliceConstIter itSlice;
		for(itSlice = slice.begin(); itSlice != slice.end(); itSlice++){
			os << "Key" << (itSlice->first)->printKey() << endl;
			const PairTSvalue& pairTSvalue = itSlice->second;
			os << "TS: " << pairTSvalue.first << " Value: " << pairTSvalue.second << endl;
		}
		return os.str();
	}	
	
string WriteStorage::printSecondaries()const{
	ostringstream os;
	os << "Same as primary, but only some keys fullfill patterns" << endl;
	for(SecConstIter itSec = secondaries.begin(); itSec != secondaries.end(); itSec++){
			const SecStorage* ss = &(itSec->second);
			SecStConstIter itSecSt;
			for (itSecSt = ss->begin(); itSecSt!= ss->end(); itSecSt++){
				os << "Key" << (itSecSt->first)->printKey() << endl;
				const TSvalue* pTSval = itSecSt->second;
				TSvalConstIter itTSval;
				for (itTSval = pTSval->begin(); itTSval != pTSval->end(); itTSval++){
					os << "TS " << itTSval->first << " Value " << itTSval->second << endl;
				}
			}
	}
	return os.str();
}

ostream &operator << (ostream &stream, const WriteStorage& ws){
		StorageConstIter itSt;
		TSvalConstIter itTSval;
		for (itSt = ws.storage.begin(); itSt != ws.storage.end(); itSt++) {
			stream << "For mapkey " << (itSt->first)->printKey() << ": " << endl;
			const TSvalue* tsValue = &(itSt->second);		 
			for(itTSval = tsValue->begin(); itTSval != tsValue->end(); itTSval++){
				stream << "[" << itTSval->first << ", " << itTSval->second << "]" << endl;
			}
			stream << endl;
		}	   
		return stream;
	}
