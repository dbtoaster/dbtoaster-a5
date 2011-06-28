#include "WSCollection.hpp"

ostream& operator << (ostream &stream, const WSCollection& wscol){
		AllMaps::const_iterator it;
		stream << "From WSCollection:" << endl;
		for(it = wscol.allMaps.begin(); it != wscol.allMaps.end(); it++){
			stream << "A WriteStorage " << it->first << ": " << endl;
			stream << *(it->second);
			stream << endl;
		}
		return stream;	
}
	
void WSCollection::garbageCollectionAll(int TS){
	AllMaps::iterator it;
	for(it = allMaps.begin(); it != allMaps.end(); it++){
		(it->second)->garbageCollection(TS);
	}
}

