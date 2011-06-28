#ifndef WRITELOG_H
#define WRITELOG_H

#include <cstring>
#include <iostream>
#include <cassert>
#include <unordered_map>

#include "Common.hpp"
#include "MultiKey.hpp"
#include "WriteStorage.hpp"

using namespace std;

typedef unordered_map<string, WriteStorage*> AllMaps;

class WSCollection{
private:
	AllMaps allMaps;

public:
	void insert (string mapName, WriteStorage* ws){
		allMaps[mapName] = ws;
	}
			
	void garbageCollectionAll(int TS);
	
	friend ostream &operator << (ostream &stream, const WSCollection& wscol);
	
	/* not used direct form allMaps[mapName], since checking will be performed.
		If there is no checking, segmentation fault will occur. */
	WriteStorage* getMap(string mapName) {
		AllMaps::iterator it;
		it=allMaps.find(mapName);
		if(it == allMaps.end()){
			throw NonexistMapExc(mapName); 
		}
		return it->second;
	}

};
#endif
