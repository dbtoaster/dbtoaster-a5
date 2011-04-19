#ifndef WRITELOG_H
#define WRITELOG_H

#include <cstring>
#include <iostream>
#include <cassert>
#include <unordered_map>
#include <boost/variant.hpp>

#include "Common.hpp"
#include "WriteStorage.hpp"

using namespace std;

class WriteLog{
private:
	AllMaps allMaps;

public:
	void insert (string mapName, Variant WriteStorage){
		allMaps["1"] = WriteStorage;
	}
			
	void truncateAll(int TS);
	
	friend ostream &operator << (ostream &stream, const WriteLog& wl);
	
	/* not used direct form allMaps[mapName], since checking will be performed.
		If there is no checking, segmentation fault will occur. */
	template<class WS>
	WS& getMap(string mapName) {
		AllMaps::iterator it;
		it=allMaps.find(mapName);
		if(it == allMaps.end()){
			throw NonexistMapExc(mapName); 
		}
		return boost::get<WS>(it->second);
	}
	
	template<class WS, class Key>
	void setValue(WS& ws, Key& key, int TS, double value){
		ws.setValue(key, TS, value);
	}
	
	/* return value which is most recent before TS */
	template<class WS, class Key>
	double getValue(WS& ws, Key& key, int TS){
		/* the most recent version could be at TS-1 */
		ws.setPointer(key, TS-1);
		if (!ws.isPointerEnd()){
			return ws.getPointerValue();
		}else{
			return 0;		
		}

		/*	in the same ts: 
			return ws.getValue(key, TS);*/
	}

	/* delta between successive <TS,Value> pairs.
		More recent of them is specified in method TS. */	
	template<class WS, class Key>
	double getDelta(WS& ws, Key& key, int TS){
		ws.setPointer(key, TS);
		if (!ws.isPointerEnd()){
			double value1 = ws.getPointerValue();
			ws.pointerMoveNext();
			if(ws.isPointerEnd()){
				return value1;
			}else{
				double value2 = ws.getPointerValue();
				return value1 - value2;
			}
		}else{
			return 0;		
		}
	}


};
#endif
