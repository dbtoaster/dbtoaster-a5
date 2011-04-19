#include "WriteLog.hpp"

/* Visitor for all WriteStorage template types.
	Put in constructor all variables, but they have to be non-template.
	If we decide to put Truncate on upper level, just invoke friend method:
		truncate(WriteStorageVariant...)
		and use getNext pointer from WriteStorage.
		NOT SURE this will do since no templates for friend, 
			and variant is not auto-converted into desired type.
		EASIER when using Pointers instead of boost:variant.
	*/
struct truncateOne : public boost::static_visitor<void>{
	explicit truncateOne(int TS) : timestamp(TS) {}

	template<class T>
	void operator()(T& t) const {
		t.truncate(timestamp);
	}
private:
	int timestamp;
};


ostream &operator << (ostream &stream, const WriteLog& wl){
		AllMaps::const_iterator it;
		stream << "From WriteLog:" << endl;
		for(it = wl.allMaps.begin(); it != wl.allMaps.end(); it++){
			stream << "A map:" << endl;
			stream << it->second;
			stream << endl;
		}
		return stream;	
}
	
void WriteLog::truncateAll(int TS){
	AllMaps::iterator it;
	for(it = allMaps.begin(); it != allMaps.end(); it++){
		boost::apply_visitor(::truncateOne(TS), it->second);
	}
}

