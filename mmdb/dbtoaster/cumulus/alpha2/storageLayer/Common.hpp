#ifndef COMMON_H
#define COMMON_H

#include <vector>
#include <map>

#include "Generated.hpp"
#include "Exceptions.hpp"

using namespace std;

/* comparison of vectors, used in WriteStorage.hpp */
class Comparison{
public:
	static bool compareVectors(const vector<int>& v1, const vector<int>& v2){
			typedef vector<int>::const_iterator Iter;
			int size1 = v1.size();
			int size2 = v2.size();
			
			/* length is not equal */
			if (size1 != size2){
				return false;
			}	
			
			/* length of exactly one of them is zero */
			if(size1 == 0 || size2 == 0){
				return false;
			}
			
			Iter it2 = v2.begin();
			for(Iter it1 = v1.begin(); it1 != v1.end(); it1++, it2++){
				if (*it1 != *it2){
					return false;
				}
			}
			
			return true;
		}
};

class compareTS{
public:
	/* provides method that will say when ts1 element will come before ts2 element.
		entries will be sorted in descending order.
		Implements < operator, and from it, both > and = is implemented 
			(as explained in http://cnx.org/content/m35767/latest/). */
	bool operator()(int ts1, int ts2) const {
		return ts1 > ts2;
	}
};

/* typedefs must follow specific implementation of templates */
typedef map<int, double, compareTS> TSvalue;
typedef TSvalue::iterator TSvalIter;
typedef TSvalue::const_iterator TSvalConstIter;

#endif
