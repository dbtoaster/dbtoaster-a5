#include "MultiKey.hpp"

/* IntKey method definition*/
bool IntKey::compareKey(const Key* pk) const {
		const IntKey* pik = dynamic_cast<const IntKey*>(pk);
		if(pik == 0){
			// they are not even of the same type
			return false;
		}else{
			return value == pik->value;	
		}
}


bool IntKey::matchPattern(const pair<const Key*, vector<int>* >& kpair) const{
		const Key* pat_key = kpair.first;
		vector<int>* pattern = kpair.second;
		int size = pattern->size();
		
		assert(size == pat_key->numElts());
		
		const IntKey* pattern_key = dynamic_cast<const IntKey*> (pat_key);
		if (pattern_key == 0){
			/* trying to compare this of type IntKey with something else*/
			return false;
		}
	
		if(value != pattern_key->value){
			if((*pattern)[0] == 0){
				/* they are non-equal and there is NO * on appropriate position in the pattern
				   (== 0 means we are NOT on * place) */
				return false;
			}
		}				
		return true;
}

string IntKey::printKey() const {
	stringstream ss;
	ss << "[" << value << "]";
	return ss.str();
}


/* MultiKey method definition*/
size_t MultiKey::hashValue() const {
	size_t sum=0;
	for (KeyList::const_iterator ki = keyList.begin(); ki != keyList.end(); ++ki){
   	size_t hash = (*ki)->hashValue();
   	sum += hash;
   }
	return sum;
}

/* 
This method will check whether two MultiKeys are the same, 
respecting the structure.
*/
bool MultiKey::compareKey(const Key* pk) const {
	const MultiKey* pmk = dynamic_cast<const MultiKey*>(pk);
	if(pmk == 0){
		// they are not even of the same type
		return false;
	}else if (numElts() != pmk->numElts()){
		/* comparing different key structures */
		return false;
	}else{
		bool result = true;
		KeyList::const_iterator ki, ki2;
		KeyList keyList2 = pmk->keyList;
		for(ki = keyList.begin(), ki2 = keyList2.begin();
			  ki != keyList.end() || ki2 != keyList2.end(); 
			  ++ki, ++ki2){
   		bool elemRes = (*ki)->compareKey(*ki2);
   		if (elemRes == false){
   			result = false;
   			break;
			}
   	}
   	/* the result is true, if all the keys are identical */
		return result;
	}
}


/* Condition: no hierarchy of MultiKey */
bool MultiKey::matchPattern(const pair<const Key*, vector<int>* >& kpair) const{
		const Key* pat_key = kpair.first;
		vector<int>* pattern = kpair.second;
		int size = pattern->size();
		
		assert(size == pat_key->numElts());
		if(size != numElts()){
			/* they differ in number of elements in the multikey */
			return false;
		}
		
		const MultiKey* pattern_key = dynamic_cast<const MultiKey*> (pat_key);
		if (pattern_key == 0){
			/* trying to compare this of type MultiKey with Leaf object (IntKey, ...)*/
			return false;
		}
		
		int loopCount = 0;
		KeyList::const_iterator ki, pattern_ki;
		KeyList pattern_keyList = pattern_key->keyList; 
		for(ki = keyList.begin(), pattern_ki = pattern_keyList.begin(); 
				ki != keyList.end(), pattern_ki != pattern_keyList.end();
				++ki, ++pattern_ki, ++loopCount){
			if (!((*ki)->compareKey(*pattern_ki))){
				// all of those are Leafs (Non-MultiKeys)
				if((*pattern)[loopCount] == 0){
					/* they are non-equal and there is NO * on appropriate position in the pattern
					   (== 0 means we are NOT on * place) */
					return false;
				}
			}
		}				
		return true;
}



/* Counting is based on the fact that there will be only one MultiKey 
	in a tree structure */
void MultiKey::add(Key* key){
	keyList.push_back(key);
	numElements++;
}

/* TODO Remove method */

int MultiKey::numElts() const {
	return numElements;
	/* General structure working for hierarchical MultiKeys:
	int count=0;
	for (KeyList::const_iterator ki = keyList.begin(); ki != keyList.end(); ++ki){
   	count+=(*ki)->numElts();
   }
	return count;
	*/
}

string MultiKey::printKey() const {
	stringstream ss;
	for (KeyList::const_iterator ki = keyList.begin(); ki != keyList.end(); ++ki){
   	ss << (*ki)->printKey();
   }
	return ss.str();
}
