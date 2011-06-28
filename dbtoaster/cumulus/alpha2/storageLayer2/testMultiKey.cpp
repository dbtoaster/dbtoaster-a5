#include "Common.hpp"
#include "MultiKey.hpp"

int main (int argc, char *argv[]){
	Key* ik1 = new IntKey(2);
	Key* ik2 = new IntKey(3);
	Key* ik3 = new IntKey(4);
	vector<int>* v1 = new vector<int>();
	v1->push_back(0);
	v1->push_back(0);
	vector<int>* v2 = new vector<int>();
	v2->push_back(0);
	v2->push_back(1);

	Key* mk1 = new MultiKey();
	mk1->add(ik1);
	mk1->add(ik2);
	cout << "Hash value(sum): " << mk1->hashValue() << ", number of elements " << mk1->numElts() << endl;

	Key* mk2 = new MultiKey();
	mk2->add(ik1);
	cout << "Compare equality for different sizes (should be 0) " << mk1->compareKey(mk2) << endl;
	cout << "Match pattern for different sizes (should be 0) : " << 
		mk2->matchPattern(pair<Key*, vector<int>* > (mk1, v1) ) << endl;
	mk2->add(ik2);
	cout << "Compare two equal MultiKeys (should be 1) " << mk1->compareKey(mk2) << endl;

	Key* mk3 = new MultiKey();
	mk3->add(ik1);
	mk3->add(ik3);
	cout << "Compare two different MultiKeys (should be 0) " << mk3->compareKey(mk1) << endl;	

	cout << "Match pattern - the same (should be 1) : " << 
		mk1->matchPattern(pair<Key*, vector<int>* > (mk2, v1) ) << endl;
		
	cout << "Match pattern - different (should be 0) : " << 
		mk1->matchPattern(pair<Key*, vector<int>* > (mk3, v1) ) << endl;
	
	cout << "Match pattern - different, but getSlice (should be 1) : " << 
		mk1->matchPattern(pair<Key*, vector<int>* > (mk3, v2) ) << endl;

	delete v1;
	v1=0;
	delete v2;
	v2=0;
	delete ik1;
	ik1=0;
	delete ik2;
	ik2=0;
	delete ik3;
	ik3=0;
	delete mk1;
	mk1=0;
	delete mk2;
	mk2=0;
	delete mk3;
	mk3=0;
	return (0);
}
