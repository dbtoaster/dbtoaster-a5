#include "Common.hpp"
#include "MultiKey.hpp"
#include "WriteStorage.hpp"
#include "WSCollection.hpp"

/* TODO
WriteStorage should be initializes as a pointer, and as a pointer inserted into WSCollection
*/

int main (int argc, char *argv[]){
	try{
		/* instantiating class, also should be generated 
			Parameters are from Structures.hpp */
		WriteStorage ws;
		
		/* inserting patterns */
		Key* mk0 = new MultiKey();
		Key* ik00 = new IntKey(3);
		Key* ik01 = new IntKey(0);
		mk0->add(ik00);
		mk0->add(ik01);
		vector<int>* v3 = new vector<int>;
		v3->push_back(0);
		v3->push_back(1);
		ws.addSecStorage(pair<Key*, vector<int>* >(mk0, v3));
	
	
		/* creating key */
		Key* mk1 = new MultiKey();
		Key* ik10 = new IntKey(3);
		Key* ik11 = new IntKey(13);
		mk1->add(ik10);
		mk1->add(ik11);
		
		/* set and get values for particular mapkey and timestamp */
		ws.setValue(mk1, 30, 300);
		double value = ws.getValueBefore(mk1, 31);
		cout << value << endl;
		
		ws.setValue(mk1, 32, 320);
		value = ws.getValueBefore(mk1, 33);
		cout << value << endl;
		
		ws.setValue(mk1, 42, 420);
		value = ws.getValueBefore(mk1, 43);
		cout << value << endl;
		
		ws.setValue(mk1, 40, 400);
		value = ws.getValueBefore(mk1, 41);
		cout << value << endl;

		cout << "GetBefore for 42(expected 400) " << ws.getValueBefore(mk1, 42) << endl;
		cout << "GetBefore for 40(expected 400) " << ws.getValueBefore(mk1, 41) << endl;
		cout << "GetBefore for 30(expected 0) " << ws.getValueBefore(mk1, 30) << endl;

		/* delta testing */
		cout << "GET delta WriteLog(expected 20) " << ws.getDelta(mk1, 45) << endl;
		cout << "GET delta WriteLog(expected 20) " << ws.getDelta(mk1, 42) << endl;
		cout << "GET delta WriteLog(expected 300) " << ws.getDelta(mk1, 30) << endl;
		cout << "GET delta WriteLog(expected 0) " << ws.getDelta(mk1, 19) << endl;


		/* other key 
		 	Secondaries and primary share the same key copy, therefore it should not be changed from main program!
			That is why we have new definitions here */
		Key* mk2 = new MultiKey();
		Key* ik20 = new IntKey(3);
		Key* ik21 = new IntKey(14);
		mk2->add(ik20);
		mk2->add(ik21);
		ws.setValue(mk2, 30, 200);

		Key* mk3 = new MultiKey();
		Key* ik30 = new IntKey(5);
		Key* ik31 = new IntKey(0);
		mk3->add(ik30);
		mk3->add(ik31);
		ws.setValue(mk3, 33, 330);

		/* getting slice */
		cout << "GetSlice [3, *] at TS 35:" << endl;
		cout << ws.printSlice(ws.getSlice(pair<const Key*, vector<int>* >(mk0, v3), 35));
		
		cout << ws.printSecondaries();


		/* WSCollection tests */
		WSCollection wscol;
		wscol.insert(string("1"), &ws);

		/* write the result out */
		cout << "Before truncating" << endl << wscol;
		
		/*  truncation to all the ws in the wscollection */ 
		wscol.garbageCollectionAll(35);
		
		cout << "After truncating:" << endl << wscol;


		/* Cleanup */
		delete v3;
		v3=0;
		delete ik20;
		ik20=0;
		delete ik21;
		ik21=0;
		delete mk2;
		mk2=0;
		delete ik10;
		ik10=0;
		delete ik11;
		ik11=0;
		delete mk1;
		mk1=0;
		delete ik00;
		ik00=0;
		delete ik01;
		ik01=0;
		delete mk0;
		mk0=0;

	} catch(exception& e) {
		cerr << e.what() << endl;
		return (-1);
	} catch(MyException& e) {
		cerr << e.what() << endl;
		return (-2);
	} catch(...){
		cerr << "Unknown error." << endl;
		return (-3);
	}
	
	return (0);
}
