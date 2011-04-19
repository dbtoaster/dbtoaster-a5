#include "Common.hpp"
#include "WriteStorage.hpp"
#include "WriteLog.hpp"

int main (int argc, char *argv[]){
	try{
	
		/* instantiating class, also should be generated 
			Parameters are from Structures.hpp */
		WriteStorage<Key1, Hash1, Compare1, Match1> ws;
	
		/* inserting patterns */
		Key1 mk0;
		mk0.loc1 = 3;
		mk0.loc2 = 0;
		vector<int> v3;
		v3.push_back(0);
		v3.push_back(1);
		ws.addSecStorage(pair<Key1, vector<int> >(mk0, v3));
		
		/* inserting this into collection */
		WriteLog wl;
		wl.insert("1", ws);
	
		/* From now on, everything is done on WriteLog */
		WriteStorage<Key1, Hash1, Compare1, Match1>& wacq =
			wl.getMap< WriteStorage<Key1, Hash1, Compare1, Match1> >("1");

		
		/* creating key */
		Key1 mk1;
		mk1.loc1=3;
		mk1.loc2=13;	
		
		/* set and get values for particular mapkey and timestamp */
		wl.setValue(wacq, mk1, 30, 300);
		double value = wl.getValue(wacq, mk1, 31);
		cout << value << endl;
		
		wl.setValue(wacq, mk1, 32, 320);
		value = wl.getValue(wacq, mk1, 33);
		cout << value << endl;

		wl.setValue(wacq, mk1, 42, 420);
		value = wl.getValue(wacq, mk1, 43);
		cout << value << endl;
		
		wl.setValue(wacq, mk1, 40, 400);
		value = wl.getValue(wacq, mk1, 41);
		cout << value << endl;
		
		cout << "GET for 42[400] " << wl.getValue(wacq, mk1, 42) << endl;
		cout << "GET for 40[400] " << wl.getValue(wacq, mk1, 41) << endl;
		cout << "GET for 30[0] " << wl.getValue(wacq, mk1, 30) << endl;
		
		cout << "GET delta WriteLog[20] " << wl.getDelta(wacq, mk1, 45) << endl;
		cout << "GET delta WriteLog[20] " << wl.getDelta(wacq, mk1, 42) << endl;
		cout << "GET delta WriteLog[300] " << wl.getDelta(wacq, mk1, 30) << endl;
		cout << "GET delta WriteLog[0] " << wl.getDelta(wacq, mk1, 19) << endl;

		/* other key 
		 	Secondaries and primary share the same key copy, therefore it should not be changed from main program!
			That is why we have new definitions here */
		Key1 mk2;
		mk2.loc1=3;
		mk2.loc2=14;	
		wl.setValue(wacq, mk2, 30, 200);
		
		Key1 mk3;
		mk3.loc1=5;
		mk3.loc2=0;
		wl.setValue(wacq, mk3, 33, 330);
		
		/* write the result out */
		cout << "Before truncating" << endl << wl;
		
		/* WriteLog invocation */
		wl.truncateAll(35);
		
		cout << "After truncating:" << endl << wl;
		
		/* getting slice */
		cout << "GetSlice [3, *] at TS 35:" << endl;
		cout << wacq.printSlice(wacq.getSlice(pair<Key1, vector<int> >(mk0, v3), 35));
	
		/*cout << wacq.printSecondaries();*/

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
