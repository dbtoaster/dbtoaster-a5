#ifndef EXCEPTIONS_HPP
#define EXCEPTIONS_HPP

#include <iostream>
using namespace std;

class MyException {
private:
	string str;
	
public:
	MyException(string s) : str(s){}
	
	string& what(){
		return str;
	}
};

class NonexistMapExc : public MyException {
public:
	NonexistMapExc (string mapName) 
		: MyException (string("Nonexisting map " + mapName)) {}
};

/*"Nonexisting map " + mapName*/
#endif
