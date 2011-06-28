#ifndef MULTIKEY_H
#define MULTIKEY_H

#include <iostream>
#include <cstring>
#include <list>
#include <cassert>

#include "Common.hpp"

using namespace std;

class Key;
typedef list<Key*> KeyList;	

class Key{
public:
	virtual ~Key(){}

	/* Operations */	
	/* maybe there are better implementations? 0 is INVALID */
	virtual size_t hashValue() const=0;
	virtual bool compareKey(const Key* pk) const=0;
	virtual bool matchPattern(const pair<const Key*, vector<int>* >& kpair) const=0;

	virtual void add(Key* key){}
	virtual int numElts()const=0;

	virtual string printKey() const=0;
};

class IntKey : public Key {
public:
	IntKey(int value) : Key() {this->value = value;}
	virtual ~IntKey(){}
	
	virtual size_t hashValue() const {return value;}
	virtual bool compareKey(const Key* pk) const;
	virtual bool matchPattern(const pair<const Key*, vector<int>* >& kpair) const;

	virtual int numElts()const{return 1;}
	
	virtual string printKey() const;
private:
	int value;
};

class MultiKey : public Key{
public: 
	MultiKey() : Key(), numElements(0) {}
	virtual ~MultiKey(){}
	
	virtual size_t hashValue() const;
	virtual bool compareKey(const Key* pk) const;
	virtual bool matchPattern(const pair<const Key*, vector<int>* >& kpair) const;
	
	virtual void add(Key* key);
	virtual int numElts() const;
	
	virtual string printKey() const;
private:
	KeyList keyList;
	int numElements;
};

#endif
