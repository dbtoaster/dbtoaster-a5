#ifndef TPCH_H
#define TPCH_H

#include <sstream>
#include <string>

using namespace std;
using namespace tr1;

typedef long long int identifier;
typedef string date;

struct lineitem {
	identifier orderkey;
	identifier partkey;
	identifier suppkey;
	int linenumber;
	double quantity;
	double extendedprice;
	double discount;
	double tax;
	string returnflag;
	string linestatus;
	date shipdate;
	date commitdate;
	date receiptdate;
	string shipinstruct;
	string shipmode;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << orderkey << ", " << partkey << ", " << suppkey << ", "
			<< linenumber << ", " << quantity << ", "
			<< extendedprice << ", " << discount << ", " << tax << ", "
			<< returnflag << ", " << linestatus << ", " << shipdate << ", "
			<< commitdate << ", " << receiptdate << ", " << shipinstruct << ", "
			<< shipmode << ", " << comment;
		return r.str();
	}
};

struct order {
	identifier orderkey;
	identifier custkey;
	string orderstatus;
	double totalprice;
	date orderdate;
	string orderpriority;
	string clerk;
	int shippriority;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << orderkey << ", " << custkey << ", " << orderstatus << ", "
			<< totalprice << ", " << orderdate << ", "
			<< orderpriority << ", " << clerk << ", "
			<< shippriority << ", " << comment;
		return r.str();
	}
};

struct part {
	identifier partkey;
	string name;
	string mfgr;
	string brand;
	string type;
	int size;
	string container;
	double retailprice;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << partkey << ", " << name << ", " << mfgr << ", "
			<< brand << ", " << type << ", " << size << ", "
			<< container << ", " << retailprice << ", " << comment;
		return r.str();
	}
};

struct customer {
	identifier custkey;
	string name;
	string address;
	identifier nationkey;
	string phone;
	double acctbal;
	string mktsegment;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << custkey << ", " << name << ", " << address << ", "
			<< nationkey << ", " << phone << ", " << acctbal << ", "
			<< mktsegment << ", " << comment;
		return r.str();
	}
};

struct supplier {
	identifier suppkey;
	string name;
	string address;
	identifier nationkey;
	string phone;
	double acctbal;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << suppkey << ", " << name << ", " << address << ", "
			<< nationkey << ", " << phone << ", "
			<< acctbal << ", " << comment;
		return r.str();
	}
};

struct nation {
	identifier nationkey;
	string name;
	identifier regionkey;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << nationkey << ", " << name << ", "
			<< regionkey << ", " << comment;
		return r.str();
	}
};

struct region {
	identifier regionkey;
	string name;
	string comment;

	string as_string()
	{
		ostringstream r;
		r << regionkey << ", " << name << ", " << comment;
		return r.str();
	}
};


#endif
