
#ifndef ALGO_TYPE_DEFINITIONS_DEMO
#define ALGO_TYPE_DEFINITIONS_DEMO

#include <iostream>
#include <fstream>
#include <list>
#include <map>
#include <queue>
#include <sstream>
#include <vector>
#include <tr1/tuple>

#include "DataTuple.h"

typedef int OrderID;
typedef int LocalID;

using namespace std;
using namespace tr1;
using namespace DBToaster::DemoAlgEngine;

typedef tuple<DataTuple, DataTuple> DataTuplesPair;
typedef tuple<int, int, double, int, int> SOBItuple;
typedef tuple<double, int, int, double, double, int, int> timedSOBItuple;
typedef tuple<int, int> PVpair;

struct AlgoMessages
{
    DataTuple tuple;
    int type;
    
};

#endif