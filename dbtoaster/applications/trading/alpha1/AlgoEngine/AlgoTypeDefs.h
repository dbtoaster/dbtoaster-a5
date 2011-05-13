
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

/*
 *Type definitions used by more then one class
 */

typedef tuple<DataTuple, DataTuple> DataTuplesPair;
typedef tuple<int, int, double, int, int> SOBItuple;
typedef tuple<double, int, int, double, double, int, int> timedSOBItuple;
typedef tuple<int, int> PVpair;

//Message sent to Exchange Simulator
struct AlgoMessages
{
    DataTuple tuple;
    int type;    //type indicates if this is an update message i.e. will Server send a responce to this message
    
};

#endif