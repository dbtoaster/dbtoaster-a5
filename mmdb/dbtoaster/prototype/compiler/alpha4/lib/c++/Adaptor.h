#include <cstring>
#include <locale>
#include <sstream>
#include <stdlib.h>

using namespace std;
using namespace std::tr1;

map<int,tuple<double,double,double> > ORDBook1;
map<int,tuple<double,double,double> > ORDBook2;

int CSVAdpt(istream &is, const char delim, const char* arglist, const char* event, double* arg){
	string s,field;
	int count=0;
	char type[20];
	type[0]=arglist[0];
	for (int i=0;arglist[i]!=0;i++){
		if(arglist[i]==','){
			count++;
			type[count]=arglist[i+1];
			}
	}
	if(!getline(is,s))return 0;
	count=0;
	istringstream ss(s);
	while(getline(ss,field,delim)){
		istringstream input(field);
		switch(type[count]){
		case 'i':
			input >>arg[count];
			break;
		case 'f':
			input >>arg[count];
			break;
		case 'd':{
			char temp;
			int year,month,day;
			input >>year >>temp >>month >>temp >>day;
			arg[count]=year*10000+month*100+day;
		}
			break;
		case 'h':{
			locale loc;
			const collate<char>& coll = use_facet<collate<char> >(loc);
			arg[count]=coll.hash(field.data(),field.data()+field.length());
		}
			break;
		default:
			break;
		}
		count++;
	}
	if(!strcmp(event,"insert"))return 1;
	if(!strcmp(event,"delete"))return 2;
	return 3;
}

int ORDAdpt(istream &is, double* arg, int brokers){
	char char1='0',type;
	int id, temp;
	is >>temp >>char1 >>id>> char1 >> type >> char1 >>arg[1] >>char1 >>arg[0];
	if(brokers!=0)arg[2]=rand()%brokers;
	if(char1!=',')return 0;
	if(type=='B'){
		ORDBook1[id]=tuple<double,double,double>(arg[0],arg[1],arg[2]);
		return 1;
	}
	if(type=='S'){
		ORDBook2[id]=tuple<double,double,double>(arg[0],arg[1],arg[2]);
		return 3;
	}
	if(type=='D'){
		if(ORDBook1.find(id)!=ORDBook1.end()){
			arg[0]=get<0>(ORDBook1[id]);
			arg[1]=get<1>(ORDBook1[id]);
			arg[2]=get<2>(ORDBook1[id]);
		return 2;
		}
		if(ORDBook2.find(id)!=ORDBook2.end()){
			arg[0]=get<0>(ORDBook2[id]);
			arg[1]=get<1>(ORDBook2[id]);
			arg[2]=get<2>(ORDBook2[id]);
		return 4;
		}
		return 6;
	}
	return 5;
}
