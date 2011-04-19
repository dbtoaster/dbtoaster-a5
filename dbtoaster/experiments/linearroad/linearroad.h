#ifndef LRCLIENT_H
#define LRCLIENT_H

#include <sstream>
#include <string>
#include <vector>

using namespace std;

struct LRData {
    int rec_type;
    int t;
    int vid;
    int speed;
    int xway;
    int lane;
    int dir;
    int seg;
    int pos;
    int qid;
    int sinit;
    int send;
    int dow;
    int tod;
    int day;

    LRData() {}

    LRData(vector<int>& fields)
    {
		assert ( fields.size() == 15 );
		rec_type = fields[0];
		t = fields[1];
		vid = fields[2];
		speed = fields[3];
		xway = fields[4];
		lane = fields[5];
		dir = fields[6];
		seg = fields[7];
		pos = fields[8];
		qid = fields[9];
		sinit = fields[10];
		send = fields[11];
		dow = fields[12];
		tod = fields[13];
		day = fields[14];
    }

    string as_string() {
        ostringstream r_ss;
        r_ss << rec_type << ", " << t << ", " << vid << ", "
            << speed << ", " << xway << ", " << lane << ", "
            << dir << ", " << seg << ", " << pos << ", "
            << qid << ", " << sinit << ", " << send << ", "
            << dow << ", " << tod << ", " << day;
        return r_ss.str();
    }
};


struct AccInSeg {
    int xway;
    int seg;
    int dir;
    int m;
    int num_vehicles;
    //double emit;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << m << ", " << xway << ", " << seg << ", "
			<< dir << ", " << num_vehicles;

		return r_ss.str();
    }

};


struct Lav {
    int m;
    int xway;
    int seg;
    int dir;
    double lav;
    /*
    double emit;
    double lav_emit;
    double lav_preagg;
    double lav_preinsert;
    double lav_postinsert;
    */

    string as_string() {
    	ostringstream r_ss;
    	r_ss << m << ", " << xway << ", " << seg << ", "
			<< dir << ", " << lav;

		return r_ss.str();
    }

};

struct VehicleCount {
    int m;
    int xway;
    int seg;
    int dir;
    int num_vehicles;
    //double emit;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << m << ", " << xway << ", " << seg
			<< ", " << dir << ", " << num_vehicles;
    	return r_ss.str();
    }

};

struct TollCalculations {
    int m;
    int xway;
    int seg;
    int dir;
    int toll;
    double lav;
    /*
    double toll_tl;
    double toll_tr;
    double accident_t;
    */

    string as_string() {
    	ostringstream r_ss;
    	r_ss << m << ", " << xway << ", " << seg << ", "
			<< dir << ", " << toll << ", " << lav;
    	return r_ss.str();
    }

};




struct NotificationsWithMins {
    int t;
    int m;
    int vid;
    int xway;
    int seg;
    int dir;
    double l;
    int lane;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << t << ", " << m << ", " << vid << ", " << xway << ", "
			<< seg << ", " << dir << ", " << l << ", " << lane;
    	return r_ss.str();
    }

};


struct TollNotifications {
    int type;
    int vid;
    int t;
    double emit;
    double lav;
    int toll;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << type << ", " << vid << ", " << t << ", " << emit
			<< ", " << lav << ", " << toll;
    	return r_ss.str();
    }

};


struct AccidentNotifications {
    int type;
    int t;
    double emit;
    int vid;
    int seg;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << type << ", " << t << ", " << emit << ", "
			<< vid << ", " << seg;
    	return r_ss.str();
    }

};


struct AccountBalanceResult {
    int type;
    int t;
    double emit;
    int result_time;
    int vid;
    int qid;
    int balance;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << type << ", " << t << ", " << emit << ", "
			<< result_time << ", " << qid << ", " << balance;
    	return r_ss.str();
    }

};

struct DailyExpenditureResult {
    int type;
    int t;
    double emit;
    int qid;
    int expenditure;

    string as_string() {
    	ostringstream r_ss;
    	r_ss << type << ", " << t << ", " << emit << ", "
			<< qid << ", " << expenditure;
    	return r_ss.str();
    }

};

#endif
