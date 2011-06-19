#ifndef DBTOASTER_STDADAPTORS_H
#define DBTOASTER_STDADAPTORS_H

#include <string>
#include <list>
#include <map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/functional/hash.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/tuple/tuple.hpp>

#include "runtime.hpp"

namespace dbtoaster {
  namespace runtime {

    using namespace std;

    struct csv_adaptor : public stream_adaptor
    {
      stream_id id;
      stream_event_type type;
      string schema;
      string delimiter;
      boost::hash<std::string> field_hash;

      csv_adaptor(stream_id i) : id(i) {}

      csv_adaptor(stream_id i, string sch) : id(i), schema(sch) {
        validate_schema();
      }

      csv_adaptor(stream_id i, int num_params,
                  const pair<string,string> params[]) : id(i)
      {
        for (int i = 0; i< num_params; ++i) {
          string k = params[i].first;
          string v = params[i].second;
          cout << "csv params: " << k << ": " << v << endl;
          if ( k == "fields" ) {
            delimiter = v;
          } else if ( k == "schema" ) {
            schema = "";
            split_iterator<string::iterator> end;
            for (split_iterator<string::iterator> it =
                  make_split_iterator(v, first_finder(",", is_equal()));
                 it != end; ++it)
            {
              string ty = copy_range<std::string>(*it);
              if ( ty == "int" ) schema += "i";
              else if ( ty == "float" ) schema += "f";
              else if ( ty == "date" ) schema += "d";
              else if ( ty == "hash" ) schema += "h";
              else {
                cerr << "invalid csv schema type " << ty << endl;
                schema = "";
              }
            }
          } else if ( k == "eventtype" ) {
            type = v == "insert"? insert_tuple : delete_tuple;
          } else {
            cerr << "invalid csv adaptor parameter " << k << endl;
          }
        }

        validate_schema();
      }

      void validate_schema() {
        bool valid = true;
        string::iterator it = schema.begin();
        for (; valid && it != schema.end(); ++it) {
          switch(*it) {
            case 'e':  // event type
            case 'i':
            case 'f':
            case 'd':
            case 'h': break;
            default: valid = false; break;
          }
        }
        if ( !valid ) schema = "";
      }

      void process(const string& data, shared_ptr<list<stream_event> > dest)
      {
        if ( dest && schema != "" ) {
          // Interpret the schema.
          event_data tuple;
          string::iterator schema_it = schema.begin();
          bool valid = true;
          bool insert = true;

          split_iterator<string::const_iterator> field_end;
          for (split_iterator<string::const_iterator> field_it =
                  make_split_iterator(data, first_finder(delimiter, is_equal()));
               valid && schema_it != schema.end() && field_it != field_end;
               ++schema_it, ++field_it)
          {
            string field = copy_range<std::string>(*field_it);
            istringstream iss(field);
            bool ins; int i,y,m,d; float f;
            vector<string> date_fields;
            switch (*schema_it) {
              case 'e': iss >> ins; insert = ins; break;
              case 'i': iss >> i; tuple.push_back(i); break;
              case 'f': iss >> f; tuple.push_back(f); break;
              case 'h': tuple.push_back(field_hash(field)); break;
              case 'd':
                split(date_fields, field, is_any_of("-"));
                valid = false;
                if ( date_fields.size() == 3 ) {
                  y = atoi(date_fields[0].c_str());
                  m = atoi(date_fields[1].c_str());
                  d = atoi(date_fields[2].c_str());
                  if ( 0 < m && m < 13 && 0 < d && d <= 31) {
                    tuple.push_back(y*10000+m*100+d); valid = true;
                  }
                }
                break;
              default: valid = false; break;
            }
            valid = valid && !iss.fail();
          }

          if ( valid )  {
            stream_event e(insert? insert_tuple : delete_tuple, id, tuple);
            dest->push_back(e);
          } else {
            cout << "adaptor could not process " << data << endl;
          }
        }
      }
    };


    //////////////////////////////
    //
    // Order books

    namespace order_books
    {
      using namespace std;
      using namespace boost;
      using namespace dbtoaster::runtime;

      enum order_book_type { tbids, tasks, both };

      // Struct to represent messages coming off a socket/historical file
      struct order_book_message {
          double t;
          int id;
          string action;
          double volume;
          double price;
      };

      // Struct for internal storage, i.e. a message without the action or
      // order id.
      struct order_book_tuple {
          double t;
          int id;
          int broker_id;
          double volume;
          double price;
          order_book_tuple() {}

          order_book_tuple(const order_book_message& msg) {
            t = msg.t;
            id = msg.id;
            volume = msg.volume;
            price = msg.price;
          }

          order_book_tuple& operator=(order_book_tuple& other) {
            t = other.t;
            id = other.id;
            volume = other.volume;
            price = other.price;
            return *this;
          }

          void operator()(event_data& e) {
            if (e.size() > 0) e[0] = t; else e.push_back(t);
            if (e.size() > 1) e[1] = id; else e.push_back(id);
            if (e.size() > 2) e[2] = broker_id; else e.push_back(broker_id);
            if (e.size() > 3) e[3] = volume; else e.push_back(volume);
            if (e.size() > 4) e[4] = price; else e.push_back(price);
          }
      };

      typedef map<int, order_book_tuple> order_book;

      struct order_book_adaptor : public stream_adaptor {
        stream_id id;
        int num_brokers;
        order_book_type type;
        shared_ptr<order_book> bids;
        shared_ptr<order_book> asks;

        order_book_adaptor(stream_id i, int nb, order_book_type t)
          : id(i), num_brokers(nb), type(t)
        {
          bids = shared_ptr<order_book>(new order_book());
          asks = shared_ptr<order_book>(new order_book());
        }

        order_book_adaptor(stream_id id, int num_params,
                           pair<string, string> params[])
        {
          bids = shared_ptr<order_book>(new order_book());
          asks = shared_ptr<order_book>(new order_book());

          for (int i = 0; i < num_params; ++i) {
            string k = params[i].first;
            string v = params[i].second;
            cout << "order book adaptor params: "
                 << params[i].first << ", " << params[i].second << endl;

            if ( k == "book" ) {
              type = (v == "bids"? tbids : tasks);
            } else if ( k == "brokers" ) {
              num_brokers = atoi(v.c_str());
            } else if ( k == "validate" ) { // Ignore.
            } else {
              cerr << "Invalid order book param " << k << ", " << v << endl;
            }
          }
        }

        bool parse_error(const string& data, int field) {
          cerr << "Invalid field " << field << " message " << data << endl;
          return false;
        }

        // Expected message format: t, id, action, volume, price
        bool parse_message(const string& data, order_book_message& r) {
          string msg = data;
          char* start = &(msg[0]);
          char* end = start;
          char action;

          for (int i = 0; i < 5; ++i)
          {
              while ( *end && *end != ',' ) ++end;
              if ( start == end ) { return parse_error(data, i); }
              if ( *end == '\0' && i != 4 ) { return parse_error(data, i); }
              *end = '\0';

              switch (i) {
              case 0: r.t = atof(start); break;
              case 1: r.id = atoi(start); break;
              case 2:
                  action = *start;
                  if ( !(action == 'B' || action == 'S' ||
                         action == 'E' || action == 'F' ||
                         action == 'D' || action == 'X' ||
                         action == 'C' || action == 'T') )
                  {
                     return parse_error(data, i);
                  }

                  r.action = action;
                  break;

              case 3: r.volume = atof(start); break;
              case 4: r.price = atof(start); break;
              default: return parse_error(data, i);
              }

              start = ++end;
          }
          return true;
        }

        void process_message(const order_book_message& msg,
                             shared_ptr<list<stream_event> > dest)
        {
            bool valid = true;
            order_book_tuple r(msg);
            stream_event_type t = insert_tuple;

            if ( msg.action == "B" ) {
              if (type == tbids || type == both) {
                r.broker_id = ((int) rand()) % 9 + 1;
                (*bids)[msg.id] = r;
                t = insert_tuple;
              } else valid = false;
            }
            else if ( msg.action == "S" ) {
              if (type == tasks || type == both) {
                r.broker_id = ((int) rand()) % 9 + 1;
                (*asks)[msg.id] = r;
                t = insert_tuple;
              } else valid = false;
            }

            else if ( msg.action == "E" ) {
              order_book_tuple x;
              bool x_valid = true;
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                x = r = bid_it->second;
                r.volume -= msg.volume;
                if ( r.volume <= 0.0 ) { bids->erase(bid_it); valid = false; }
                else { (*bids)[msg.id] = r; }
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  x = r = ask_it->second;
                  r.volume -= msg.volume;
                  if ( r.volume <= 0.0 ) { asks->erase(ask_it); valid = false; }
                  else { (*asks)[msg.id] = r; }
                } else {
                  //cout << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                  x_valid = false;
                }
              }
              if ( x_valid ) {
                event_data fields(5);
                x(fields);
                stream_event y(delete_tuple, id, fields);
                dest->push_back(y);
              }
              t = insert_tuple;
            }

            else if ( msg.action == "F" )
            {
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                r = bid_it->second;
                bids->erase(bid_it);
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  r = ask_it->second;
                  asks->erase(ask_it);
                } else {
                  //cout << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                }
              }
              t = delete_tuple;
            }

            else if ( msg.action == "D" )
            {
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                r = bid_it->second;
                bids->erase(bid_it);
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  r = ask_it->second;
                  asks->erase(ask_it);
                } else {
                  //cout << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                }
              }
              t = delete_tuple;
            }

            /*
            // ignore for now...
            else if ( v->action == "X")
            else if ( v->action == "C")
            else if ( v->action == "T")
            */
            else { valid = false; }


            if ( valid ) {
              event_data fields(5);
              r(fields);
              stream_event e(t, id, fields);
              dest->push_back(e);
            }
        }

        void process(const string& data, shared_ptr<list<stream_event> > dest)
        {
            // Grab a message from the data.
            order_book_message r;
            bool valid = parse_message(data, r);

            if ( valid ) {
              // Process its action, updating the internal book.
              process_message(r, dest);
            }
        }
      };
    }

    //////////////////////////////
    //
    // TPCH files

    namespace tpch
    {
      typedef long long int identifier;
      typedef string date;

      struct lineitem
      {
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

        string as_string() {
          ostringstream r;
          r << orderkey << ", " << partkey << ", " << suppkey << ", "
            << linenumber << ", " << quantity << ", " << extendedprice
            << ", " << discount << ", " << tax << ", " << returnflag
            << ", " << linestatus << ", " << shipdate << ", " << commitdate
            << ", " << receiptdate << ", " << shipinstruct << ", "
            << shipmode << ", " << comment;
            return r.str();
        }
      };

      struct order
      {
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
                    << totalprice << ", " << orderdate << ", " << orderpriority
                    << ", " << clerk << ", " << shippriority << ", " << comment;
            return r.str();
        }
      };

      struct part
      {
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
            r << partkey << ", " << name << ", " << mfgr << ", " << brand << ", "
                    << type << ", " << size << ", " << container << ", "
                    << retailprice << ", " << comment;
            return r.str();
        }
      };

      struct customer
      {
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
            r << custkey << ", " << name << ", " << address << ", " << nationkey
                    << ", " << phone << ", " << acctbal << ", " << mktsegment
                    << ", " << comment;
            return r.str();
        }
      };

      struct supplier
      {
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
            r << suppkey << ", " << name << ", " << address << ", " << nationkey
                    << ", " << phone << ", " << acctbal << ", " << comment;
            return r.str();
        }
      };

      struct partsupp
      {
        identifier partkey;
        identifier suppkey;
        int availqty;
        double supplycost;
        string comment;

        string as_string()
        {
            ostringstream r;
            r << partkey << ", " << suppkey << ", " << availqty << ", "
                << supplycost << ", " << comment;
            return r.str();
        }
      };

      struct nation
      {
        identifier nationkey;
        string name;
        identifier regionkey;
        string comment;

        string as_string()
        {
            ostringstream r;
            r << nationkey << ", " << name << ", " << regionkey << ", " << comment;
            return r.str();
        }
      };

      struct region
      {
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


      //
      // Field parsers

      inline void parseLineitemField(int field, lineitem& r, char* data)
      {
          switch(field) {
          case 0: r.orderkey = atoll(data); break;
          case 1: r.partkey = atoll(data); break;
          case 2: r.suppkey = atoll(data); break;
          case 3: r.linenumber = atoi(data); break;
          case 4: r.quantity = atof(data); break;
          case 5: r.extendedprice = atof(data); break;
          case 6: r.discount = atof(data); break;
          case 7: r.tax = atof(data); break;
          case 8: r.returnflag = string(data); break;
          case 9: r.linestatus = string(data); break;
          case 10: r.shipdate = date(data); break;
          case 11: r.commitdate = date(data); break;
          case 12: r.receiptdate = date(data); break;
          case 13: r.shipinstruct = string(data); break;
          case 14: r.shipmode = string(data); break;
          case 15: r.comment = string(data); break;
          default:
              cerr << "Invalid lineitem field id " << field << endl;
              break;
          }
      }

      inline void parseOrderField(int field, order& r, char* data)
      {
          switch(field) {
          case 0: r.orderkey = atoll(data); break;
          case 1: r.custkey = atoll(data); break;
          case 2: r.orderstatus = string(data); break;
          case 3: r.totalprice = atof(data); break;
          case 4: r.orderdate = date(data); break;
          case 5: r.orderpriority = string(data); break;
          case 6: r.clerk = string(data); break;
          case 7: r.shippriority = atoi(data); break;
          case 8: r.comment = string(data); break;
          default:
              cerr << "Invalid order field id " << field << endl;
              break;
          }
      }

      inline void parsePartField(int field, part& r, char* data)
      {
          switch(field) {
          case 0: r.partkey = atoll(data); break;
          case 1: r.name = string(data); break;
          case 2: r.mfgr = string(data); break;
          case 3: r.brand= string(data); break;
          case 4: r.type = string(data); break;
          case 5: r.size = atoi(data); break;
          case 6: r.container = string(data); break;
          case 7: r.retailprice = atof(data); break;
          case 8: r.comment = string(data); break;
          default:
              cerr << "Invalid part field id " << field << endl;
              break;
          }
      }

      inline void parseCustomerField(int field, customer& r, char* data)
      {
          switch(field) {
          case 0: r.custkey = atoll(data); break;
          case 1: r.name = string(data); break;
          case 2: r.address = string(data); break;
          case 3: r.nationkey = atoll(data); break;
          case 4: r.phone = string(data); break;
          case 5: r.acctbal = atof(data); break;
          case 6: r.mktsegment = string(data); break;
          case 7: r.comment = string(data); break;
          default:
              cerr << "Invalid customer field id " << field << endl;
              break;
          }
      }

      inline void parseSupplierField(int field, supplier& r, char* data)
      {
          switch(field) {
          case 0: r.suppkey = atoll(data); break;
          case 1: r.name = string(data); break;
          case 2: r.address = string(data); break;
          case 3: r.nationkey = atoll(data); break;
          case 4: r.phone = string(data); break;
          case 5: r.acctbal = atof(data); break;
          case 6: r.comment = string(data); break;
          default:
              cerr << "Invalid supplier field id " << field << endl;
              break;
          }
      }

      inline void parsePartSuppField(int field, partsupp& r, char* data)
      {
          switch(field) {
          case 0: r.partkey = atoll(data); break;
          case 1: r.suppkey = atoll(data); break;
          case 2: r.availqty = atoi(data); break;
          case 3: r.supplycost = atof(data); break;
          case 4: r.comment = string(data); break;
          default:
              cerr << "Invalid partsupp field id " << field << endl;
              break;
          }
      }

      inline void parseNationField(int field, nation& r, char* data)
      {
          switch(field) {
          case 0: r.nationkey = atoll(data); break;
          case 1: r.name = string(data); break;
          case 2: r.regionkey = atoll(data); break;
          case 3: r.comment = string(data); break;
          default:
              cerr << "Invalid nation field id " << field << endl;
              break;
          }
      }

      inline void parseRegionField(int field, region& r, char* data)
      {
          switch(field) {
          case 0: r.regionkey = atoll(data); break;
          case 1: r.name = string(data); break;
          case 2: r.comment = string(data); break;
          default:
              cerr << "Invalid region field id " << field << endl;
              break;
          }
      }

      template<typename T>
      struct tpch_adaptor : public csv_adaptor {
        typedef boost::function<void (int, T&, char*)> field_parser;
        field_parser parser;
        int num_fields;

        tpch_adaptor(stream_id i, string tpch_rel) : csv_adaptor(i) {
            parser = get_parser(tpch_rel);
        }

        tpch_adaptor(stream_id i, string tpch_rel, int num_params,
                     const pair<string, string> params[])
          : csv_adaptor(i,num_params,params)
        {
            parser = get_parser(tpch_rel);
        }

        field_parser get_parser(string tpch_rel) {
          field_parser p;
          if ( tpch_rel == "lineitem" ) {
              p = &parseLineitemField; num_fields = 16;
          } else if ( tpch_rel == "orders" ) {
              p = &parseOrderField; num_fields = 9;
          } else if ( tpch_rel == "customer" ) {
              p = &parseCustomerField; num_fields = 8;
          } else if ( tpch_rel == "supplier") {
              p = &parseSupplierField; num_fields = 7;
          } else if ( tpch_rel == "part") {
              p = &parsePartField; num_fields = 9;
          } else if ( tpch_rel == "partsupp") {
              p = &parsePartSuppField; num_fields = 5;
          } else if ( tpch_rel == "nation") {
              p = &parseNationField; num_fields = 4;
          } else if ( tpch_rel == "region") {
              p = &parseRegionField; num_fields = 3;
          } else {
            cerr << "Invalid TPCH relation " << tpch_rel << endl;
          }

          return p;
        }

        bool parse_error(const string& data, int field) {
          cerr << "Invalid field " << field << ": " << data << endl;
          return false;
        }

        bool parse_record(const string& data, T& r) {
          string msg = data;
          char* start = &(msg[0]);
          char* end = start;
          for (int i = 0; i < num_fields; ++i) {
            while ( *end && *end != delimiter ) ++end;
            if ( start == end ) return parse_error(data, i);
            if ( *end == '\0' && i != (num_fields - 1) ) return parse_error(data, i);
            *end = '\0';

            parser(i, r, start);
            start = ++end;
          }
          return true;
        }

        void process(const string& data, shared_ptr<list<stream_event> > dest)
        {
          T r;
          if ( parse_record(data, r) ) {
            stream_event e(id, insert_tuple, r);
            dest->push_back(e);
          }
        }
      };
    }
  }
}

#endif
