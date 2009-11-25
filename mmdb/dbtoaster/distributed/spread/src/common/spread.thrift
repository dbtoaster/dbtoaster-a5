
struct NodeID {
  1: string  host,
  2: i32     port  //could use 16 bits, but thrift ints are unsigned, and I don't care
}

typedef i64 MapID

typedef i64 Version

struct Entry {
  1:           MapID      source,
  2:           list<i64>  key,
}

typedef map<Entry,double> GetResult

enum PutFieldType {
  VALUE = 1, ENTRY = 2, ENTRYVALUE = 3
}

enum AggregateType {
  SUM = 1,
  MAX = 2,
  AVG = 3
}

struct PutField {
  1:          PutFieldType  type, 
  2:          string        name,
  3: optional double        value,
  4: optional Entry         entry
}

exception SpreadException {
  1: string why
}

struct PutParams {
  1: list<PutField> params
}

service MapNode {
  oneway void put     ( 1: Version         id,
                        2: i64             template,  //the put template ID (see the map file)
                        3: list<double>    params
                        ),

  oneway void mass_put( 1: Version         id,
                        2: i64             template,
                        3: i64             expected_gets,
                        4: list<double>    params,
                        ),
  
  GetResult get       ( 1: list<Entry>   target
                        ) throws (1:SpreadException error),

  oneway void fetch   ( 1: list<Entry>   target,      // -1(s) in the key field == wildcards
                        2: NodeID        destination,
                        3: Version       cmdid
                        ),

  oneway void push_get( 1: GetResult     result,
                        2: Version       cmdid
                        ),
  
  GetResult aggreget  ( 1: list<Entry>   target,
                        2: AggregateType agg
                        ) throws (1:SpreadException error),
  
  string dump  (),
  
  oneway void localdump  (),
  
}

service SwitchNode {
  void update( 1: string table, 
               2: list<string> params),
  
  string dump() throws (1:SpreadException error)
}

service SlicerNode {
  oneway void start_switch  ( ),
  oneway void start_node    ( 1: i32 port  ),
  oneway void start_client  ( ),
  void shutdown      ( ),
  oneway void start_logging ( 1: NodeID target ),
  oneway void receive_log   ( 1: string log_message ),
  string poll_stats  ( )
}
