
struct NodeID {
  1: string  host,
  2: i32     port  //could use 16 bits, but thrift ints are unsigned, and I don't care
}

typedef i64 MapID

typedef i64 Version

struct MapEntry {
  1:           MapID      source,
  2:           list<i64>  key,
}

typedef map<MapEntry,double> GetResult

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
  4: optional MapEntry      entry
}

exception SpreadException {
  1: string why,
  2: optional bool retry
}

struct PutParams {
  1: list<PutField> params
}

struct PutRequest {
  1: i64 template,
  2: i64 id_offset,
  3: i64 num_gets
}

struct GetRequest {
  1: NodeID target,
  2: i64    id_offset,
  3: list<MapEntry> entries
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
  
  GetResult get       ( 1: list<MapEntry>   target
                        ) throws (1:SpreadException error),

  oneway void fetch   ( 1: list<MapEntry>   target,      // -1(s) in the key field == wildcards
                        2: NodeID        destination,
                        3: Version       cmdid
                        ),

  oneway void push_get( 1: GetResult     result,
                        2: Version       cmdid
                        ),
  
  oneway void meta_request( 1: i64 base_cmd,
                            2: list<PutRequest> put_list,
                            3: list<GetRequest> get_list,
                            4: list<double> params)
  
  GetResult aggreget  ( 1: list<MapEntry>   target,
                        2: AggregateType agg
                        ) throws (1:SpreadException error),
  
  string dump  (),
  
  oneway void localdump  ()
  
}

service SwitchNode {
  void update( 1: string table, 
               2: list<string> params) throws (1:SpreadException error),
  
  string dump() throws (1:SpreadException error),
  
  oneway void request_backoff( 1: NodeID node ),
  oneway void finish_backoff( 1: NodeID node)
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
