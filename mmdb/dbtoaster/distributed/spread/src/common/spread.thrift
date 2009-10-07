
struct NodeID {
  1: string  host,
  2: i32     port  //could use 16 bits, but thrift ints are unsigned, and I don't care
}

typedef i64 MapID

typedef i64 Version

struct Entry {
  1:           MapID      source,
  2:           list<i64>  key,
  3:           Version    version,
  4: optional  NodeID     node
}

typedef map<Entry,double> GetResult

enum PutFieldType {
  VALUE = 1, ENTRY = 2, ENTRYVALUE = 3
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

service MapNode {
  oneway void put(      1: Version         id,
                        2: i64             template,  //the put template ID (see the map file)
                        3: Version         oldVersion,
                        4: list<PutField>  params     //0-based; The first entry in the list is param #0
                        ),
  oneway void massput(  1: Version         id,
                        2: i64             template,
                        3: list<i64>       partitions,
                        4: Version         oldVersion,
                        5: i64             expectedGets,
                        6: list<PutField>  params,
                        7: list<Entry>     history    //Individual puts that this put is dependent on
                        ),
  
  GetResult get       ( 1: list<Entry>   target
                        ) throws (1:SpreadException error),

  oneway void fetch   ( 1: list<Entry>   target,      // -1(s) in the key field == wildcards
                        2: NodeID        destination,
                        3: Version       cmdid
                        ),

  oneway void pushget ( 1: GetResult     result,
                        2: Version       cmdid
                        ),
  
  
  string dump         ()

}