
struct NodeID {
  1: string  host,
  2: i32     port  //could use 16 bits, but thrift ints are unsigned, and I don't care
}

typedef i64 MapID

typedef i64 Version

struct Entry {
  1:           MapID    source,
  2:           i64      key,
  3:           Version  version,
  4: optional  NodeID   node
}

typedef map<Entry,double> GetResult

enum PutFieldType {
  VALUE = 1, ENTRY = 2, ENTRYVALUE = 3
}

struct PutField {
  1:          PutFieldType  type, 
  2: optional double        value,
  3: optional Entry         entry
}

exception SpreadException {
  1: string why
}

service MapNode {
  void put(             1: Version         id,
                        2: i64             template,  //the put template ID (see the map file)
                        3: Entry           target,    //this is treated as param #0
                        4: list<PutField>  params     //params are offset by 1 (params[0] = #1)
                        ) throws (1:SpreadException error),

  GetResult get       ( 1: list<Entry>   target
                        ) throws (1:SpreadException error),

  oneway void fetch   ( 1: list<Entry>   target,
                        2: NodeID        destination,
                        3: Version       cmdid
                        ),

  oneway void pushget ( 1: GetResult     result,
                        2: Version       cmdid
                        ),
  
  string dump         ()

}