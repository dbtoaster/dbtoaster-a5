
struct NodeID {
  1: i32  host,
  2: i16  port
}

typedef i64 MapID

typedef i64 Version

struct Entry {
  1: MapID    source,
  2: i64      key,
  3: Version  version,
  4: NodeID   node
}

typedef map<Entry,double> GetResult

enum PutFieldType {
  VALUE = 1, ENTRY = 2, ENTRYVALUE = 3
}

struct PutField {
  1:          PutFieldType  type 
  2:          i64           id
  3: optional double        value
  4: optional Entry         entry
}

struct PutCommand {
  1: Version         id
  2: i64             command
  3: list<PutField>  params
}

exception SpreadException {
  1: string why
}

service MapNode {
  void put(             1: PutCommand    cmd
                        ) throws (1:SpreadException error),

  oneway void fetch   ( 1: list<Entry>   target,
                        2: NodeID        destination,
                        3: Version       cmdid
                        ),

  oneway void pushget ( 1: GetResult     result,
                        2: Version       cmdid
                        ),

  GetResult get       ( 1: list<Entry>   target
                        ) throws (1:SpreadException error)
}