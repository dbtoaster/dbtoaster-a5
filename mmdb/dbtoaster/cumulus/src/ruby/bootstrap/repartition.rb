
require 'node/multikeymap'
require 'fileutils'
require 'getoptlong'

$dataset_path = "/home/yanif/datasets/tpch/100m"
$output_path = "."
$repartition_spec = nil

$quiet = false

$map_names   = []
$write_nodes = []
$read_nodes  = []

#
# Internals
#############

$env = nil

$tables = {
  "LINEITEM"  => [ "lineitem.tbl", "lineitem.tbl.u1", 6000000, nil   ,        ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"] ],
  "ORDERS"    => [ "orders.tbl"  , "order.tbl.u1"   , 1500000, "o_orderkey",  ["o_orderkey", "o_custkey", "o_status", "o_totalprice", "o_orderdate", "o_opriority", "o_clerk", "o_spriority", "o_comment"] ],
  "PART"      => [ "part.tbl"    , nil              , 200000 , "p_partkey",   ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"] ],
  "PARTSUPP"  => [ "partsupp.tbl", nil              , 800000 , nil   ,        ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"] ],
  "CUSTOMER"  => [ "customer.tbl", nil              , 150000 , "c_custkey",   ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"] ],
  "SUPPLIER"  => [ "supplier.tbl", nil              , 10000  , "s_suppkey",   ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"] ],
  "REGION"    => [ "region.tbl"  , nil              , 5      , "r_regionkey", ["r_regionkey", "r_name", "r_comment"] ],
  "NATION"    => [ "nation.tbl"  , nil              , 25     , "n_nationkey", ["n_nationkey", "n_name", "n_regionkey", "n_comment"] ],
}

class Array
  def each_cross_product(depth = 0, pattern = Array.new)
    if depth >= size then
      if depth == 0 then yield pattern.clone else yield end;
    else
      self[depth].each do |val|
        pattern.push(val);
        each_cross_product(depth+1, pattern) { if depth == 0 then yield pattern.clone else yield end };
        pattern.pop;
      end
    end
  end
  
  def cross_product(depth = 0, pattern = Array.new)
    ret = Array.new;
    each_cross_product { |out| ret.push(out) };
    ret;
  end
end

def initialize_node_db(name, i, num_keys, patterns, basepath)
  puts "Creating database for node #{i} map #{name}";
  node_db_path = File.join("#{basepath}", "node#{i}")
  
  # TODO: pfiles
  MultiKeyMap.new(num_keys, patterns, name, [], node_db_path)
end

def initialize_db(name, num_nodes, num_keys, patterns, basepath = ".")
  (0...num_nodes).collect do |i|
    if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? i) then
      initialize_node_db(name, i, num_keys, patterns, basepath)
    end
  end
end

def load_db(in_path, name, num_nodes, num_keys)
  (0...num_nodes).collect { |i|
    if $read_nodes.nil? or ($read_nodes.length == 0) or ($read_nodes.include? i) then
      puts "Loading database for node #{i} map #{name}"
      existing_db_path = File.join("#{in_path}", "node#{i}")
      MultiKeyMap.new(num_keys, [], name, [], existing_db_path)
    end
  }
end

# Should be kept in sync with Spread's Entry.compute_partition
def compute_partition(partition_keys, partition_dim_sizes, entry)
  partition_keys.collect { |i| entry[i] % partition_dim_sizes[i] }
end

def repartition(in_path, out_path, name, num_keys, existing_num_nodes,
                aps, partition_dims, partition_dim_sizes, node_partitions)
  
  # Create multikeymaps
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, num_keys, aps, out_path)

  # Read from all dbs, recomputing partitions.
  existing_dbs = load_db(in_path, name, existing_num_nodes, num_keys).compact
  existing_dbs.each do |mk|
    counter = 0

    mk.scan do |entry, value|
      segment = compute_partition(partition_dims, partition_dim_sizes, entry)
      node_idx = node_partitions.index(segment)
      if node_idx.nil? then
        raise "Error: No valid node partition found for #{segment.join(',').to_s}"
      end

      if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? node_idx) then
        node_mk_map = node_dbs[node_idx]
        node_mk_map[entry] = value
      end

      counter += 1
      if counter % 10000 == 0 then
        puts "Processed #{counter} tuples" unless $quiet;
      end
    end

  end

  # Close maps
  existing_dbs.each { |mk| mk.close }
  node_dbs.each { |mk| mk.close }
end


#####################
#
# Top level

def print_usage
  puts "Usage: repartition.rb -d <in path> -o <out path> <repartition spec>"
  exit
end

#
# Spec format: <name>/<existing # nodes>/<dims indexes>/<dim sizes>[/<access patterns>]
# where:
#   dim indexes: indexes of partition keys in the full set of map keys
#   dim sizes: # of partitions for each dimension in dim indexes
#   access partition example: 0|1|2|0,1|1,2
#     specifies 4 access partitions, separated by '|', with dims separated by ','

def process_repartition_spec(rspec)
  name_s, nk_s, en_s, pk_l,ds_l,ap_l = rspec.strip.split("/")
  if ([ name_s, nk_s, en_s, pk_l, ds_l ].any? { |x| x.nil? }) then
    print_usage
  end

  name = name_s.to_s
  num_keys = nk_s.to_i

  if $maps.nil? or ($maps.length == 0) or ($maps.include? name) then
    existing_num_nodes = en_s.to_i

    partition_dims = pk_l.split(",").collect { |x| x.to_i }
    partition_dim_sizes = ds_l.split(",").collect { |x| x.to_i }
    node_partitions = partition_dim_sizes.collect { |di| (0...di).to_a }.cross_product

    aps = unless ap_l.nil? then
            ap_l.split("|").map { |x| x.split(".").map { |y| y.to_i } }
          else [] end

    puts "Repartitioning #{name}" +
      " en: #{existing_num_nodes.to_s}" +
      " pk: " + partition_dims.join(",")
      " ds: " +  partition_dim_sizes.join(",")
      " aps: " + aps.collect { |x| x.join(".") }.join("|")

    repartition($dataset_path, $output_path,
                name, num_keys, existing_num_nodes, aps,
                partition_dims, partition_dim_sizes, node_partitions)
  end
end


def main
  if $repartition_spec.nil? then print_usage end

  initialize_env;
  lines = File.open($repartition_spec, "r").readlines
  lines.each { |l| process_repartition_spec(l) }
  $env.close
end

opts = GetoptLong.new(
  [ "-q", "--quiet",        GetoptLong::NO_ARGUMENT ],
  [ "-r", "--repartition",  GetoptLong::REQUIRED_ARGUMENT ],
  [ "-o", "--output",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-d", "--domains",      GetoptLong::REQUIRED_ARGUMENT ],
  [ "-m", "--maps",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-l", "--read-nodes",   GetoptLong::REQUIRED_ARGUMENT ],
  [ "-s", "--write-nodes",  GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt,arg|
  case opt
    when "-q", "--quiet"        then $quiet = true;
    when "-r", "--repartition"  then $repartition_spec = arg;
    when "-d", "--domains"      then (if File.directory? arg then
                                       $dataset_path = arg
                                      else raise "Error: no such directory #{arg}" end);
    when "-o", "--output"       then (if File.directory? arg then
                                        $output_path = arg 
                                      else raise "Error: no such directory #{arg}" end);
    when "-m", "--maps"         then $maps = arg.split(",");
    when "-l", "--read-nodes"   then $read_nodes = arg.split(",").collect { |x| x.to_i };
    when "-s", "--write-nodes"  then $write_nodes = arg.split(",").collect { |x| x.to_i };
  end
end

$repartition_spec = ARGV[0] if ($repartition_spec.nil?) and (ARGV.length > 0);
main;

