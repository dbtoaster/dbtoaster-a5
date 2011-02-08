require 'node/multikeymap'

$write_nodes = []
$read_nodes  = []

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
  node_db_path = File.join(basepath, "node#{i}")
  FileUtils.mkdir_p(node_db_path) unless File.directory?(node_db_path);
  puts "Creating database for node #{i} map #{name} at #{node_db_path}";
  
  return MultiKeyMap.new(num_keys, patterns, name, basepath, "node#{i}", 0.0)
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
      MultiKeyMap.new(num_keys, [], name, in_path, "node#{i}")
    end
  }
end

# Should be kept in sync with Cumulus' Entry.compute_partition
def compute_partition(partition_keys, partition_dim_sizes, entry)
  partition_keys.collect { |i| entry[i] % partition_dim_sizes[i] }
end

def validate_entry(entry, keys)
  unless (entry.is_a? Array)
    puts "Error : Param is not an array, param class : #{params.class()}";
  end
  unless (entry.size == keys.size)
    puts "Error : Param of wrong length, param length : #{params.size}";
  end
end

def write_mk_entry(keys, partition_dims, partition_dim_sizes, node_partitions, mk_maps, entry, value)
  segment = compute_partition(partition_dims, partition_dim_sizes, entry)
  node_idx = node_partitions.index(segment)

  if node_idx.nil? then
    raise "No valid node partition found for #{segment.join(',').to_s}"
  end

  if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? node_idx) then
    node_mk_map = mk_maps[node_idx]

    # Validate entry
    validate_entry(entry, keys) unless keys.nil?;

    # Write out k,v to map
    raise "Error: Attempt to set a value for a wildcard key" if entry.include?(node_mk_map.wildcard);
    node_mk_map[entry] = value;
  end
end

# Repartitioning method, common to both bootstrap scripts.
def repartition(in_path, out_path, name, num_keys, existing_num_nodes,
                aps, partition_dims, partition_dim_sizes, node_partitions)
  
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, num_keys, aps, out_path)

  # Read from all maps, recomputing partitions.
  existing_dbs = load_db(in_path, name, existing_num_nodes, num_keys).compact
  existing_dbs.each do |edb_mk|
    counter = 0
    edb_mk.scan([]) do |entry, value|
      write_mk_entry(nil, partition_dims, partition_dim_sizes, node_partitions, node_dbs, entry, value)

      counter += 1
      if counter % 1000 == 0 then
        puts "Processed #{counter} tuples"
      end
    end
    
  end

  existing_dbs.each { |mk| mk.close if mk; }
  node_dbs.each { |mk| mk.close if mk; }
end

