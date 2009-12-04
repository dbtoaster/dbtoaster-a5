#!/usr/bin/env ruby

require 'oci8'
require 'bdb2'
require 'fileutils'
require 'getoptlong'

$dataset_path = "/home/yanif/datasets/tpch/100m"
$output_path = "."
$spec_file = nil
$repartition_spec = nil

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

def initialize_env()
  $env = Bdb::Env.new(0);
  $env.cachesize = 128*1024*1024
  $env.open(".", Bdb::DB_INIT_CDB | Bdb::DB_INIT_MPOOL | Bdb::DB_CREATE, 0);
end

def initialize_node_db(name, i, patterns, basepath)
  puts "Creating database for node #{i} map #{name}";

  #puts "Getting env db..."
  node_primary_db = $env.db;
  #puts "Got env db..."

  if not(File.directory?(File.join("#{basepath}","node#{i}"))) then
    FileUtils.makedirs(File.join("#{basepath}", "node#{i}"))
  end

  node_db_file = File.join("#{basepath}", "node#{i}", "db_#{name}_primary.db")

  #puts "Opening primary..."
  node_primary_db.open(nil, node_db_file, nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
  node_secondary_dbs = Hash.new;
  node_db = Array.new;
  j = 0;
  #puts "Opened primary."

  patterns.each do |pattern|
    #puts "#{pattern}"

    #puts "Getting secondary default db"
    node_db[j] = $env.db;
    #puts "Got secondary default db"

    node_db[j].flags = Bdb::DB_DUPSORT;
    node_pdb_file = File.join("#{basepath}","node#{i}", "db_#{name}_#{j}.db")

    #puts "Opening #{node_pdb_file}"
    node_db[j].open(nil, node_pdb_file, nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
    node_primary_db.associate(nil, node_db[j], 0,
      proc { |sdb, key, data| Marshal.dump(pattern.map{|k| Marshal.load(key)[k]})});
    node_secondary_dbs[pattern] = node_db[j];
    j = j + 1;
  end
  [node_primary_db, node_secondary_dbs]
end

def initialize_db(name, num_nodes, patterns, basepath = ".")
  (0...num_nodes).collect do |i|
    if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? i) then
      initialize_node_db(name, i, patterns, basepath)
    end
  end
end

def load_db(in_path, name, num_nodes)
  (0...num_nodes).collect { |i|
    if $read_nodes.nil? or ($read_nodes.length == 0) or ($read_nodes.include? i) then
      puts "Loading database for node #{i} map #{name}"
      node_db = $env.db
      node_db_file = "#{in_path}/node#{i}/db_#{name}_primary.db"
      node_db.open(nil, node_db_file, nil, Bdb::Db::HASH, Bdb::DB_RDONLY, 0)
      node_db
    end
  }
end

def finished(numvars, domfiles)
  domfiles.select{ |v,f| f.eof }.length == numvars
end

def next_values(domfiles, lastvals)
  read = true
  read_idx = lastvals.length == 0? 0 : -1

  # Find files where all successive files are at EOF.
  l = domfiles.length
  done_files = domfiles.select do |x,f|
    i = domfiles.index([x,f])
    f.eof and
      (domfiles[i..-1].select { |x2,f2| f2.eof }.length == (l-i))
  end

  # Reset successor files unless the first file has hit EOF (in which case we're done..)
  if not(done_files.nil? || done_files.length == 0) then
    reset_idx = domfiles.index(done_files.first)
    if reset_idx > 0 then
      read_idx = reset_idx-1
      (reset_idx...domfiles.length).each { |i| domfiles[i][1].rewind }
    else read = false end
  end

  # Use prev values, and any new values for files reset
  # TODO: duplicate elimination
  if read then
    lastvals[0...read_idx].
      concat(domfiles[read_idx..-1].map do |x,f|
        p,v=x; f.readline.strip.split("|")[p.to_i]
      end)
  else [] end
end

def interpret_keys(keys, names, values, result)
  keys.map do |k|
    if k =~ /Q\./ then
      result[k.gsub(/Q\.([0-9]+)/, "\\1").to_i]
    else values[names.index(k)] end
  end
end

def validate_entry(entry, keys)
  unless (entry.is_a? Array)
    puts "Error : Param is not an array, param class : #{params.class()}";
  end
  unless (entry.size == keys.size)
    puts "Error : Param of wrong length, param length : #{params.size}";
  end
end

# Should be kept in sync with Spread's Entry.compute_partition
def compute_partition(partition_keys, partition_dim_sizes, entry)
  partition_keys.collect { |i| entry[i] % partition_dim_sizes[i] }
end

def generate_from_domains(in_path, out_path, name, domvars, query, bindvars, keys, aps,
                          partition_dims, partition_dim_sizes, node_partitions)
  domfiles = domvars.map do |r,p,v|
      [[p,v], File.open(File.join(in_path, $tables[r][0]), "r") ]
    end
  names = domfiles.map { |x,f| x[1] }
  values = next_values(domfiles,[])

  # Connect, prepare map query
  conn = OCI8.new(nil,nil)
  cursor = conn.parse(query)

  puts "Executing #{query}"

  # Create BerkeleyDB database, and secondary indexes from aps
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, aps, out_path)

  counter = 0
  while not(finished(domvars.length, domfiles))
    values.each_index do |i|
      cursor.bind_param(":"+names[i],values[i])
    end
    cursor.exec()
    while r = cursor.fetch()
      entry = interpret_keys(keys,names,values,r)
      segment = compute_partition(partition_dims, partition_dim_sizes, entry)
      node_idx = node_partitions.index(segment)

      if node_idx.nil? then
        raise "No valid node partition found for #{segment.join(',').to_s}"
      end

      if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? node_idx) then
        primary, secondaries = node_dbs[node_idx]

        # Validate entry
        validate_entry(entry, keys)

        # Write out k,v to BDB
        raise "Error: Attempt to set a value for a wildcard key" if entry.include?(-1);
        primary.put(nil, Marshal.dump(entry), r[-1].to_s, 0);
      end
    end

    values = next_values(domfiles,values)

    counter += 1
    if (counter % 10000) == 0 then
      puts "Processed #{counter} keys for #{name}"
    end
  end

  puts "Map #{name} size: #{counter}"

  # Close databases
  node_dbs.each do |primary, secondaries|
    unless secondaries.nil? then secondaries.each_value { |db| db.close(0) } end
    unless primary.nil? then primary.close(0) end
  end

  domfiles.each { |x,f| f.close }
end

def generate_from_query(out_path, name, query, keys, aps,
                        partition_dims, partition_dim_sizes, node_partitions)
  # Connect, prepare map query
  conn = OCI8.new(nil,nil)
  cursor = conn.parse(query)

  puts "Executing #{query}"

  # Create BerkeleyDB database, and secondary indexes from aps
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, aps, out_path)

  #puts "Actually execing query..."

  counter = 0
  cursor.exec()
  while r = cursor.fetch()
    #puts "#{name} #{counter}"

    entry = keys.map { |k| r[k.sub(/Q\./,"").to_i] }
    segment = compute_partition(partition_dims, partition_dim_sizes, entry)
    node_idx = node_partitions.index(segment)

    if node_idx.nil? then
      puts "NS: "+keys.join(",") + " " + entry.join(",")
      puts "R: " + r.to_s
      raise "No valid node partition found for  #{segment.join(',').to_s}"
    end

    if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? node_idx) then
      primary, secondaries = node_dbs[node_idx]

      # Validate entry
      validate_entry(entry, keys)

      # Write out k,v to BDB
      raise "Error: Attempt to set a value for a wildcard key" if entry.include?(-1);
      primary.put(nil, Marshal.dump(entry), r[-1].to_s, 0);
    end

    counter += 1
    if (counter % 10000) == 0 then
      puts "Processed #{counter} tuples for #{name}"
    end
  end

  puts "Map #{name} size: #{counter}"

  # Close databases
  node_dbs.each do |primary, secondaries|
    unless secondaries.nil? then secondaries.each_value { |db| db.close(0) } end
    unless primary.nil? then primary.close(0) end
  end
end

def generate_map(in_path, out_path, name, domvars, query, bindvars, keys, aps,
                 partition_dims, partition_dim_sizes, node_partitions)
  if keys.all? { |k| k =~ /Q\./ } then
    generate_from_query(out_path, name, query,
      keys, aps, partition_dims, partition_dim_sizes, node_partitions)
  else
    generate_from_domains(in_path, out_path,
      name, domvars, query, bindvars,
      keys, aps, partition_dims, partition_dim_sizes, node_partitions)
  end
end


def repartition(in_path, out_path, name, existing_num_nodes,
                aps, partition_dims, partition_dim_sizes, node_partitions)
  
  # Create BerkeleyDB database, and secondary indexes from aps
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, aps, out_path)

  # Read from all dbs, recomputing partitions.
  existing_dbs = load_db(in_path, name, existing_num_nodes).compact
  existing_dbs.each do |db|
    edb_cursor = db.cursor(nil,0)
    counter = 0

    until (r = edb_cursor.get(nil,nil,Bdb::DB_NEXT)).nil?
      k,v=r
      entry = Marshal.restore(k)
      segment = compute_partition(partition_dims, partition_dim_sizes, entry)
      node_idx = node_partitions.index(segment)
      if node_idx.nil? then
        raise "Error: No valid node partition found for #{segment.join(',').to_s}"
      end

      if $write_nodes.nil? or ($write_nodes.length == 0) or ($write_nodes.include? node_idx) then
        primary, secondaries = node_dbs[node_idx]
        primary.put(nil, k, v, 0)
      end

      counter += 1
      if counter % 1000 == 0 then
        puts "Processed #{counter} tuples"
      end
    end

    edb_cursor.close
  end

  # Close databases
  existing_dbs.each { |db| db.close(0) }

  node_dbs.each do |primary, secondaries|
    unless secondaries.nil? then secondaries.each_value { |db| db.close(0) } end
    unless primary.nil? then primary.close(0) end
  end
end

#####################
#
# Unit test
$test_tables = {
  "a"   => [ "a.tbl" ],
  "b"   => [ "b.tbl" ],
  "c"   => [ "c.tbl" ]
}

$test_input = 
  [["a.0","dom_a"],["b.0","dom_b"],["c.0","dom_c"]]

def gen_test()
  $test_tables.each_pair do |n,f|
    fh = File.open(f[0],"w")
    x = rand(100)
    fh.write((x...2*x).to_a.join("\n")+"\n")
    fh.close()
  end

  in_file = File.open("unit_test", "w")
  in_file.write([
    "<name>",
    $test_input.map { |rp,v| rp+"=>"+v }.join(","),
    "<query>",
    $test_input.map { |rp,v| rp+"=>"+v }.join(","),
    $test_input.map { |rp,v| v }.join(",")
  ].join("\n"))
  in_file.close()
end

def test(name, domvars, query, bindvars)
  domfiles = domvars.map { |r,p,v| [[p,v], File.open($test_tables[r][0], "r") ] }
  values = next_values(domfiles,[])

  while not(finished(domvars.length, domfiles))
    params = []
    values.each_index do |i|
      params.push(domfiles[i][0][1],values[i])
    end

    puts params.flatten.join(",")
    values = next_values(domfiles,values)
  end
end

#gen_test()


#####################
#
# Top level

def print_usage
  puts "Usage: bootstrap.rb <map spec file>"
  puts "Usage: bootstrap.rb [-r|--repartition] <repartition spec> -d <in path> -o <out path>"
  exit
end

def process_bootstrap_spec(spec_lines)
  # Read 6 line blocks
  (0...spec_lines.length).step(6) do |i|
    name, domvar_l, query, bindvar_l, keys_ap_l, partition_l = spec_lines[i,6]

    name = name.strip
    query = query.strip

    if $maps.nil? or ($maps.length == 0) or ($maps.include? name) then

      # Split dom vars, and for each dom var split tables and positions
      domvars = domvar_l.strip.split(",").map do |x|
        rp,v = x.split("=>",2) 
        r,p=rp.split(".")
        [r,p,v]
      end

      # Split bind vars
      bindvars = bindvar_l.strip.split(",")
      
      # Split keys and access patterns
      key_l_ap_opt = keys_ap_l.strip.split("/")
      keys = key_l_ap_opt[0].split(",")
      aps =
        if key_l_ap_opt.length > 1 then
          key_l_ap_opt[1].split("|").map do
          |x| x.split(".").map { |y| y.to_i } end
        else [] end

      pk_l, ds_l = partition_l.strip.split("/")
      partition_dims = pk_l.split(",").collect { |x| x.to_i }
      partition_dim_sizes = ds_l.split(",").collect { |x| x.to_i }
      node_partitions = partition_dim_sizes.collect { |di| (0...di).to_a }.cross_product

      puts "Bootstrapping map #{name}: " +
        " keys: "+keys.join(",") +
        " pd: "+partition_dims.join(",") +
        " pds: "+partition_dim_sizes.join(",") +
        " nn: "+node_partitions.size.to_s

      generate_map($dataset_path, $output_path,
                   name, domvars, query, bindvars,
                   keys, aps, partition_dims, partition_dim_sizes, node_partitions)
      #test(name, domvars, query, bindvars)
    end
  end
end

#
# Spec format: <name>/<existing # nodes>/<dims indexes>/<dim sizes>[/<access patterns>]
# where:
#   dim indexes: indexes of partition keys in the full set of map keys
#   dim sizes: # of partitions for each dimension in dim indexes
#   access partition example: 0|1|2|0,1|1,2
#     specifies 4 access partitions, separated by '|', with dims separated by ','

def process_repartition_spec(rspec)
  name_l, en_l, pk_l,ds_l,ap_l = rspec.strip.split("/")
  if ([ name_l, en_l, pk_l, ds_l ].any? { |x| x.nil? }) then
    print_usage
  end

  name = name_l.to_s

  if $maps.nil? or ($maps.length == 0) or ($maps.include? name) then
    existing_num_nodes = en_l.to_i

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
                name, existing_num_nodes, aps,
                partition_dims, partition_dim_sizes, node_partitions)
  end
end


def main
  unless (not($spec_file.nil?)) ^ (not($repartition_spec.nil?))
    print_usage
  end

  initialize_env;
  unless $spec_file.nil? then
    lines = File.open($spec_file, "r").readlines
    if (lines.length % 6) != 0 then
      puts "Invalid input file, expected 6 lines per map."
      exit
    end
    process_bootstrap_spec(lines)
  else
    lines = File.open($repartition_spec, "r").readlines
    lines.each { |l| process_repartition_spec(l) }
  end
  $env.close
end

opts = GetoptLong.new(
  [ "-f", "--file",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-r", "--repartition",  GetoptLong::REQUIRED_ARGUMENT ],
  [ "-o", "--output",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-d", "--domains",      GetoptLong::REQUIRED_ARGUMENT ],
  [ "-m", "--maps",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-l", "--read-nodes",   GetoptLong::REQUIRED_ARGUMENT ],
  [ "-s", "--write-nodes",  GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt,arg|
  case opt
    when "-f", "--file"         then $spec_file = arg;
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

$spec_file = ARGV[0] if ($spec_file.nil?) and (ARGV.length > 0);
main;

