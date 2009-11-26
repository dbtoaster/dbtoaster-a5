#!/usr/bin/env ruby

require 'oci8'
require 'bdb2'

$dataset_path = "/home/yanif/datasets/tpch/100m"
$output_path = "."

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

def initialize_db(name, patterns, basepath = ".", delete_old = true)
  puts "Creating database for map : " + name ;
  env = Bdb::Env.new(0);
  env.cachesize = 128*1024*1024
  env.open(".", Bdb::DB_INIT_CDB | Bdb::DB_INIT_MPOOL | Bdb::DB_CREATE, 0);
  primary_db = env.db;
  if (File.exist? "#{basepath}/db_#{name}_primary.db") && delete_old then
    File.delete("#{basepath}/db_#{name}_primary.db")
  end
  primary_db.open(nil, "#{basepath}/db_#{name}_primary.db", nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
  secondary_dbs = Hash.new;
  db = Array.new;
  i = 0;
  patterns.each do |pattern|
    #puts "#{pattern}"
    db[i] = env.db;
    db[i].flags = Bdb::DB_DUPSORT;
    if (File.exist? "#{basepath}/db_#{name}_#{i}.db") && delete_old then
      File.delete("#{basepath}/db_#{name}_#{i}.db")
    end
    db[i].open(nil, "#{basepath}/db_#{name}_#{i}.db", nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
    primary_db.associate(nil, db[i], 0, proc { |sdb, key, data| Marshal.dump(pattern.map{|k| Marshal.load(key)[k]})});
    secondary_dbs[pattern] = db[i];
    i = i + 1;
  end
  [env, primary_db, secondary_dbs]
end

def generate_map(in_path, out_path, name, domvars, query, bindvars, keys, aps)
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
  env, primary, secondaries = initialize_db(name, aps, out_path)

  counter = 0
  while not(finished(domvars.length, domfiles))
    values.each_index do |i|
      cursor.bind_param(":"+names[i],values[i])
    end
    cursor.exec()
    while r = cursor.fetch()
      entry = interpret_keys(keys,names,values,r)

      # Validate entry
      unless (entry.is_a? Array)
        puts "Error : Param is not an array, param class : #{params.class()}";
      end
      unless (entry.size == keys.size)
        puts "Error : Param of wrong length, param length : #{params.size}";
      end

      # Write out k,v to BDB
      raise "Error: Attempt to set a value for a wildcard key" if entry.include?(-1);
      primary.put(nil, Marshal.dump(entry), r[-1].to_s, 0);

    end
    values = next_values(domfiles,values)

    counter += 1
    if (counter % 10000) == 0 then
      puts "Processed #{counter} keys for #{name}"
    end
  end

  puts "Map #{name} size: #{counter}"

  # Close databases
  secondaries.each_value { |db| db.close(0) }
  primary.close(0)
  env.close
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
# Read 5 line blocks

def process(lines)
  (0...lines.length).step(5) do |i|
    name, domvar_l, query, bindvar_l, keys_ap_l = lines[i,5]

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
        |x| x.split(",").map { |y| y.to_i } end
      else [] end
    
    generate_map($dataset_path, $output_path,
      name.strip, domvars, query.strip, bindvars, keys, aps)
    #test(name, domvars, query, bindvars)
  end
end

def main(f)
  lines = File.open(f, "r").readlines
  if (lines.length % 5) != 0 then
    puts "Invalid input file, expected 5 lines per map."
    exit
  end

  process(lines)
end

if ARGV.length != 1 then
  puts "Usage: bootstrap.rb <map spec file>"
  exit
end

main(ARGV[0])
