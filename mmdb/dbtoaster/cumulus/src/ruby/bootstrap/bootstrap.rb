
require 'java'
require 'rubygems'
require 'fileutils'
require 'getoptlong'
require 'bootstrap_mixins'

$dataset_path = "/home/yanif/datasets/tpch/100m"
$output_path = "."
$spec_file = nil
$repartition_spec = nil

# Oracle JDBC driver
#orcl_driver = Java::JavaClass.for_name("oracle.jdbc.driver.OracleDriver")
#puts "Loaded Oracle driver: #{orcl_driver.java_class}"

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
      result.getLong((k.gsub(/Q\.([0-9]+)/, "\\1").to_i)+1)
    else values[names.index(k)] end
  end
end

def generate_from_domains(in_path, out_path, name, domvars, query, bindvars, keys, aps,
                          partition_dims, partition_dim_sizes, node_partitions)
  domfiles = domvars.map do |r,p,v|
      [[p,v], File.open(File.join(in_path, $tables[r][0]), "r") ]
    end
  names = domfiles.map { |x,f| x[1] }
  values = next_values(domfiles,[])

  # Connect, prepare map query
  url = "jdbc:oracle:thin:@localhost:1521:tpch"
  conn = java.sql.DriverManager.getConnection(url, "cumulus", "slicedbread")
  query_stmt = conn.prepareStatement(query)

  puts "Executing #{query}"

  # Instantiate maps and access patterns
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, keys.size, aps, out_path)

  counter = 0
  while not(finished(domvars.length, domfiles))
    values.each_index { |i| query_stmt.setLong(i+1,values[i]) }
    rs = query_stmt.executeQuery()
    while rs.next() do
      entry = interpret_keys(keys,names,values,rs)
      value = rs.getDouble(keys.size+1)
      write_mk_entry(keys, partition_dims, partition_dim_sizes, node_partitions, node_dbs, entry, value)
      rs.close()
    end

    values = next_values(domfiles,values)

    counter += 1
    if (counter % 10000) == 0 then
      puts "Processed #{counter} keys for #{name}"
    end
  end

  puts "Map #{name} size: #{counter}"

  # Close maps
  node_dbs.each { |mk| mk.close }
  domfiles.each { |x,f| f.close }
end

def generate_from_query(out_path, name, query, keys, aps,
                        partition_dims, partition_dim_sizes, node_partitions)

  # Connect, prepare map query
  url = "jdbc:oracle:thin:@localhost:1521:tpch"
  conn = java.sql.DriverManager.getConnection(url, "cumulus", "slicedbread")
  query_stmt = conn.prepareStatement(query)

  puts "Executing #{query}"

  # Instantiate maps with access patterns
  num_nodes = node_partitions.size
  node_dbs = initialize_db(name, num_nodes, keys.size, aps, out_path)

  counter = 0
  rs = query_stmt.executeQuery()
  while rs.next() do
    entry = keys.map { |k| rs.getLong(k.sub(/Q\./,"").to_i+1) }
    value = rs.getDouble(keys.size+1)
    write_mk_entry(keys, partition_dims, partition_dim_sizes, node_partitions, node_dbs, entry, value)

    counter += 1
    if (counter % 10000) == 0 then
      puts "Processed #{counter} tuples for #{name}"
    end
  end
  rs.close()

  puts "Map #{name} size: #{counter}"

  # Close maps
  node_dbs.each { |mk| mk.close if mk; }
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

def multikeymap_test
  num_keys = 2
  patterns = []
  name = "testmap"
  mk = MultiKeyMap.new(num_keys, patterns, name)
  keys = [[1,1], [2,2]]
  val = 1.0
  keys.each { |k| mk[k] = val; val += 1.0 }
  keys.each { |k| r = mk[k]; puts "Key/val: #{k.join(",")} #{r}" }
  mk.close
end

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
  name_s,nk_s,en_s, pk_l,ds_l,ap_l = rspec.strip.split("/")
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
  unless (not($spec_file.nil?)) ^ (not($repartition_spec.nil?))
    print_usage
  end

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

# Unit test invocation
#gen_test()
#multikeymap_test;
