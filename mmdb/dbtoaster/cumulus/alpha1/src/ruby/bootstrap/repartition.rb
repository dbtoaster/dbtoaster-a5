##
## Use a simpler repartitioning script, that has no JDBC/ODBC/database connectivity dependencies.

require 'bootstrap_mixins'
require 'fileutils'
require 'getoptlong'

$dataset_path = "/home/yanif/datasets/tpch/100m"
$output_path = "."
$repartition_spec = nil

$quiet = false

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

