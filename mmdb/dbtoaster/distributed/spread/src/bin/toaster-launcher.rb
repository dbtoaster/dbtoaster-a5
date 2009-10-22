
require 'getoptlong';
require 'ok_mixins';
require 'template'

$output = STDOUT;
$nodes = Array.new;
$partition_directives = Array.new;
$domain_directives = Array.new;
$test_directives = Array.new;
$persist = false;

def parse_arg(opt, arg)
  case opt
    when "-o", "--output"    then $output = File.open(arg, "w+")
    when "-n", "--node"      then $nodes.push(arg);
    when "-p", "--partition" then $partition_directives.push(arg.split(":"));
    when "-d", "--domain"    then $domain_directives.push(arg.split("="));
    when "-t", "--test"      then $test_directives.push(arg);
    when "-s", "--persist"   then $persist = true;
    else raise "Unknown option: " + opt;
  end
end

opts = GetoptLong.new(
  [ "-o", "--output",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-n", "--node",      GetoptLong::REQUIRED_ARGUMENT ],
  [ "-p", "--partition", GetoptLong::REQUIRED_ARGUMENT ],
  [ "-d", "--domain",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-t", "--test",      GetoptLong::REQUIRED_ARGUMENT ],
  [ "-s", "--persist",   GetoptLong::NO_ARGUMENT ]
).each do |opt, arg| parse_arg(opt, arg) end;

$nodes = ["Alpha", "Beta" ] unless $nodes.size > 0;

local_dir = Dir.getwd()
compiler_dir = File.dirname(__FILE__) + "/../../../../prototype/compiler/alpha3";
Dir.chdir(compiler_dir)

DBT = open("|./dbtoaster.top -noprompt", "w+");
DBT.write("open DBToasterTop;;\n");
DBT.write("compile_sql_to_spread \"" + 
  ARGV.collect { |f| 
    File.readlines(local_dir + "/" + f).collect { |l| 
      if l[0..1] == "--" then l = l.split(" "); parse_arg(l.shift, l.join(" ")); nil
      else l.chomp end;
    }.compact.join(" ") 
  }.join(" ") + 
  "\";;\n");
DBT.close_write();

data = DBT.readlines

# line 1 is the annoying-ass header.  Delete it
compiled = data.drop_front.join("").gsub(/^.*string list[^\[]*\[([^#]*)"\]\n.*/, "\\1").split("\";").collect do |l|
  l.gsub(/^ *\n? *"([^"]*) *\n?/, "\\1\n").gsub(/\\t/, "	").gsub(/\[\]/, "[1]").gsub(/^ *\+/, "");
end

if compiled.size < 2 then
  puts "Error compiling:" + data.join("");
  exit(-1)
end

# 2nd half of the rules are deletion rules.  Kill them for now.
# compiled = compiled.slice(0, compiled.size/2);

templates = compiled.collect do |l|
  puts "Loading template: " + l;
  UpdateTemplate.new(l);
end


$partition_directives = $partition_directives.collect_hash;
$domain_directives = $domain_directives.collect_hash do |e| [e[0], e[1].split(",")] end;

map_info =
  UpdateTemplate.map_names.collect do |map,info|
    domain = $domain_directives.assert_key(map) { Array.new(info["params"], 100000) };
    raise "Domain with invalid dimension.  Map: " + map + "; Expected: " + info["params"].to_s + "; Saw: " + domain.size.to_s unless info["params"].to_i == domain.size;
    
    split_partition = $partition_directives.assert_key(map) { 0 };
    raise "Split Partition too big.  Asked to split on: " + split_partition.to_s + "; Size: " + domain.size.to_s unless (split_partition.to_i < domain.size) || (domain.size == 0);
    
    { "map"       => map, 
      "id"        => info["id"].to_i, 
      "domain"    => domain.collect{|d| d.to_i}, 
      "partition" => split_partition.to_i
    }
  end

$output.write("############ Node Definitions\n");
$nodes.each_index do |node_index|
  node = $nodes[node_index];
  $output.write("node " + node + "\n");
  map_info.each do |map|
    if (map["domain"].size == 0) then
      if node_index == 0 then $output.write("partition Map " + map["id"].to_s + "[0::2]\n") end;
    else
      $output.write("partition Map " + map["id"].to_s + "[" +
        map["domain"].collect_index do |i, d|
          if i == map["partition"] then step = (d / $nodes.size); (step * node_index).to_s + "::" + (step * (node_index+1)).to_s
          else "0::" + d.to_s end;
        end.join(",") + "]\n");
    end
    
  end
end

$output.write("\n\n############ Put Templates\n");
template_id = 1;
templates.each do |l|
  $output.write("template " + template_id.to_s + " " + l.to_s + "\n");
  template_id += 1;
end

$output.write("\n\n############ Test Sequence\n");
$output.write($test_directives.collect do |l| "update " + l end.join("\n")+"\n");
$output.write("persist\n") if $persist;

