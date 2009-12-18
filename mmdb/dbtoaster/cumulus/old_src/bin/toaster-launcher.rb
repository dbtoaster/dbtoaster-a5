
require 'getoptlong';
require 'ok_mixins';
require 'template';
require 'toaster';

$output = STDOUT;
$boot = STDOUT;
$toaster = DBToaster.new()
$success = false;
$options = Hash.new;

# Use home directory as default partition file location, since this is also
# the default for the source directive.
$pfile_basepath = "~"

opts = GetoptLong.new(
  [ "-o", "--output",      GetoptLong::REQUIRED_ARGUMENT ],
  [       "--node",        GetoptLong::REQUIRED_ARGUMENT ],
  [       "--switch",      GetoptLong::REQUIRED_ARGUMENT ],
  [       "--partition",   GetoptLong::REQUIRED_ARGUMENT ],
  [       "--domain",      GetoptLong::REQUIRED_ARGUMENT ],
  [       "--test",        GetoptLong::REQUIRED_ARGUMENT ],
  [       "--slice",       GetoptLong::REQUIRED_ARGUMENT ],
  [       "--key",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-r", "--transforms",  GetoptLong::REQUIRED_ARGUMENT ],
  [       "--persist",     GetoptLong::NO_ARGUMENT ],
  [ "-k", "--ignore-keys", GetoptLong::NO_ARGUMENT ],
  [ "-w", "--switch-addr", GetoptLong::REQUIRED_ARGUMENT ],
  [ "-b", "--boot",        GetoptLong::REQUIRED_ARGUMENT ],
  [ "-p", "--pfile",       GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt, arg| 
  case opt
    when "-o", "--output"      then $output = File.open(arg, "w+"); at_exit { File.delete(arg) unless $toaster.success? && $success };
    when "-k", "--ignore-keys" then $options[:toast_keys] = false;
    when "-b", "--boot"        then $boot = File.open(arg, "w+"); at_exit { File.delete(arg) unless $toaster.success? && $success };
    when "-p", "--pfile"       then $pfile_basepath = arg;
    else                            $toaster.parse_arg(opt, arg)
  end
end

ARGV.each do |f|
  $toaster.load(File.open(f).readlines);
end

$toaster.toast($options);


puts "=========  Maps  ==========="
$toaster.map_info.each_value do |info|
  puts info["map"].to_s + "(" + info["id"].to_s + ")" + " : " + info["num_keys"].to_s + " keys" unless info["discarded"];
end

map_keys = Hash.new;
$output.write("\n\n############ Put Templates\n");
$toaster.each_template do |i, template|
  $output.write("template " + (i+1).to_s + " " + template.to_s + "\n");
  map_keys.assert_key(template.target.source) { template.target.keys };
end

#$output.write("\n\n############ Map Information\n");
#$toaster.each_map do |map, info|
#  $output.write("map " + map.to_s + " => Depth " + info["depth"].to_s + ";");
#end

puts "========== Map definitions ==========="
$toaster.map_info.each_value do |info|
  n = info["map"].to_s
  if $toaster.map_formulae.key?(n) then
    boot_spec = $toaster.map_formulae[n]

    last_node_partition = info["partition"][info["partition"].size-1]
    partition_keys = []
    last_node_partition.each_index do |i|
      if last_node_partition[i] != 0 then partition_keys.push(i) end
    end
  
    partition_sizes = partition_keys.collect do |i|
      info["partition"][info["partition"].size-1][i]+1
    end
  
    #node_partitions = info["partition"].collect do |np|
    #  partition_keys.collect { |i| np[i] }.join(".")
    #end

    boot_spec_s = [ "param_sources", "query", "params", "keys"].collect do |k|
      boot_spec[k]
    end.join("\n")+(boot_spec["aps"].length > 0? ("/"+boot_spec["aps"]) : "");

    p = [ partition_keys.join(","), partition_sizes.join(",") ].join("/")
    map_def = [n, boot_spec_s, p].join("\n")
    
    puts map_def
    $boot.write(map_def+"\n")
  end
end

puts "==== Partition Choices =====";

$toaster.map_info.each_value do |info|
  unless info["discarded"] then
    puts info["map"].to_s + "(" + info["id"].to_s + ")\n" + 
      info["partition"][info["partition"].size-1].collect_index { |i,c| "    " + map_keys[info["id"]][i].to_s + " (" + i.to_s + "): " + (c.to_i + 1).to_s }.join("\n");
  end
end

$output.write("\n\n############ Node Definitions\n");
first_node = true;
$output.write("switch " + $toaster.switch+"\n");

$toaster.each_node do |node, partitions, address, port|
  $output.write("node " + node.to_s + "\n");
  $output.write("address " + address.to_s + ":" + port.to_s + "\n");
  partitions.each_pair do |map, plist|
    plist.each_index do |pidx|
      segment = plist[pidx]
      map_segment = map.to_s + "[" + segment.join(",") + "]"
      map_name = $toaster.map_info[map]["map"].to_s
      node_id = $toaster.map_info[map]["partition"].index(segment)

      $output.write("partition Map " + map_segment + "\n");

      if $toaster.map_formulae.key? map_name then
        primary_pfile = "#{$pfile_basepath}/node#{node_id.to_s}/db_#{map_name}_primary.db"
        aps = $toaster.map_formulae[map_name]["aps"].split("|")
        secondary_pfiles = []
        aps.each_index do |i| secondary_pfiles.push(
          "#{$pfile_basepath}/node#{node_id.to_s}/db_#{map_name}_#{i}.db")
        end
        node_pfiles = [primary_pfile].concat(secondary_pfiles).join(",")
        $output.write("pfile Map " + map_segment + " " + node_pfiles + "\n")
      end
    end
  end
end

$output.write("\n\n############ Test Sequence\n");
$output.write($toaster.test_directives.collect do |l| "update " + l end.join("\n")+"\n");
$output.write("persist\n") if $toaster.persist;

unless $toaster.slice_directives.empty? then
  $output.write("\n\n############ Slicer Debugging Directives\n");
  $output.write($toaster.slice_directives.join("\n") + "\n");
end

$success = true;
