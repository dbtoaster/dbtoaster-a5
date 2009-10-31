
require 'getoptlong';
require 'ok_mixins';
require 'template';
require 'toaster';

$output = STDOUT;
$slicefile = nil;

$toaster = DBToaster.new()
$success = false;
$options = Hash.new;

opts = GetoptLong.new(
  [ "-o", "--output",      GetoptLong::REQUIRED_ARGUMENT ],
  [       "--node",        GetoptLong::REQUIRED_ARGUMENT ],
  [       "--partition",   GetoptLong::REQUIRED_ARGUMENT ],
  [       "--domain",      GetoptLong::REQUIRED_ARGUMENT ],
  [       "--test",        GetoptLong::REQUIRED_ARGUMENT ],
  [ "-s", "--slicefile",   GetoptLong::REQUIRED_ARGUMENT ],
  [       "--slice",       GetoptLong::REQUIRED_ARGUMENT ],
  [       "--key",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-r", "--transforms",  GetoptLong::REQUIRED_ARGUMENT ],
  [       "--persist",     GetoptLong::NO_ARGUMENT ],
  [ "-k", "--ignore-keys", GetoptLong::NO_ARGUMENT ]
).each do |opt, arg| 
  case opt
    when "-o", "--output"      then $output = File.open(arg, "w+"); at_exit { File.delete(arg) unless $toaster.success? && $success };
    when "-k", "--ignore-keys" then $options[:toast_keys] = false;
    when "-s", "--slicefile"   then $slicefile = File.open(arg, "w+"); at_exit { File.delete(arg) unless $toaster.success? && $success };
    else                            $toaster.parse_arg(opt, arg)
  end
end

ARGV.each do |f|
  $toaster.load(File.open(f).readlines);
end

$toaster.toast($options);

raise "Error: --slice requires a --output" if $slicefile && !($output.is_a? File);
$slicefile.write("config " + File.basename($output.path) + "\n") if $slicefile;


puts "=========  Maps  ==========="
$toaster.map_info.each_value do |info|
  puts info["map"].to_s + "(" + info["id"].to_s + ")" unless info["discarded"];
end

$output.write("\n\n############ Put Templates\n");
$toaster.each_template do |i, template|
  $output.write("template " + (i+1).to_s + " " + template.to_s + "\n");
end

#$output.write("\n\n############ Map Information\n");
#$toaster.each_map do |map, info|
#  $output.write("map " + map.to_s + " => Depth " + info["depth"].to_s + ";");
#end

$output.write("\n\n############ Node Definitions\n");
first_node = true;
$toaster.each_node do |node, partitions, address, port|
  $output.write("node " + node.to_s + "\n");
  partitions.each_pair do |map, plist|
    plist.each do |partition|
      $output.write("partition Map " + map.to_s + "[" + 
        partition.collect { |pkey| pkey.begin.to_s + "::" + pkey.end.to_s }.join(",") + "]\n");
    end
  end
  $slicefile.write("node " + node + "@" + address.to_s + ":" + port.to_s + "\n") if $slicefile;
end

$output.write("\n\n############ Test Sequence\n");
$output.write($toaster.test_directives.collect do |l| "update " + l end.join("\n")+"\n");
$output.write("persist\n") if $toaster.persist;

if $slicefile 
  $slicefile.write("switch localhost\n");
  $slicefile.write($toaster.slice_directives.join("\n"));
end

$success = true;
