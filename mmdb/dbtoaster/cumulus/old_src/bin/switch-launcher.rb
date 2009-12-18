
require 'thrift_compat';
require 'node';
require 'switch';
require 'template';
require 'getoptlong';
require 'config';

$stdout.sync = true;

puts "done\nInitializing Switch..."

Logger.default_level = Logger::INFO;
Logger.default_name = "Switch";

port = 52981;
node_addrs = Hash.new

GetoptLong.new(
  [ "-p", "--port",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-n", "--node",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-q", "--quiet",   GetoptLong::NO_ARGUMENT ],
  [ "-v", "--verbose", GetoptLong::NO_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "-p", "--port" then port = arg.to_i;
    when "-n", "--node" then 
      match = /([a-zA-Z0-9]+)@([a-zA-Z\.0-9\-]+)(:[0-9]+)?/.match(arg)
      raise "Invalid node parameter: " + arg unless match;
      node_addrs[match[1]] = NodeID.make(
        match[2],
        if match[3] then match[3].gsub(/^:/, "").to_i else 52982 end
      )
    when "-q", "--quiet" then Logger.default_level = Logger::WARN;
    when "-v", "--verbose" then Logger.default_level = Logger::DEBUG;
  end
end

conf = Config.new;

ARGV.each { |f| conf.load(File.new(f)) };

handler, server = SwitchNode::Processor.listen(port);

conf.nodes.each_pair do |node, info|
  handler.install_node(info["address"], info["partitions"]);
  puts("Identified node " + node.to_s + " at " + info["address"].to_s + "\n" + 
    info["partitions"].collect do |map, partition|
      "  Map " + map.to_s + "[" + partition.join(",") +"]";
    end.join("\n"));
end
conf.partition_sizes.each_pair do |map, sizes|
  handler.define_partition(map, sizes);
end
conf.templates.each_pair do |id, cmd|
  handler.install_template(cmd, id)
#  puts "Loaded Template " + id.to_s;
end
handler.metacompile_templates;

#Thread.current[:main_thread] = 0;
#Thread.new do
#  Thread.current[:monitor_thread] = 0;
#  loop do
#    sleep 10
#    puts(Thread.list.collect do |t|
#      t.inspect + " : " + t.keys.collect { |k| k.to_s }.join(",");
#    end)
#  end
#end

puts "Starting switch server on port " + port.to_s + "..."

server.serve();