
require 'thrift';
require 'node';
require 'switch';
require 'spread_types_mixins';
require 'ok_mixins';
require 'template';
require 'getoptlong';
require 'config';

puts "Initializing Server...\n"

Logger.default_level = Logger::INFO;
Logger.default_name = "sliceDBread";

$config = Config.new;

GetoptLong.new(
  [ "-p", "--port",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-n", "--name",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-q", "--quiet",   GetoptLong::NO_ARGUMENT ],
  [ "-v", "--verbose", GetoptLong::NO_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "-p", "--port" then $config.my_port = arg.to_i;
    when "-n", "--name" then $config.my_name = arg;
    when "-q", "--quiet" then Logger.default_level = Logger::WARN;
    when "-v", "--verbose" then Logger.default_level = Logger::DEBUG;
  end
end

ARGV.each { |cfile| $config.load(File.new(cfile)) };

Logger.default_name = $config.my_name;

handler, server = MapNode::Processor.listen($config.my_port, $config.my_name);
handler.setup($config);

puts "============== Patterns ============";
handler.patterns.each do |map, patterns|
  puts ("====> Map " + map.to_s + " : " +
    patterns.collect { |pat| "("+ pat.join(",") + ")" }.join("; "));
end

puts "done\nStarting node " + $config.my_name + " server on port " + $config.my_port.to_s + "..."

Signal.trap("HUP") { exit }
server.serve();

puts "done\n";