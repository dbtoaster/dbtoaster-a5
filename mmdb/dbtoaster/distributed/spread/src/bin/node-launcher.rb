
require 'thrift';
require 'node';
require 'switch';
require 'spread_types_mixins';
require 'ok_mixins';
require 'template';
require 'getoptlong';


puts "done\nInitializing Server..."

$port = 52982;
$name = nil;
$preload = nil;
$preload_shifts = Hash.new;

Logger.default_level = Logger::INFO;
Logger.default_name = "sliceDBread";

GetoptLong.new(
  [ "-p", "--port",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "-n", "--name",    GetoptLong::REQUIRED_ARGUMENT ],
  [ "--preload",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "--shift",         GetoptLong::REQUIRED_ARGUMENT ],
  [ "-q", "--quiet",   GetoptLong::NO_ARGUMENT ],
  [ "-v", "--verbose", GetoptLong::NO_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "-p", "--port" then $port = arg.to_i;
    when "-n", "--name" then $name = arg; Logger.default_name = $name;
    when "-q", "--quiet" then Logger.default_level = Logger::WARN;
    when "-v", "--verbose" then Logger.default_level = Logger::DEBUG;
    when "--preload" then $preload = arg.split(/:/);
    when "--shift"   then arg = arg.split(":"); $preload_shifts[ [arg[0].to_i,arg[1].to_i] ] = arg[2].to_i;
  end
end
cmds = Array.new;
ARGV.each do |opt|
  reading = $name.nil?
  File.open(opt).each do |line|
    cmd = line.chomp.split(/ +/);
    Logger.debug { "GOT " + cmd[0].to_s }
    unless $name.nil? then
      if cmd[0] == "node" then
        Logger.debug { "Found start for node " + cmd[1] }
        reading = (cmd[1] == $name)
        next;
      end
    end
    if reading || (cmd[0] == "template") then
      Logger.debug { " \\____ LINE READ" }
      cmds.push(line);
    end
  end
end

$name = "Solo Node" unless $name;
handler, server = MapNode::Processor.listen($port, $name);
handler.setup(cmds);
puts "Preloading : " + $preload.to_a.join(", ");
handler.preload($preload, $preload_shifts) if $preload;

puts "done\nStarting node " + $name + " server on port " + $port.to_s + "..."

Signal.trap("HUP") { exit }
server.serve();

puts "done\n";