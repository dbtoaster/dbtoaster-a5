
require 'thrift';
require 'node';
require 'switch';
require 'spread_types_mixins';
require 'template';
require 'getoptlong';

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

nodes = Hash.new;
curr_node = nil;
puttemplates = Hash.new;
    
ARGV.collect { |f| File.new(f).readlines }.flatten.each do |line|
  Logger.debug { "GOT: " + line.chomp }
  if line[0] != '#'[0] then 
    cmd = line.scan(/[^ \r\n]+/);
    case cmd[0]
      when "node" then curr_node = nodes.assert_key(cmd[1]){ Array.new }
      when "template" then cmd.shift; puttemplates[cmd.shift.to_i] = UpdateTemplate.new(cmd.join(" "));
      when "partition" then 
        match = /Map *([0-9]+)\[([0-9:, ]+)\]/.match(line);
        raise SpreadException.new("Unable to parse partition line: " + line) if match.nil?;
        curr_node.push(
          [ match[1].to_i,
            match[2].split(/ *, */).collect { |c| c.split(/ *:: */)[0].to_i }.freeze,
            match[2].split(/ *, */).collect { |c| c.split(/ *:: */)[1].to_i }.freeze
          ]);
    end
  end
end

handler, server = SwitchNode::Processor.listen(port);

nodes.each_pair do |id, partitions|
  handler.install_node(node_addrs[id], partitions);
  puts("Identified node " + id.to_s + " at " + node_addrs[id].to_s + " : \n" + 
    partitions.collect do |pinfo|
      "  " + pinfo[0].to_s + "[" + pinfo[1].collect_pair(pinfo[2]){ |s, e| s.to_s + "::" + e.to_s }.join(", ") +"]";
    end.join("\n"));
end
puttemplates.each_pair do |id, cmd|
  handler.install_template(cmd, id)
  puts "Loaded Template " + id.to_s;
end

puts "done\nStarting switch server on port " + port.to_s + "..."

server.serve();

puts "done\n";