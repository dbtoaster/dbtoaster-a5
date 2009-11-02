
require 'thrift'
require 'spread_types'
require 'map_node'
require 'ok_mixins'
require 'spread_types_mixins'
require 'query'
require 'config'
require 'getoptlong'

config = Config.new;
GetoptLong.new(*Config.opts).each do |opt, arg|
  config.parse_opt(opt, arg);
end

ARGV.each do |file|
  config.load(File.open(file));
end

engine = MapQuery.new(config, 1);

keys = config.map_keys(1).collect { |k| k.downcase };

puts "Aggregate Keys: " + keys.join(", ");

print "> ";
while (line = STDIN.gets) do
  cmd = /(sum) *\(?(([a-zA-Z0-9_]+:[0-9]+)*)\)?/.match(line);
  if cmd then
    params = cmd[2].split(/, */).collect_hash { |kv| kv.split(":") };
    params.each_key { |k| raise "Unknown Key: " + k unless keys.include? k }
    key = keys.collect { |k| params.fetch(k, -1) };
    puts(
      "Result: " + 
      case cmd[1]
        when "sum" then engine.sum(key);
        else raise "Unknown aggregate type: " + cmd[1]
      end.to_s
    )
  else
    puts "Error: Unable to parse: " + line.to_s unless cmd;
  end
  print "> ";
end


#puts(MapNode::Client.connect("localhost").aggreget( [ Entry.make(1, [-1]) ], AggregateType::AVG ).collect { |e, v| e.to_s + " => " + v.to_s }.join("\n"))
