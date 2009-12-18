
require 'thrift_compat';
require 'ok_mixins'
require 'spatial_index'

class MapQuery
  attr_reader :map, :keys;
  
  def initialize(config, map, keys = Array.new)
    @config, @map, @keys = config, map, keys;
    boundaries = @config.partition_ranges(map);
    @index = SpatialIndex.new(boundaries);
    @index.update do |key, value|
      node = @config.node_for_entry(map, key.collect { |range| range.begin });
      raise "No node covering: [" + key.collect { |range| range.begin.to_s + "::" + range.end.to_s }.join(", ") + "]" unless node;
      node;
    end
    @nodecache = Hash.new
  end
  
  
  def query(type, key)
    nodes = Array.new;
    @index.scan(key) do |node|
      nodes.push( [
        @nodecache.assert_key(node) do
          addr = @config.address_for_node(node); 
          MapNode::Client.connect(addr.host, addr.port)
        end,
        node
      ] )
    end
    nodes.collect do |node_info|
        Thread.new(node_info[0], node_info[1]) do |node, name|
          Logger.info { "Dispatching subquery to : " + name }
          start = Time.now;
          Thread.current[:output] = node.aggreget([MapEntry.make(@map, key)], type).values[0].to_f
          Logger.info { "Received subquery response from : " + name + " (" + (Time.now - start).to_s + " sec)" }
        end
    end.collect do |thread|
      thread.join;
      thread[:output];
    end
  end
  
  def sum(key)
    tot = 0;
    query(AggregateType::SUM, key).each { |v| tot += v };
    tot;
  end
  
  def max(key)
    Math.max(*query(AggregateType::MAX, key));
  end
  
  def interpret(line)
    cmd = /(sum) *\(?(([a-zA-Z0-9_]+:[0-9]+)*)\)?/.match(line);
    if cmd then
      params = cmd[2].split(/, */).collect_hash { |kv| kv.split(":") };
      params.each_key { |k| raise "Unknown Key: " + k unless @keys.include? k }
      key = @keys.collect { |k| params.fetch(k, -1) };
      case cmd[1]
        when "sum" then sum(key);
        else raise "Unknown aggregate type: " + cmd[1]
      end.to_s
    else
      puts "Error: Unable to interpret: " + line.to_s unless cmd;
    end
  end
end

