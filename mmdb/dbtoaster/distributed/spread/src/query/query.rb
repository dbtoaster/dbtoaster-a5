
require 'thrift'
require 'spread_types'
require 'map_node'
require 'ok_mixins'
require 'spread_types_mixins'
require 'spatial_index'

class MapQuery
  def initialize(config, map)
    @config, @map = config, map;
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
    ret = Array.new;
    @index.scan(key) do |node|
      ret.push(
        @nodecache.assert_key(node) do
          addr = @config.address_for_node(node); 
          MapNode::Client.connect(addr.host, addr.port)
        end.aggreget([Entry.make(@map, key)], type).values[0].to_f
      )
    end
    ret;
  end
  
  def sum(key)
    tot = 0;
    query(AggregateType::SUM, key).each { |v| tot += v };
    tot;
  end
  
  def max(key)
    Math.max(*query(AggregateType::MAX, key));
  end
end