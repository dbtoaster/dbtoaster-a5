require 'thrift';
require 'map_node';
require 'spread_types';

###################################################

class PartitionKey
  attr_reader :range, :node_name
  
  def initialize(low, high, node_name)
    @range = low.collect_pair(high) do |l, h| (l...h) end
    @node_name = node_name;
  end
  
  def contains?(key)
    return false unless key.size == @range.size;
    @range.each_pair(key) do |r, k|
      return false unless (k.is_a? String) || r === k;
    end
    return true;
  end
  
  def <=>(other)
    @range <=> other.range;
  end
  
  def overlaps?(other)
    @range.each_pair(other.range) do |a, b|
      return true unless (a.end < b.begin) || (b.end < a.begin);
    end
    return false;
  end
  
  def to_s
    @range.collect do |a| a.begin.to_s + "::" + a.end.to_s end.join(" ; ") + " @ " + @node_name.to_s
  end
end

###################################################

class EntryPartitions
  attr_reader :entry, :partitions;
  
  def initialize(entry)
    @entry = entry;
    @partitions = Array.new;
  end
end

###################################################

class MapLayout
  def initialize()
    @maplist = Hash.new;
  end
  
  def install(map_id, low, high, node)
    @maplist.assert_key(map_id) { Array.new }.push(PartitionKey.new(low, high, node));
    sort(map_id);
  end
  
  def validate
    @maplist.each_key do |map_id|
      last = nil;
      sort(map_id);
      @maplist[map_id].each do |partition|
        raise SpreadException.new("Overlapping partition boundaries for map : " + map_id.to_s) unless last.nil? || last.overlaps?(partition);
        last = partition;
      end
    end
  end
  
  def nodes
    nodes = Set.new;
    @maplist.each_value do |val|
      @val.each do |pkey|
        nodes.add(pkey.node_name);
      end
    end
    nodes.to_a;
  end
  
  def find_nodes(write, reads)
    (reads.clone << write).each do |entry| raise SpreadException.new("Invalid Entry " + entry.to_s + "; No map with that id") unless @maplist.has_key? entry.source; end;
    
    @maplist[write.source].each do |write_partition|
      Logger.debug { "Checking Partition : " + write_partition.to_s + " for " + write.key.join(", ") } 
      if write_partition.contains? write.key then
        Logger.debug { "Collecting GETS for : " + write_partition.to_s };
      
        loop_ranges = write.key.collect_pair(write_partition.range) do |key, range|
          if key.is_a? String then [key, range] else nil end;
        end.compact.collect_hash do |pair| pair end;
        
        read_partitions = Hash.new
        
        reads.collect do |read_entry|
          sources = EntryPartitions.new(read_entry);
          @maplist[read_entry.source].each do |read_partition|
            unless (read_entry.key.find_pair(read_partition.range) do |key, range|
                case key
                  when String then !(range.overlaps? loop_ranges[key]);
                  else             !(range === key);
                end
              end) then
                sources.partitions.push(read_partition)
            end
          end
          sources;
        end.each do |ep|
          ep.partitions.each do |partition|
            read_partitions.assert_key(partition.node_name) { Array.new }
            read_partitions[partition.node_name].push(ep.entry);
          end
        end
        read_partitions.each_value do |entries| entries.uniq! end;
        read_partitions.delete(write_partition.node_name);
        yield write_partition, read_partitions;
      end
    end
  end
  
  def lookup(entry)
    raise SpreadException.new("Invalid Entry " + entry.to_s + "; No map with that id") unless @maplist.has_key? entry.source;
    @maplist[entry.source].each do |partition|
      return partition if partition.contains? entry.key;
    end
    raise SpreadException.new("Invalid Entry " + entry.to_s + "; No corresponding partition");
  end
  
  def sort(map_id)
    @maplist[map_id].sort!;
    last = nil;
  end
end

