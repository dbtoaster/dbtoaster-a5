require 'thrift';
require 'map_node';
require 'spread_types';
require 'spatial_index';

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
  attr_reader :maplist;
  
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
      val.each do |pkey|
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
            read_partitions[partition.node_name].push(ep);
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
  
  def compile_trigger(template)
    CompiledTrigger.new(template, self);
  end
end

###################################################

class CompiledMessageSet
  attr_reader :node, :entry, :fetches;
  def initialize(put_target, fetch_targets, put_template, fetch_templates)
    @node = put_target.node_name;
    @entry = put_template
    @fetches = 
      fetch_targets.collect do |node, entries|
        [ node, 
          entries.collect do |e| 
            raise SpreadException.new("Invalid entry index!?! (" + e.entry.index.to_s + ")") unless e.entry.index >= 0 && fetch_templates.size > e.entry.index;
            fetch_templates[e.entry.index]
          end
        ];
      end.collect_hash
  end
  
  def to_s
    @entry.to_s + " @ " + @node.to_s + " ; " + @fetches.size.to_s + " fetches";
  end
end

###################################################

class CompiledTrigger
  attr_reader :template;
  def initialize(template, layout)
    @template = template;
    # I'm going to assume that partition boundaries occur across the entire map.
    # If that's not true, this code will have to change.  It doesn't need to be
    # efficient, but this change makes things infintely easier to think about.
    boundaries =
      template.paramlist.collect do |i|
        # For each parameter in the table to be uploaded, find its domain by 
        # scanning the corresponding components of the target map's partitions.
        layout.maplist[template.target.source].collect do |part|
          # So, for each partition, extract all of the ranges covered by
          # this axis of the partition map.
          part.range[template.target.key.index(i)];
        end.uniq.sort do |a, b|
          # Eliminate duplicates, and then sort over the first index.
          a.begin <=> b.begin;
        end.collect do |r|
          # Further subdivision may be necessary for the fetches.  
          # Identify corresponding ranges in the maps being read from
          subdivisions = template.entries.collect do |e|
            if e.keys.include? i then
              layout.maplist[e.source].collect do |part|
                part.range[e.key.index(i)] if part.range[e.key.index(i)].overlaps? r;
              end
            end
          end.flatten.compact.uniq
          # If there are 0 or 1 corresponding ranges, all the reads occur on the same boundary lines
          # If there are more ranges... we need to subdivide
          if subdivisions.size > 1 then
            subdivisions
          else
            r
          end
        end.flatten
      end
    
    # The result (boundaries) is a list of lists of ranges, the same input that
    # SpatialIndex expects
    @index = SpatialIndex.new(boundaries);
    
    # Now we need to loop over all possible targets in the space to generate
    # gets and puts for the trigger
    @index.update do |key, oldval|
      # Pretend we just got an update for a put to a corner of the partition's keyspace.
      # Generate the parameter mapping for the update
      param_map = template.param_map(key.collect { |k| k.begin })
      
      put_list = Array.new;
      i = -1;
      layout.find_nodes(
        template.target.clone(param_map),
        template.entries.collect { |e| e = e.clone(param_map); e.index = (i += 1); e; }
      ) do |put_target, fetch_targets|
        
        # And save the results
        put_list.push(CompiledMessageSet.new(put_target, fetch_targets, template.target, template.entries));
      end
#      Logger.debug { "Generated Puts: " + put_list.join(", ") + "; for key: " + key.join(", ") }
      put_list;
    end
  end
  
  def each_update(key)
    @index[key].each { |message_set| yield message_set };
  end
  
  def to_s
    @index.collect do |key, msg_sets|
      "ON " + template.relation + "[" + 
        key.collect { |k| k.begin.to_s + "::" + k.end.to_s }.join(", ") + 
      "] :\n" +
      msg_sets.collect { |ms| "   " + ms.to_s }.join("\n");
    end.join("\n");
  end
end