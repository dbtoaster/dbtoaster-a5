require 'thrift';
require 'map_node';
require 'spread_types';
require 'spatial_index';
###################################################

class MapLayout
  # IMPORTANT NOTE: THIS CLASS IS WRITTEN TO ASSUME THAT PARTITIONING IS DONE
  # UNIFORMLY ACROSS ALL MAPS WITH A PARTICULAR "KEY".  IF IN ANY 
  # TEMPLATE/TRIGGER, THE SAME KEY IS USED TO INDEX INTO THE SAME MAP, THE
  # PARTITION BOUNDARIES FOR THAT KEY IN BOTH MAPS HAD BETTER BE THE SAME.

  attr_reader :maplist;
  
  def initialize()
    @maplist = Hash.new { |h,k| h[k] = Hash.new };
    @partition_sizes = Hash.new { |h,k| h[k] = Array.new };
  end
  
  def set_partition_size(map_id, size)
    @partition_sizes[map_id] = size.clone.freeze;
  end
  
  def install(map_id, partition, node)
    @maplist[map_id][partition] = node;
  end
  
  def nodes
    nodes = Set.new;
    @maplist.each_value do |partitions|
      partitions.each_value do |node|
        nodes.add(node);
      end
    end
    nodes.to_a;
  end
  
  def compile_trigger(template)
    compiled_trigger = CompiledTrigger.new(template);
    
    ([template.target].concat(template.entries)).collect do |e|
      partition_node_pairs = @maplist[e.source].to_a;
      partition_node_pairs.collect { |pnp| [e.keys.zip(pnp[0]), pnp[1], e] }
    end.each_cross_product do |candidate_write_and_reads|
      key_partition_map = Hash.new;
      if candidate_write_and_reads.assert do |partition_node|
        # Iterate over all partitions defined by this entry in the candidate.  If there's a key
        # conflict, then the candidate is invalid.  Otherwise, define the key for comparison against
        # later elements.
        partition_node[0].assert do |key_partition|
          key_partition_map.assert_key(key_partition[0]) { key_partition[1] } == key_partition[1];
        end
      end then # candidate_write_and_reads.assert (candidate is self-consistent)
        target = candidate_write_and_reads.shift;
        compiled_trigger.discover_fragment(
          target[1], 
          target[2],
          candidate_write_and_reads.collect { |r| r[1] },
          candidate_write_and_reads.collect { |r| r[2] },
          key_partition_map
        )
      end
    end
    compiled_trigger
  end
end

###################################################

class CompiledTrigger
  def initialize(trigger)
    @trigger = trigger;
    @index = Hash.new { |h,k| h[k] = Hash.new { |ih,ik| ih[ik] = Hash.new { |iih,iik| iih[iik] = Array.new } } }
    @relevant_params = 
      (trigger.entries << trigger.target).collect do |e| 
        e.keys 
      end.flatten.delete_if do |k|
        not trigger.paramlist.include? k;
      end.collect_hash do |k|
        [k, 0, trigger.paramlist.index(k)];
      end
    @partition_size = Array.new(trigger.paramlist.size, 1);
  end
  
  def recompute_partition_size
    @partition_size = @trigger.paramlist.collect { |param| @relevant_params.fetch(param, [nil, 1])[1] };
  end
  
  def discover_fragment(put_node, put_entry, fetch_nodes, fetch_entries, key_partition_map)
    put_map = @index[@trigger.paramlist.collect { |param| 
      if @relevant_params.has_key? param 
      then  @relevant_params[param] = Math.max(key_partition_map[param], @relevant_params[param]); 
            key_partition_map[param] 
      else  0 
      end 
    }][put_node]
    
    fetch_nodes.each_pair(fetch_entries) do |node, entry|
      put_map[node].push(
        [
          entry.source, 
          entry.keys.collect do |k|
            if k.is_a? Number then k
            elsif template.paramlist.include? k then k
            else -1;
            end
          end
        ]
      ) if node != put_node;
    end
    recompute_partition_size;
  end
  
  def fire(params)
    raise SpreadException.new("Wildcard in paramlist: #{params.join(", ")}") unless params.assert { |part| part.to_i >= 0 };
    put_nodes = Array.new;
    @index[Entry.compute_partition(params, @partition_size)].each_pair do |put_node, fetchlist|
      fetchlist.each_pair do |fetch_node, entry_list|
        yield(
          entry_list.collect do |source, keys|
            Entry.make(source, keys.collect { |k| if k.is_a? String then params[@relevant_params[k][2]] else k end })
          end,
          fetch_node,
          put_node,
          put_nodes.size);
      end
      put_nodes.push([put_node, fetchlist.size]);
    end
    put_nodes;
  end
  
  def requires_loop?
    @trigger.requires_loop?;
  end
  
  def index
    @trigger.index
  end
end

