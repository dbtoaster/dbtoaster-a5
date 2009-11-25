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
    
    map_refs = ([template.target].concat(template.entries))
    
    partition_fragments = template.paramlist.collect do |param|
      map_refs.collect { |ref| @partition_sizes[ref.source][ref.key.index(param)] if ref.key.include? param }.compact;
    end.collect { |fragment| if fragment.size <= 0 then [1] else fragment.sort.reverse end }
    
    raise SpreadException.new ("Error: template #{template.index}: Largest partition size for a given key must be evenly divisible by all smaller partition sizes") unless partition_fragments.assert { |f| f.assert { |small_f| (f[0] % small_f) == 0 } }
    
    partition_size = partition_fragments.collect { |f| f[0] }
    
    compiled_trigger = CompiledTrigger.new(template, partition_size);
    
    partition_size.collect do |size|
      (0...size).to_a
    end.each_cross_product do |relation_partition|
#      puts "#{template.relation} (#{template.index}) : " + relation_partition.join(",");
      template.target.keys.collect_index do |i, key_dim|
        if template.paramlist.include? key_dim then
          [ relation_partition[template.paramlist.index(key_dim)] % @partition_sizes[template.target.source][i] ]
        else
          (0...(@partition_sizes[template.target.source][i]))
        end
      end.each_cross_product do |put_partition|
        put_node = @maplist[template.target.source][put_partition]
        compiled_trigger.discover_put(relation_partition, put_node)
#        puts "   Requires put: " + put_partition.zip(template.target.keys).collect { |part, key| "#{key}:#{part}" }.join(",") + " @ #{put_node}" ;
        valuations = 
          template.paramlist.zip(relation_partition).concat(
            template.target.keys.zip(put_partition)
          ).collect_hash;
        template.entries.collect do |e|
          e.keys
        end.flatten.uniq.collect do |k|
          if valuations.has_key? k then
            [ [k, valuations[k]] ]
          else
            (0...(map_refs.find { |ref| @partition_sizes[ref.source][ref.key.index(param)] if ref.key.include? param } || 1)).collect do |i|
              [k, i]
            end
          end
        end.each_cross_product do |fetch_partition|
          fetch_valuations = fetch_partition.collect_hash
          template.entries.each do |e|
            key_partition = e.keys.collect_index { |i,k| fetch_valuations[k] % @partition_sizes[e.source][i] }
            fetch_node = @maplist[e.source][key_partition]
#            puts "        Requires fetch: #{e}:[#{key_partition}] @ #{fetch_node}";
            e = Entry.make(
              e.source,
              e.keys.collect do |k| 
                if k.is_a? String 
                  then if template.paramlist.include? k then template.paramlist.index(k) else -1 end
                  else "#{k}"
                end
              end
            )
            compiled_trigger.discover_fetch(relation_partition, put_node, fetch_node, e)
          end
        end
      end
    end
    
    compiled_trigger
  end
end

###################################################

class CompiledTrigger
  def initialize(trigger, partition_size)
    @trigger = trigger;
    @index = Hash.new { |h,k| h[k] = Hash.new { |ih,ik| ih[ik] = Hash.new { |iih,iik| iih[iik] = Array.new } } }
    @relevant_params = 
      (trigger.entries << trigger.target).collect do |e| 
        e.keys 
      end.flatten.delete_if do |k|
        not trigger.paramlist.include? k;
      end.collect_hash do |k|
        [k, [k, 0, trigger.paramlist.index(k)]];
      end
    @partition_size = partition_size
  end
  
  def discover_put(input, put_node)
    @index[input][put_node];
  end
  
  def discover_fetch(input, put_node, fetch_node, fetch_entry)
    @index[input][put_node][fetch_node].push(fetch_entry) unless put_node == fetch_node;
  end
  
  def fire(params)
    raise SpreadException.new("Wildcard in paramlist: #{params.join(", ")}") unless params.assert { |part| part.to_i >= 0 };
    put_nodes = Array.new;
#    puts "Partition: #{Entry.compute_partition(params, @partition_size)} / #{@partition_size.join(",")}"
    @index[Entry.compute_partition(params, @partition_size)].each_pair do |put_node, fetchlist|
      fetchlist.each_pair do |fetch_node, entry_list|
        yield(
          entry_list.collect do |e|
            Entry.make(e.source, e.key.collect { |k| if k.is_a? String then k.to_i elsif k < 0 then k else params[k] end });
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
  
  def to_s
    "(#{@trigger.index}) : ON #{@trigger.relation} [#{@partition_size.join(",")}]\n" +
      @index.collect do |partition, put_list| 
        "  [#{partition.zip(@trigger.paramlist).collect {|part,param| param.to_s+":"+part.to_s }.join(",")}] \n" + 
        put_list.collect do |put_node, fetch_list|
          "      #{put_node} {#{@trigger.target.source}[#{@trigger.target.key.join(",")}]} <= " + 
          fetch_list.collect do |fetch_node, fetch_entries|
            "#{fetch_node} { #{fetch_entries.join(",")} }"
          end.join("; ")
        end.join("\n")
      end.join("\n");
  end
end

