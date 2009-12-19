
require 'util/spatial_index';

###################################################

class MapLayout
  # IMPORTANT NOTE: THIS CLASS IS WRITTEN TO ASSUME THAT PARTITIONING IS DONE
  # DEMI-UNIFORMLY ACROSS ALL MAPS WITH A PARTICULAR "KEY".  IF IN ANY 
  # TEMPLATE/TRIGGER, THE SAME KEY IS USED TO INDEX INTO TWO MAPS, THE BIGGER
  # PARTITION BOUNDARY FOR THAT KEY HAD BETTER DIVISIBLE BY THE SMALLER.

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
    nodes = Array.new;
    @maplist.each_value do |partitions|
      partitions.each_value do |node|
        nodes.push(node);
      end
    end
    nodes.uniq;
  end
  
  def compile_trigger(template)
    
    map_refs = ([template.target].concat(template.entries))
    
    partition_fragments = template.paramlist.collect do |param|
      map_refs.collect { |ref| @partition_sizes[ref.source][ref.key.index(param)] if ref.key.include? param }.compact;
    end.collect { |fragment| if fragment.size <= 0 then [1] else fragment.sort.reverse end }
    
    raise SpreadException.new("Error: template #{template.index}: Largest partition size for a given key must be evenly divisible by all smaller partition sizes") unless partition_fragments.assert { |f| f.assert { |small_f| (f[0] % small_f) == 0 } }
    
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
            (0...(map_refs.find { |ref| @partition_sizes[ref.source][ref.key.index(k)] if ref.key.include? k } || 1)).collect do |i|
              [k, i]
            end
          end
        end.each_cross_product do |fetch_partition|
          fetch_valuations = fetch_partition.collect_hash
          template.entries.each do |e|
            key_partition = e.keys.collect_index { |i,k| fetch_valuations[k] % @partition_sizes[e.source][i] }
            fetch_node = @maplist[e.source][key_partition]
#            puts "        Requires fetch: #{e}:[#{key_partition}] @ #{fetch_node}";
            e = MapEntry.new(
              e.source,
              e.keys.collect do |k| 
                if k.is_a? String 
                  then if template.paramlist.include? k then "#{template.paramlist.index(k)}" else -1 end
                  else k.to_i
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
  attr_reader :trigger, :partition_size;

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
  
  def relation
    trigger.relation;
  end
  
  def message_index
    @index;
  end
  
  def discover_put(input, put_node)
    @index[input][put_node];
  end
  
  def discover_fetch(input, put_node, fetch_node, fetch_entry)
    unless put_node == fetch_node
      @index[input][put_node][fetch_node].push(fetch_entry) unless @index[input][put_node][fetch_node].include? fetch_entry;
    end
  end
  
  def fire(params)
    raise SpreadException.new("Wildcard in paramlist: #{params.join(", ")}") unless params.assert { |part| part.to_i >= 0 };
    put_nodes = Array.new;
#    puts "Partition: #{NetTypes.compute_partition(params.to_java(:Long), @partition_size.to_java(:Long))} / #{@partition_size.join(",")}"
    @index[NetTypes.compute_partition(params.to_java(:Long), @partition_size.to_java(:Long)).to_a].each_pair do |put_node, fetchlist|
      fetchlist.each_pair do |fetch_node, entry_list|
        yield(
          entry_list.collect do |e|
            MapEntry.new(e.source, e.key.collect { |k| if k.is_a? String then params[k.to_i] elsif k < 0 then k else params[k] end });
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
            "#{fetch_node} { #{fetch_entries.to_a.join(",")} }"
          end.join("; ")
        end.join("\n")
      end.join("\n");
  end
end

###################################################

class MetaCompiledNodeMessage
  attr_reader :node, :puts, :fetches;
  
  def initialize(node)
    @node = node;

    # List of [TemplateID, id_offset, Expected Gets]
    @puts = Array.new;
    
    # Map of TargetNode -> [id_offset, List of MapEntry]
    @fetches = Hash.new { |h,k| h[k] = Array.new }; 
  end
  
  def compile
    @put_messages = @puts.collect { |params| PutRequest.new(params[0].to_i, params[1].to_i, params[2].to_i); }
    @fetch_messages = @fetches.collect do |target, get_sets| 
      get_sets.collect do |get_set|
        GetRequest.new(target, get_set[0], get_set[1].to_a.clone);
      end
    end.concat!;
    @replacements = @fetch_messages.collect_index do |i, get| 
      get.replacements.to_a.collect { |params| params.to_a.unshift(i) }
    end.concat!
  end
  
  def dispatch(node_handler, params, base_cmd_id)
    @replacements.each do |request, entry, key_dimension, param| 
      @fetch_messages[request].entries[entry].key[key_dimension] = params[param];
    end
    node_handler.meta_request(base_cmd_id, @put_messages, @fetch_messages, params);
  end
  
  def to_s(line_leader = "")
    "#{line_leader}[#{node}] <= Templates #{@puts.collect { |pu| pu[0] }.uniq.join(",")}" +
      @fetches.collect do |target, entry_offset_list|
        "\n#{line_leader}  [#{target}] <= [#{node}] : " + 
        entry_offset_list.collect do |id_offset, entry_list|
          entry_list.to_a.join(", ");
        end.join(", ")
      end.join("");
  end
end

###################################################

class MetaCompiledTrigger
  def initialize(relation)
    @relation = relation;
    @cmd_size = Hash.new(0);
    @partition_size = nil;
    initialize_nodes;
  end
  
  def initialize_nodes
    old_nodes = @nodes;
    @nodes = Hash.new { |h,k| h[k] = Hash.new { |ih,ik| ih[ik] = MetaCompiledNodeMessage.new(ik) } }
    old_nodes
  end
  
  def load(trigger)
    raise "Invalid trigger; Expected relation #{@relation}, but got trigger for relation #{trigger.relation}" unless trigger.relation == @relation;
    
    if @partition_size then
      rescaling = @partition_size.zip(trigger.partition_size).collect { |a, b| if a < b then b.to_f / a.to_f else 1 end }
      raise "Conflicting partition sizes #{@partition_size.join(",")} <= #{trigger.partition_size.join(",")}" unless rescaling.assert { |a| a.to_i.to_f == a };
      rescaling.each_with_index do |r,i|
        old_keys = @nodes.keys
        (1...r.to_i).each do |mult|
          old_keys.each do |key|
            new_key = key.clone;
            new_key[i] = key[i] + mult.to_i * @partition_size[i];  #### THIS LINE ASSUMES MODULUS PARTITIONING
            @nodes[new_key] = @nodes[key].clone;
          end
        end
      end
      @partition_size = @partition_size.zip(rescaling).collect { |old, mult| old * mult.to_i }
    else
      @partition_size = trigger.partition_size;
    end
    
    rescaling = @partition_size.zip(trigger.partition_size).collect { |a, b| if b < a then a.to_f / b.to_f else 1 end }
    raise "Conflicting partition sizes #{@partition_size.join(",")} <= #{trigger.partition_size.join(",")}" unless rescaling.assert { |a| a.to_i.to_f == a };

    rescaling.collect { |a| (0...a.to_i).to_a }.each_cross_product do |pshift|
      trigger.message_index.each_pair do |partition, put_list|
        partition = partition.clone.zip(pshift, trigger.partition_size).collect do |base, shift, size|
          base + shift * size;
        end
        put_list.each_pair do |put_node, fetch_list|
          @nodes[partition][put_node].puts.push([trigger.index, @cmd_size[partition], trigger.requires_loop? ? fetch_list.size : -1]);
          fetch_list.each_pair do |fetch_node, fetch_entries|
            raise "Nil fetch_entries" unless fetch_entries;
            @nodes[partition][fetch_node].fetches[put_node].push([@cmd_size[partition], fetch_entries])
          end
          @cmd_size[partition] = @cmd_size[partition] + 1;
        end
      end
    end
  end
  
  def compile
    @nodes.each_value do |partition_nodes|
      partition_nodes.each_value do |nodeMessage|
        nodeMessage.compile;
      end
    end
  end
  
  def fire(params)
    partition = NetTypes.compute_partition(params.to_java(:Long), @partition_size.to_java(:Long)).to_a;
    @nodes[partition].each_value do |nodeMessage|
      yield nodeMessage;
    end
    @cmd_size[partition]
  end
  
  def to_s
    @nodes.keys.sort.collect do |partition|
      nodes = @nodes[partition];
      "#{@relation}[#{partition.join(", ")}]" + 
      nodes.values.collect { |n| "\n" + n.to_s("   ") }.join("");
    end.join("\n")
  end
end
