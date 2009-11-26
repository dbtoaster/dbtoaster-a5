require 'thrift';
require 'map_node';
require 'spread_types';
require 'template';
require 'maplayout';

###################################################

class SwitchNodeHandler
  def initialize
    @next_template = 0;
    @next_cmd = 1;
    @next_update = 1;
    @fetch_count = 0;
    @templates = Hash.new;
    @layout = MapLayout.new;
    @nodelist = Hash.new;
    @update_count = 0;
    @update_timer = nil;
  end
  
  def node(name)
#    RubyProf.pause
    ret = 
      @nodelist.assert_key(name) do
  #      Logger.default.info { "Establishing connection to : " + name.to_s; }
        MapNode::Client.connect(name.host, name.port); 
      end;
#    RubyProf.resume
    ret
  end
  
  def cmdid
    @next_cmd;
  end
  
  def next_cmd
    @next_cmd += 1;
  end
  
  def next_update
    puts "Switch: #{@next_update} updates processed; #{@next_cmd} puts, #{@fetch_count} fetches" if (@next_update % 1000 == 0) 
    @next_update += 1;
  end

  def update(table, params)
    raise SpreadException.new("Unknown table '"+table.to_s+"'") unless @templates.has_key? table.to_s;
    params.collect! { |param| param.to_i }
    @templates[table.to_s].each do |trigger|
      put_nodes = trigger.fire(params) do |fetch_entries, fetch_node, put_node, put_index|
        @fetch_count += 1;
        node(fetch_node).fetch(fetch_entries, put_node, cmdid+put_index);
      end
      if trigger.requires_loop?
        put_nodes.each do |put_node, num_gets|
          node(put_node).mass_put(cmdid.to_i, trigger.index.to_i, num_gets.to_i, params);
          next_cmd;
        end
      else
        raise SpreadException.new("More than one put node (#{put_nodes.size}) for a non-mass put: Template: #{trigger.index}; Entry #{table} => #{params.join(",")}") unless put_nodes.size == 1;
        node(put_nodes[0][0]).put(cmdid.to_i, trigger.index.to_i, params);
        next_cmd;
      end
    end
    next_update
  end
  
  def dump()
    @layout.nodes.collect do |n|
      "\n-----------" + n.to_s + "-----------\n" + node(n).dump;
    end.join("\n");
  end
  
  def install_template(template, index = (@next_template += 1))
    template = UpdateTemplate.new(template) if template.is_a? String;
    Logger.debug { "Loading Template " + index.to_s + ": " + template.summary; }
    template.index = index;
    compiled = @layout.compile_trigger(template);
    puts "Compiled Trigger: \n" + compiled.to_s;
    @templates.assert_key(template.relation.to_s){ Array.new }.push(compiled);
  end
  
  def install_node(node_name, partition_list)
    partition_list.each_pair do |map, partitions|
      partitions.each do |partition|
        Logger.default.debug { "Learning of Map " + map.to_s + "[" + partition.join(", ") + "] @ " + node_name.to_s }
        @layout.install(map, partition, node_name);
      end
    end
  end
  
  def define_partition(map, sizes)
    @layout.set_partition_size(map, sizes);
  end
end