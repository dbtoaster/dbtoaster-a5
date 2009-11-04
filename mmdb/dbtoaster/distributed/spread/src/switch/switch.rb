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

  def update(table, params)
    raise SpreadException.new("Unknown table '"+table.to_s+"'") unless @templates.has_key? table.to_s;
    @templates[table.to_s].each do |trigger|
#      Logger.default.info { "Update triggered template: " + trigger.template.to_s }
      param_map = trigger.template.param_map(params);
      trigger.each_update(params) do |message_set|
#        Logger.default.info { "Generating put command (v" + cmdid.to_s + ") : " + message_set.to_s }
        if trigger.template.requires_loop? then
          #puts "MASS_PUT("+cmdid.to_s+";"+trigger.template.index.to_s+";"+message_set.fetches.size.to_s+";"+params.join("/")+")@"+message_set.node.to_s;
          node(message_set.node).mass_put(cmdid, trigger.template.index, message_set.fetches.size, params)
        else
          #puts "PUT("+cmdid.to_s+";"+trigger.template.index.to_s+";["+params.join("/")+"])@"+message_set.node.to_s;
          node(message_set.node).put(cmdid, trigger.template.index, params)
        end
#        message_set.each_fetch(params) do |dest, entries|
#          node(dest).fetch(entries, message_set.node, cmdid);
#        end
        message_set.fetches.each_pair do |dest, entries|
          #puts "FETCH("+entries.collect { |e| e.instantiate(param_map) }.join("/")+";"+message_set.node.to_s+";"+cmdid.to_s+")@"+dest.to_s;
          node(dest).fetch(
            entries.collect { |e| e.instantiate(param_map) },
            message_set.node,
            cmdid
          )
        end
        next_cmd;
      end
    end
  end
  
  def update_slow(table, params)
    raise SpreadException.new("Unknown table '"+table.to_s+"'") unless @templates.has_key? table.to_s;
    @templates[table.to_s].each do |trigger|
      template = trigger.template;
      Logger.default.info { "Update triggered template: " + template.to_s }
      raise SpreadException.new("Invalid row size (" + params.size.to_s + " and not " + template.paramlist.size.to_s + ") for template: " + template.to_s) unless params.size == template.paramlist.size;
      param_map = template.param_map(params);
      @layout.find_nodes(
        template.target.clone(param_map),
        template.entries.collect do |entry| entry.clone(param_map) end
      ) do |write_partition, read_partitions|
        Logger.default.info { "Generating put command (v" + cmdid.to_s + ") for : Map " + template.target.source.to_s + "[" + write_partition.to_s.gsub(/ @/, "] @") }
        if template.requires_loop? then
          node(write_partition.node_name).mass_put(cmdid, template.index, read_partitions.size, PutParams.make(param_map))
        else
          node(write_partition.node_name).put(cmdid, template.index, PutParams.make(param_map))
        end
        read_partitions.each_pair do |dest, entries|
          Logger.default.info { "Fetching: " + entries.join(", " ) + " from " + dest.to_s }
          node(dest).fetch(
            entries.collect do |e| e.entry.instantiate(param_map) end, 
            write_partition.node_name, 
            cmdid
          );
        end
        next_cmd;
      end
    end
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
    Logger.debug { "Compiled Trigger: \n" + compiled.to_s; }
    @templates.assert_key(template.relation.to_s){ Array.new }.push(compiled);
  end
  
  def install_node(node_name, partitions)
    partitions.each do |partition|
      Logger.default.debug { "Learning of Map " + partition[0].to_s + "[" + partition[1].collect_pair(partition[2]) do |s, e| s.to_s + "::" + e.to_s end.join(" ; ") + "] @ " + node_name.to_s }
      @layout.install(partition[0], partition[1], partition[2], node_name);
    end
  end
end