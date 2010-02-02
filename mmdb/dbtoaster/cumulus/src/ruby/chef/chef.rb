
require 'config/template';
require 'chef/maplayout';

###################################################

class ChefNodeHandler
  include Java::org::dbtoaster::cumulus::chef::ChefNode::ChefNodeIFace;

  include CLogMixins;
  self.logger_segment = "Chef";
    
  def initialize(num_templates)
    @next_template = 0;
    @next_cmd = 1;
    @next_update = 1;
    @fetch_count = 0;
    @templates = Hash.new { |h,k| h[k] = Array.new };
    @layout = MapLayout.new;
    @nodelist = nil;
    @update_count = 0;
    @update_timer = nil;
    @start_time = nil;
    @backoff_nodes = Array.new;
    @metacompiled = nil
    @num_templates = num_templates;
    @batch_size = 10;
  end
  
  
  def cmdid
    @next_cmd;
  end
  
  def next_cmd
    @next_cmd += @num_templates + 1;
  end
  
  def next_update
    @start_time = Time.now if @start_time.nil?; 
    if (@next_update % 1000 == 0) then
      rate = @next_update / (Time.now - @start_time)
      info { "Switch: #{@next_update} updates processed, rate: #{rate}, #{@next_cmd} puts, #{@fetch_count} fetches" }
    end
    @next_update += 1;
  end

  def request_backoff(node)
    @backoff_nodes.add(node);
    debug { "Switch: #{node} requesting backoff; backoff set now at #{@backoff_nodes.to_a.join(",")}" };
  end
  
  def finish_backoff(node)
    @backoff_nodes.delete(node);
    debug { "Switch: #{node} no longer needs backoff; backoff set now at #{@backoff_nodes.to_a.join(",")}" }
  end

#  def update(table, params)
#    raise SpreadException.new("Unknown table '"+table.to_s+"'") unless @templates.has_key? table.to_s;
#    raise SpreadException.backoff("Backoff: Nodes #{@backoff_nodes.to_a.join(",")} lagged") unless @backoff_nodes.empty?;
#    params = params.collect { |param| param.to_i }
#    if @metacompiled then 
#      @next_cmd += @metacompiled[table.to_s].fire(params) do |nodeMessage|
#        nodeMessage.dispatch(@nodelist[nodeMessage.node], params, @next_cmd);
#      end
#      next_update;
#    else
#      @templates[table.to_s].each do |trigger|
#        put_nodes = trigger.fire(params) do |fetch_entries, fetch_node, put_node, put_index|
#          @fetch_count += 1;
#          @nodelist[fetch_node].fetch(fetch_entries, put_node, cmdid+put_index);
#        end
#        if trigger.requires_loop?
#          put_nodes.each do |put_node, num_gets|
#            @nodelist[put_node].mass_put(cmdid.to_i, trigger.index.to_i, num_gets.to_i, params);
#            next_cmd;
#          end
#        else
#          raise SpreadException.new("More than one put node (#{put_nodes.size}) for a non-mass put: Template: #{trigger.index}; Entry #{table} => #{params.join(",")}") unless put_nodes.size == 1;
#          @nodelist[put_nodes[0][0]].put(cmdid.to_i, trigger.index.to_i, params);
#          next_cmd;
#        end
#      end
#      next_update
#    end
#  end
  
  def update(table, params)
    breadcrumb = cmdid.to_i
    @nodelist = $config.nodes.values.collect { |node_info| MapNode.getClient(node_info["address"], @batch_size) } unless @nodelist;
    params = params.collect { |param| param.to_f };
    @nodelist.each do |client|
      trace { "Update #{breadcrumb} #{table}(#{params.join(",")}) => #{client.getOutputProtocol.getTransport.getRemote.getHostName}" }
      if client.is_a? MapNode::MapNodeClient then
        client.update(table, params, breadcrumb)
      else
        client.forward_update(table, params, breadcrumb)
      end
    end
    next_cmd;
    next_update;
  end
  
  def forward_update(table, params, basecmd)
    @nodelist = $config.nodes.values.collect { |node_info| MapNode.getClient(node_info["address"], @batch_size) } unless @nodelist;
    params = params.collect { |param| param.to_f };
    @nodelist.each do |client|
      trace { "FUpdate  #{basecmd} #{table}(#{params.join(",")}) => #{client.getOutputProtocol.getTransport.getRemote.getHostName}" }
      if client.is_a? MapNode::MapNodeClient then
        client.update(table, params, basecmd)
      else client.forward_update(table, params, basecmd) end
    end
  end

  def set_forwarders(nodes, map_nodes)
    node_str = nodes.collect { |node_info| node_info.getHostName }.join(",")
    if map_nodes > 0 then
      debug { "Setting forwards to map nodes: #{node_str}" }
    else
      debug { "Setting forwards to switch nodes: #{node_str}" }
    end
    @nodelist = nodes.collect do |node_info|
      if map_nodes > 0 then 
        MapNode.getClient(node_info, @batch_size)
      else
        ChefNode.getClient(node_info, @batch_size)
      end
    end
  end
  
  def query(mapid, params)
    breadcrumb = cmdid.to_i
    @nodelist = $config.nodes.values.collect { |node_info| MapNode.getClient(node_info["address"], @batch_size) } unless @nodelist;
    @nodelist.each do |client|
      trace { "Query #{breadcrumb} Map #{mapid}[#{params.join(",")}] => #{client.getOutputProtocol.getTransport.getRemote.getHostName}" }
      if client.is_a? MapNode::MapNodeClient then
        client.query(mapid, params, breadcrumb)
      else client.forward_query(mapid, params, breadcrumb) end
    end
    next_cmd;
  end
  
  def forward_query(mapid, params, basecmd)
    @nodelist = $config.nodes.values.collect { |node_info| MapNode.getClient(node_info["address"], @batch_size) } unless @nodelist;
    @nodelist.each do |client|
      trace { "FQuery #{basecmd} Map #{mapid}[#{params.join(",")}] => #{client.getOutputProtocol.getTransport.getRemote.getHostName}" }
      if client.is_a? MapNode::MapNodeClient then
        client.query(mapid, params, basecmd)
      else client.forward_query(mapid, params, basecmd) end
    end
  end

  def dump()
    @layout.nodes.collect do |n|
      "\n-----------" + n.to_s + "-----------\n" + @nodelist[n].dump;
    end.join("\n");
  end
  
  def metacompile_templates
    @metacompiled = 
      @templates.to_a.collect_hash do |relation|
        relation, templates = *relation;
        mct = MetaCompiledTrigger.new(relation)
        templates.each { |t| mct.load(t) }
        mct.compile;
        [relation, mct]
      end
    debug { @metacompiled.values.join("\n\n") }
  end
  
  def install_template(template, index = (@next_template += 1))
    template = UpdateTemplate.new(template) if template.is_a? String;
    template.index = index;
    compiled = @layout.compile_trigger(template);
    debug { "Compiled Trigger: \n" + compiled.to_s };
    @templates[template.relation.to_s].push(compiled);
  end
  
  def install_node(node_name, partition_list)
    partition_list.each_pair do |map, partitions|
      partitions.each do |partition|
        @layout.install(map, partition, node_name);
      end
    end
  end
  
  def define_partition(map, sizes)
    @layout.set_partition_size(map, sizes);
  end
end


handler = ChefNodeHandler.new($config.templates.size);
$config.nodes.each_pair do |node, info|
  handler.install_node(info["address"], info["partitions"]);
  CLog.debug { "Identified node " + node.to_s + " at " + info["address"].to_s + "\n" + 
    info["partitions"].collect do |map, partition|
      "  Map " + map.to_s + "[" + partition.join(",") +"]";
    end.join("\n") };
end
$config.partition_sizes.each_pair do |map, sizes|
  handler.define_partition(map, sizes);
end
#$config.templates.each_pair do |id, cmd|
#  handler.install_template(cmd)
#  debug { "Loaded Template " + id.to_s }
#end
#handler.metacompile_templates;

handler;