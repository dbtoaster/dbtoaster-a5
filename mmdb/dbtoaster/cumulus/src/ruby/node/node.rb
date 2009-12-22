
require 'config/template';
require 'node/multikeymap';
require 'node/versionedmap';

###################################################

class LogNotification
  def initialize()
    nil
  end
  
  # for the semantics of Hold and Release, see RemoteCommitNotification
  def release
  end
  
  def fire(entry, value)
    puts entry.to_s + " = " + value.to_s;
  end
end

###################################################

class CommitNotification
  def initialize(record)
    @record = record;
  end
  
  # for the semantics of Hold and Release, see RemoteCommitNotification
  def release
  end
  
  def fire(entry, value)
    return if value.nil?;
    Logger.debug {"Fired Notification: " + entry.to_s + " = " + value.to_s }
    @record.discover(entry, value);
  end
end

###################################################

class PluralRemoteCommitNotification
  attr_reader :cmdid, :params, :subqueries
  def initialize(entries, cmdid, params)
    @cmdid, @params = cmdid, params;
    @count = @entries.size;
    @subqueries = Array.new;
  end
  
  def register_subquery(fetch_component)
    @subqueries.push(PluralRemoteCommitSubquery.new(fetch_component, self));
  end
  
  def check_ready
    if @subqueries.assert { |sq| sq.ready } then
      destinations = Hash.new { Hash.new };
      @subqueries.each do |sq|
        sq.load_destinations(destinations)
      end
      destinations.each_pair do |node, entries|
        MapNode.getClient(node).push_get()
      end
    end
  end
end

###################################################

class PluralRemoteCommitSubquery
  attr_reader :requires_loop, :ready;
  
  def initialize(fetch_component, parent)
    @fetch_component, @parent = fetch_component, parent;
    @requires_loop = @fetch_component.entry.requires_loop?;
    @entries = Hash.new;
    @ready = false;
  end
  
  def fire(entry, value)
    @entries[entry] = value;
    unless @requires_loop then
      @ready = true;
      @parent.check_ready;
    end
  end
  
  def release
    @ready = true;
    @parent.check_ready;
  end
  
  def load_destinations(destinations)
    params = parent.params.clone;
    partition_size = $config.partition_sizes[@fetch_component.target.source];
    @entries.each_pair do |entry, value|
      @fetch_component.entry_mapping.each { |mapping| params[mapping[1]] = entry.key[mapping[0]]; } 
      target_partition = @fetch_component.target.partition(params, partition_size);
      @fetch_component.target_partitions.each_pair do |partition, node|
        if partition.zip(target_partition).assert { |partition| (partition[1] == -1) || (partition[1] == partition[0]) } then
          destinations[node][entry] = value;
        end
      end
    end
  end
end

###################################################

class RemoteCommitNotification
  @@nodecache = Hash.new;
  
  def initialize(entries, destination, cmdid)
    @entries, @destination, @cmdid = Hash.new, destination, cmdid;
    entries.each do |e|
      @entries[e] = nil;
    end
    @count = @entries.size;
    @holding = 0;
  end
  
  # Hold and Release are used to deal with multitarget requests where the exact targetlist is unknown
  # After hold is called, the response will not be sent until release is called an equivalent number
  # of times.  If a fetch is multitarget, hold will be called once for every fetch and the corresponding
  # release will be called by the map when the set of results is known (but not necessarilly available)
  def hold
    Logger.debug { "Hold" }
    @holding += 1;
  end
  
  def release
    @holding -= 1;
    Logger.debug { "Release: " + @holding.to_s }
    @count = 0
    @entries.each_value do |v| 
      if v == nil then @count += 1; end; 
    end;
    check_ready;
  end
  
  def fire(entry, value)
    @count -= 1 unless (value == nil) || (!@entries[entry].nil?);
    @entries[entry] = value unless (value == nil) && (@entries.has_key? entry);
    check_ready;
  end
  
  private
  
  def check_ready
    Logger.debug { "RemoteCallback Check Ready: holding = " + @holding.to_s + "; entries left = " + @count.to_s }
    if (@holding <= 0) && (@count <= 0) then
      Logger.debug { "Connecting to " + @destination.to_s }
      peer = MapNode.getClient(@destination);
      peer.push_get(@entries, @cmdid);
      Logger.debug { "push finished" }
      true;
    end
  end
end

###################################################

class MapNodeStats
  attr_reader :name;
  attr_writer :name;
  
  def initialize(name, handler)
    @name, @handler = name, handler;
    @stats = @mass_puts = @puts = @fetches = @pushes = 0;
    @max_push = @push_size = 0;
  end
  
  def stat
    if ((@stats += 1) % 5000) == 0 then
      @handler.sync;
      puts "Status: " + @name + ";" + 
        " put "      + @puts.to_s +
        " mass_put " + @mass_puts.to_s +
        " fetch "    + @fetches.to_s +
        " pushes "   + @pushes.to_s +
        " avg_push " + (@push_size.to_f / @pushes.to_f).to_s +
        " max_push " + @max_push.to_s + 
        " backlog(pending/cleared/server) " + @handler.backlog.to_s + "/" + @handler.cleared_backlog.to_s + "/" + @handler.server_backlog.to_s
    end
  end
  
  def mass_put
    @mass_puts += 1;
    stat;
  end
  
  def put
    @puts += 1;
    stat;
  end
  
  def fetch
    @fetches += 1;
    stat;
  end
  
  def push(size)
    @pushes += 1;
    @push_size += size
    @max_push = size if @max_push < size;
    stat;
  end
end

###################################################

class ValuationApplicator
  attr_reader :valuation, :log;
  
  def initialize(id, target, valuation, handler, log)
    @id, @target, @valuation, @handler, @log = id, target, valuation, handler, log;
    @final_val, @record = nil, nil;
    begin
      @final_val = @valuation.to_f
    rescue SpreadException => e
      # it's not ready
      Logger.debug { e.to_s }
    end
  end
  
  def ready?
    not @final_val.nil?
  end
  
  def discover(key, value)
#    puts "Discovered that : #{key} = #{value}";
    @valuation.discover(key, value)
    begin
      @final_val = @valuation.to_f
      if @record then
#        puts "Map #{valuation.target.source}[#{@target.key.join(",")}] += #{final_val}";
        @record.discover(@target.key, @final_val).finish;
        @handler.finish_valuating(@id);
      end
    rescue SpreadException => e
      # it's not ready
      #puts e.to_s;
    end
  end
  
  def finish_message
  end
  
  def expect_local
  end
  
  def find_local
  end
  
  def apply(partitions)
    if @final_val then
#      puts "Map #{valuation.target.source}[#{@target.key.join(",")}] += #{@final_val}" if @log;
      partitions[0].update(@target.key, @final_val);
      @handler.finish_valuating(@id);
    else
      @record = partitions[0].declare_pending;
    end
  end
end

###################################################

class MassValuationApplicator
  attr_reader :log;
  
  def initialize(id, valuation, expected_gets, handler, log)
    @id, @handler, @log = id, handler, log;
    @expected_gets = expected_gets+1  #One, local get is implicit
    @final_val, @records = nil, nil;
    @expected_locals = 0;
    @evaluator = TemplateForeachEvaluator.new(valuation);
#    puts "Creating insert for #{valuation.target.source}; expecting #{@expected_gets} gets" if @log;
  end
  
  def valuation
    @evaluator.valuation;
  end
  
  def discover(key, value)
#    puts "Discovered that #{key} = #{value}" if @log;
    @evaluator.discover(key, value)
  end
  
  def expect_local
    @expected_locals += 1;
  end
  
  def find_local
    @expected_locals -= 1;
    complete if ready?
  end
  
  def ready?
    (@expected_gets <= 0) && (@expected_locals <= 0);
  end
  
  def finish_message
    @expected_gets -= 1;
    if ready? then
      complete;
      @records = nil;
    end
  end
  
  def apply(partition_list)
    if partition_list.find { |part| part.backlogged? } || (not ready?) then
      @records = partition_list.collect_hash { |part| [part.partition, part.declare_pending] };
      complete if ready?
    else
      partitions = partition_list.collect_hash { |part| [part.partition, part] };
      @evaluator.foreach do |target, delta_value|
#        puts "Map #{valuation.target.source}[#{target.key.join(",")}] += #{delta_value}" if @log;
        partition = target.partition(@handler.partition_sizes[target.source]);
        partitions[partition].update(target.key, delta_value) if partitions.has_key? partition;
      end
      @handler.finish_valuating(@id);
    end
  end
  
  private
  
  def complete
    return unless @records;
    @evaluator.foreach do |target, delta_value|
#      puts "Map #{valuation.target.source}[#{target.join(",")}] += #{delta_value}" if @log;
      partition = target.partition(@handler.partition_sizes[target.source]);
      @records[partition].discover(target.key, delta_value) if @records.has_key? partition;
    end
    @records.each_value { |record| record.finish }
    @handler.finish_valuating(@id);
  end
end

###################################################

class MapNodeHandler
  attr_reader :partition_sizes;
  
  include Java::org::dbtoaster::cumulus::node::MapNode::MapNodeIFace;

  def initialize(name)
    @maps = Hash.new { |h,k| h[k] = Hash.new };
    @templates = Hash.new;
    @cmdcallbacks = Hash.new;
    @stats = MapNodeStats.new(name, self);
    @partition_sizes = Hash.new;
    @log_maps = Array.new;
    @program = CompiledM3Program.new;
  end
  
  ############# Internal Accessors
  
  def sync
    @maps.each_value { |partitions| partitions.each_value { |partition| partition.sync } };
  end
  
  def find_partition(source, key)
    raise SpreadException.new("find_partition for wildcard keys uses loop_partitions") if key.include?(-1);
    partition = NetTypes.compute_partition(key, @partition_sizes[source.to_i]).to_a;
    if (@maps.has_key? source.to_i) && (@maps[source.to_i].has_key? partition) then
      @maps[source.to_i][partition];
    else
      raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + partition.join(",") + "]; Known maps: " + @maps.collect {|k,v| k.to_s+"{"+v.keys.collect { |partid| "[#{partid.join(",")}]" }.join(";") + "}"}.join(", "));
    end
  end
  
  def loop_partitions(source, key)
    key = NetTypes.compute_partition(key, @partition_sizes[source.to_i]);
    @maps[source].each_pair do |map_key, partition|
      if key.zip(map_key).assert { |k, mk| (k == -1) || (k == mk) } then
        yield partition;
      end
    end
  end
  
  def create_partition(map, partition, size, pfiles)
    @partition_sizes[map] = size;
    @maps[map.to_i][partition.collect { |partdim| partdim.to_i }] =
      MapPartition.new(map, partition,  @templates.values.collect do |t|
        t.access_patterns(map.to_i) end.concat!.uniq, pfiles);
    Logger.debug { "Created partition " + @maps[map.to_i].to_s }
  end
  
  def install_put_template(index, cmd)
    @templates[index.to_i] = cmd;
    @maps.each_pair do |map, partition_list| 
      partition_list.each_value do |partition| 
        cmd.access_patterns(map).each do |pat| 
          partition.add_pattern(pat)
        end
      end
    end
    cmd.compile_to_local(
      @program, 
      @maps.collect_hash { |map_partitions| [ map_partitions[0], map_partitions[1].keys ] }
    );
    Logger.debug {"Loaded Put Template ["+index.to_s+"]: " + @templates[index.to_i].to_s }
  end
  
  def dump()
    @maps.keys.sort.collect do |map|
      @maps[map].collect do |key, partition|
        "Partition for Map " + partition.to_s + "\n" + partition.dump;
      end.join "\n"
    end.join "\n";
  end
  
  def localdump()
    puts dump();
  end
  
  ############# Basic Remote Accessors

  def create_valuation(template, param_list)
    if ! @templates.has_key? template then 
      raise SpreadException.new("Unknown put template: " + template.to_s); 
    end
    
    template = @templates[template];
    valuation = TemplateValuation.new(template, template.param_map(param_list));
    
    #valuation.prepare(@templates[template].target.instantiate(version, valuation.params).freeze)
    
    valuation;    
  end
  
  def finish_valuating(id)
    @cmdcallbacks.delete(id);
  end
  
  def preload_locals(id, applicator)
    # initialize the template with values we can obtain locally
    # These are values that either...
    # 1) Exist in a local partition
    # 2) Have already been received thanks to some network hiccup.
    # we might end up changing required inside the loop, so we clone it first.
    
    #initialize the template with values we've already received
    if (@cmdcallbacks[id] != nil) then
      @cmdcallbacks[id].each do |response|
        response.each do |target_value|
          applicator.discover(target_value[0], target_value[1]);
        end
        applicator.finish_message;
      end
    end
    
    @cmdcallbacks.delete(id);
    
    # This is also treated as an implicit message to ourselves.  
    applicator.valuation.entries.keys.each do |req|
#      puts "Checking for #{req}"
      begin
        # If the value is known, then get() will fire immediately.
        # Note also that discover will fire the callbacks if it is necessary to do so.
#        puts "checking for : #{req.source}[#{req.key.to_a.join(",")}]";
        loop_partitions(req.source, req.key) do |partition| 
#          puts "found : " + partition.to_s 
          applicator.expect_local
          partition.get(
            req.key,
            proc { |key, value| applicator.discover(MapEntry.new(req.source, key), value) },
            proc { applicator.find_local }
          )
        end;
      rescue Exception => e;
        # this just means that we don't have the partition for this requirement.
        Logger.debug { e.to_s }
      end
    end
    applicator.finish_message;
    
    #register ourselves to receive updates in the future
    unless applicator.ready? then
      @cmdcallbacks[id] = applicator;
    end
  end


  def put(id, template, params)
    Logger.debug {"Put on template " + template.to_s + " with Params: " + params.to_s }
    valuation = create_valuation(template, params);
    target = @templates[template].target.instantiate(valuation.params).freeze;
    applicator = ValuationApplicator.new(id, target, valuation, self, (@log_maps.include? valuation.target.source));
    preload_locals(id, applicator)
    applicator.apply([find_partition(target.source, target.key)]);
    @stats.put;
  end
  
  def mass_put(id, template, expected_gets, params)
    Logger.debug { "Mass Put (id:" + id.to_s + "; template:" + template.to_s + ") with Params: " + params.to_s }
    valuation = create_valuation(template, params);
    applicator = MassValuationApplicator.new(id, valuation, expected_gets, self, (@log_maps.include? valuation.target.source));
    preload_locals(id, applicator);
    plist = Array.new;
    loop_partitions(valuation.target.source, valuation.target.key) { |partition| plist.push(partition) }
    applicator.apply(plist);
    @stats.mass_put;
  end
  
  def get(target)
    ret = Hash.new()
    target.each do |t|
      raise SpreadException("Multitarget get requests are unsupported; use aggreget()") unless t.has_wildcards?;
      ret[t] = find_partition(t.source,t.key).get(t);
    end
    ret;
  end
  
  def aggreget(target, agg)
    target.collect_hash do |t|
      values = [];
      if t.has_wildcards? then
        loop_partitions(t.source, t.key) do |partition|
          values.concat(partition.get(t));
        end
      else
        values.push(find_partition(t.source, t.key).get(t));
      end
      [t, AggregateType.aggregate(agg, values)]
    end
  end
  
  ############# Asynchronous Reads

  def fetch(target, destination, cmdid)
    begin
      # When we create the notification, we need to exclude all multi-target targets, since
      # those targets will never actually be instantiated.
      request = RemoteCommitNotification.new(
        target.to_a.clone.delete_if do |entry| entry.has_wildcards? end, 
        destination, cmdid
      );
      target.each do |t|
        Logger.debug {"Fetch: " + t.to_s }
        # get() will fire the trigger if the value is already defined
        # the actual message will not be sent until all requests in this 
        # command can be fulfilled.
        if t.has_wildcards? then
          loop_partitions(t.source, t.key) do |partition|
            request.hold;
            partition.get(
              t.key, 
              proc { |key, value| request.fire(MapEntry.new(t.source, key), value) },
              proc { request.release }
            );
          end
        else
          find_partition(t.source, t.key).get(
            t.key, 
            proc { |key, value| request.fire(MapEntry.new(t.source, key), value) },
            nil
          );
        end
      end
    rescue Exception => ex
      puts "Error: " + ex.to_s;
      puts ex.backtrace.join("\n");
    end
    @stats.fetch;
  end
  
  def push_get(result, cmdid)
    Logger.debug {"Pushget: " + result.size.to_s + " results for command " + cmdid.to_s }
    if cmdid == 0 then
      Logger.info {
        "  Fetch Results Pushed: " + 
        result.collect do |entry, val| entry.to_s + " = " + val.to_s end.join(", ")
      }
      return
    end
    
    if @cmdcallbacks[cmdid] == nil then
      # Case 1: we don't know anything about this put.  Save the response for later use.
      @cmdcallbacks[cmdid] = Array.new;
      @cmdcallbacks[cmdid].push(result);
    elsif @cmdcallbacks[cmdid].is_a? Array then
      # Case 2: we haven't received a put message yet, but we have received other fetch results
      @cmdcallbacks[cmdid].push(result);
    else
      # Case 3: we have a push message waiting for these results
      result.each do |target_value|
        @cmdcallbacks[cmdid].discover(target_value[0], target_value[1])
      end
      @cmdcallbacks[cmdid].finish_message;
    end
    @stats.push(result.size);
  end
  
  def meta_request(base_cmd, put_list, get_list, params)
    put_list.each do |put_request|
      if put_request.num_gets >= 0 then
        mass_put(put_request.id_offset+base_cmd, put_request.template, put_request.num_gets, params);
      else
        put(put_request.id_offset+base_cmd, put_request.template, params);
      end
    end
    get_list.each do |get_request|
      fetch(get_request.entries, get_request.target, get_request.id_offset+base_cmd);
    end
  end
  
  def update(relation, params, base_cmd)
    rules = @program.getRelationComponent(relation);
    rules.puts.each do |put_msg|
      if put_msg.condition.match(params) then
        if put_msg.num_gets == -1 then
          valuation = put_msg.template.valuation;
          target = valuation.target;
          applicator = ValuationApplicator.new(
            base_cmd + put_msg.id_offset, target, valuation, self, false
          )
          preload_locals(base_cmd + put_msg.id_offset, applicator);
          @stats.put;
        else
          valuation = put_msg.template.valuation;
          applicator = MassValuationApplicator(
            base_cmd + put_msg.id_offset, valuation, put_msg.num_gets, self, false
          )
          preload_locals(base_cmd + put_msg.id_offset, applicator);
          plist = Array.new;
          loop_partitions(valuation.target.source, valuation.target.key) { |partition| plist.push(partition) }
          applicator.apply(plist);
          @stats.mass_put;
        end
      end
    end
    put_entries = rules.fetches.collect do |fetch_msg|
      if fetch_msg.condition.match(params) then
        [ fetch_msg.condition.
      end
    end
  end
  
  ############# Internal Control
  
  def backlog
    @maps.values.sum { |partitions| partitions.values.sum { |part| part.backlog } };
  end
  
  def cleared_backlog
    @maps.values.sum { |partitions| partitions.values.sum { |part| part.pending_cleared } };
  end
  
  def server_backlog
    if @server then @server.backlog else 0 end;
  end
  
  def setup(config, name)
    puts "Initializing node: #{name}";
    config.my_config["partitions"].each_pair do |map, partition_list|
      partition_list.each do |partition|
        pfiles = config.my_config["pfiles"][map].fetch(partition, nil);
        create_partition(map, partition, config.partition_sizes[map], pfiles)
      end
    end
    
    config.my_config["values"].each_pair do |map, keylist|
      keylist.each_pair do |key, value|
        find_partition(map, key).set(key, value);
      end
    end
    
    config.templates.each_pair do |tid, template|
      install_put_template(tid, template);
    end
    
    @log_maps.concat(config.log_maps);
  end
  
  def partitions  
    ret = Array.new;
    @maps.each_pair do |map, partitions|
      partitions.each_value do |partition|
        ret.push([partition.mapid, partition.partition]);
      end
    end
    ret;
  end
  
  def patterns
    @maps.collect do |map, partitions|
      [ map, partitions.values[0].patterns.keys ]
    end
  end
  
  def designate_server(server)
    @server = server;
  end
  
  def monitor_backlog(switch, local_id, water_marks = (7000...10000))
    return if @backlog_monitor;
    @backlog_monitor = Thread.new(self, switch, water_marks, local_id) do |handler, callback_addr, range, me|
      state = :low
      callback = nil;
      sleep 3;
      loop do
        begin
          backlog = handler.server_backlog
          if state == :low && backlog > range.end then
            puts "Node #{me} requesting backoff";
            callback = ChefNode.getClient(callback_addr) unless callback;
            callback.request_backoff(me);
            state = :high;
          elsif state == :high && backlog < range.begin then
            puts "Node #{me} no longer needs backoff"
            callback = ChefNode.getClient(callback_addr) unless callback;
            callback.finish_backoff(me);
            state = :low
          end
        rescue Exception => e
          puts e.to_s;
          puts e.get_backtrace;
        end
        sleep 1;
      end
    end
  end
  
end

handler = MapNodeHandler.new($config.my_name);
handler.setup($config, $config.my_name);
return handler;