require 'thrift';
require 'map_node';
require 'spread_types';
require 'template';
require 'multikeymap';
require 'versionedmap';

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
      peer = @@nodecache.assert_key(@destination.to_s) { MapNode::Client.connect(@destination.host, @destination.port) };
      peer.push_get(@entries, @cmdid);
      Logger.debug { "push finished" }
      true;
    end
  end
end

###################################################

class MapNodeStats
  def initialize(name, handler)
    @name, @handler = name, handler;
    @stats = @mass_puts = @puts = @fetches = @pushes = 0;
  end
  
  def stat
    if ((@stats += 1) % 50000) == 0 then
      @handler.sync;
      Logger.warn { "Status: " + @name + ";" + 
        " put "      + @puts.to_s +
        " mass_put " + @mass_puts.to_s +
        " fetch "    + @fetches.to_s +
        " pushes "    + @pushes.to_s +
      "" }
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
  
  def push
    @pushes += 1;
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
    puts "Discovered that : #{key} = #{value}";
    @valuation.discover(key, value)
    begin
      @final_val = @valuation.to_f
      if @record then
        puts "Map #{valuation.target.source}[#{@target.key.join(",")}] += #{final_val}" if @log;
        @record.discover(@target.key, @final_val).finish;
        @handler.finish_valuating(@id);
      end
    rescue SpreadException => e
      # it's not ready
      Logger.debug { e.to_s }
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
      puts "Map #{valuation.target.source}[#{@target.key.join(",")}] += #{@final_val}" if @log;
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
    puts "Creating insert for #{valuation.target.source}; expecting #{@expected_gets} gets" if @log;
  end
  
  def valuation
    @evaluator.valuation;
  end
  
  def discover(key, value)
    puts "Discovered that #{key} = #{value}" if @log;
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
      puts "I have to wait for some data????" if @log
      @records = partition_list.collect_hash { |part| [part.partition, part.declare_pending] };
      complete if ready?
    else
      puts "All data is ready and available! (#{valuation.target.source})" if @log
      partitions = partition_list.collect_hash { |part| [part.partition, part] };
      @evaluator.foreach do |target, delta_value|
        puts "Map #{valuation.target.source}[#{target.key.join(",")}] += #{delta_value}" if @log;
        partitions[target.partition(@handler.partition_sizes[@evaluator.valuation.target.source])].update(target.key, delta_value);
      end
      @handler.finish_valuating(@id);
    end
  end
  
  private
  
  def complete
    return unless @records;
    @evaluator.foreach do |target, delta_value|
      puts "Map #{valuation.target.source}[#{target.join(",")}] += #{delta_value}" if @log;
      @records[target.partition].discover(target, delta_value);
    end
    @records.each_value { |record| record.finish }
    @handler.finish_valuating(@id);
  end
end

###################################################

class MapNodeHandler
  attr_reader :partition_sizes;

  def initialize(name)
    @maps = Hash.new { |h,k| h[k] = Hash.new };
    @templates = Hash.new;
    @cmdcallbacks = Hash.new;
    @stats = MapNodeStats.new(name, self);
    @partition_sizes = Hash.new;
    @log_maps = Set.new;
  end
  
  ############# Internal Accessors
  
  def sync
    @maps.each_value { |partitions| partitions.each_value { |partition| partition.sync } };
  end
  
  def find_partition(source, key)
    raise SpreadException.new("find_partition for wildcard keys uses loop_partitions") if key.include?(-1);
    
    if @maps.has_key? source.to_i then
      @maps[source.to_i][Entry.compute_partition(key, @partition_sizes[source.to_i])];
    else
      raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.join(",") + "]; Known maps: " + @maps.keys.join(", "));
    end
  end
  
  def loop_partitions(source, key)
    key = Entry.compute_partition(key, @partition_sizes[source.to_i]);
    @maps[source].each_pair do |map_key, partition|
      if key.zip(map_key).assert { |k, mk| (k == -1) || (k == mk) } then
        yield partition;
      end
    end
  end
  
  def create_partition(map, partition, size)
    @partition_sizes[map] = size;
    @maps[map.to_i][partition] = MapPartition.new(map, partition, @templates.values.collect { |t| t.access_patterns(map.to_i) }.concat!.uniq);
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
        response.each_pair do |t, v|
          applicator.discover(t,v);
        end
        applicator.finish_message;
      end
    end
    
    @cmdcallbacks.delete(id);
    
    # This is also treated as an implicit message to ourselves.  
    applicator.valuation.entries.keys.each do |req|
      begin
        # If the value is known, then get() will fire immediately.
        # Note also that discover will fire the callbacks if it is necessary to do so.
        Logger.debug { "checking for : " + req.to_s } 
        puts "Checking for #{req}" if applicator.log;
        loop_partitions(req.source, req.key) do |partition| 
          puts "Found partition" if applicator.log;
          Logger.debug { "found : " + partition.to_s + ":" + partition.dump }
          applicator.expect_local
          partition.get(
            req.key,
            proc { |key, value| applicator.discover(Entry.make(req.source, key), value) },
            proc { applicator.find_local }
          )
        end;
      rescue Thrift::Exception => e;
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
        target.clone.delete_if do |entry| entry.has_wildcards? end, 
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
              t, 
              proc { |key, value| request.fire(Entry.make(t.source, key), value) },
              proc { request.release }
            );
          end
        else
          find_partition(t.source, t.key).get(
            t, 
            proc { |key, value| request.fire(Entry.make(t.source, key), value) },
            nil
          );
        end
      end
    rescue Thrift::Exception => ex
      puts "Error: " + ex.to_s;
      puts ex.backtrace.join("\n");
    end
    @stats.fetch;
  end
  
  def push_get(result, cmdid)
    Logger.info {"Pushget: " + result.size.to_s + " results for command " + cmdid.to_s }
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
      result.each_pair do |target, value|
        @cmdcallbacks[cmdid].discover(target, value)
      end
      @cmdcallbacks[cmdid].finish_message;
    end
    @stats.push;
  end
  
  ############# Internal Control
  
  def setup(config)
    config.my_config["partitions"].each_pair do |map, partition_list|
      partition_list.each do |partition|
        create_partition(map, partition, config.partition_sizes[map])
      end
    end
    
    config.my_config["values"].each_pair do |map, keylist|
      keylist.each_pair do |key, value|
        find_partition(map, key).set(key, 0, value);
      end
    end
    
    config.templates.each_pair do |tid, template|
      install_put_template(tid, template);
    end
    
    @log_maps.merge(config.log_maps);
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
  
end