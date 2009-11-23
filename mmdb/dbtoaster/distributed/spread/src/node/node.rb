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

class MassPutDiscoveryMultiplexer
  def initialize(valuation, expected_gets)
    @records = Array.new;
    @expected_gets = expected_gets;
    @evaluator = TemplateForeachEvaluator.new(valuation);
  end
  
  def add_record(record)
    if @expected_gets <= 0 then
      record.complete;
    end
    @records.push(record)
  end
  
  def discover(entry, value)
    Logger.debug {"Discovered that " + entry.to_s + " = " + value.to_s }
    @evaluator.discover(entry, value);
  end
  
  def finish_message
    @expected_gets -= 1;
    if @records && @expected_gets <= 0 then
      @evaluator.foreach do |target, delta_value|
        Logger.debug {"Generated Delta : " + target.to_s + " += " + delta_value.to_s }
        @records.each do |record| record.put(target, delta_value) end;
      end
      @records.each do |r| r.complete end;
      @records = nil;
      return true;
    end
    false;
  end
  
  def required
    @evaluator.valuation.entries.keys;
  end
end

###################################################

class MapNodeStats
  def initialize(name)
    @name = name;
    @stats = @mass_puts = @puts = @fetches = @pushes = 0;
  end
  
  def stat
    if ((@stats += 1) % 50000) == 0 then
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

class MapNodeHandler

  def initialize(name)
    @maps = Hash.new { |h,k| h[k] = Hash.new };
    @templates = Hash.new;
    @cmdcallbacks = Hash.new;
    @stats = MapNodeStats.new(name);
    @partition_sizes = Hash.new;
    @log_maps = Set.new;
  end
  
  ############# Internal Accessors
  
  def find_partition(source, key, &callback)
    raise SpreadException.new("find_partition for wildcard keys requires a callback") if key.include?(-1)&& callback.nil?;
    
    if @maps.has_key? source.to_i then
      @maps[source.to_i][Entry.compute_partition(key, @partition_sizes[source.to_i])];
    else
      raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.join(",") + "]; Known maps: " + @maps.keys.join(", "));
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
  
  def install_discovery(id, record, params)
    # initialize the template with values we can obtain locally
    # we might end up changing required inside the loop, so we clone it first.
        
    # This is treated as an implicit message to ourselves.  
    record.required.clone.each do |req|
      begin
        # If the value is known, then get() will fire immediately.
        # in this case, get() will call discover, which will in turn remove
        # the requirement from @required.
        # Note also that discover will fire the callbacks if it is necessary to do so.
        Logger.debug { "checking for : " + req.to_s } 
        find_partition(req.source, req.key) do |partition| 
          Logger.debug { "found : " + partition.to_s + ":" + partition.dump }
          partition.get(req, CommitNotification.new(record)) 
        end;
      rescue Thrift::Exception => e;
        # this just means that the partition is unavailable.
      end
    end
    
    #initialize the template with values we've already received
    gets_pending = true;
    if (@cmdcallbacks[id] != nil) && (@cmdcallbacks[id].is_a? Array) then
      @cmdcallbacks[id].each do |response|
        response.each_pair do |t, v|
          record.discover(t,v);
        end
        gets_pending = false if record.finish_message;
      end
    end
    
    #register ourselves to receive updates in the future
    if gets_pending then
      @cmdcallbacks[id] = record;
    else
      @cmdcallbacks.delete(id);
    end
  end


  def put(id, template, params)
    Logger.debug {"Put on template " + template.to_s + " with Params: " + params.to_s }
    valuation = create_valuation(template, params);
    target = @templates[template].target.instantiate(valuation.params).freeze;
    record = find_partition(target.source, target.key).insert(target, id, valuation);
    record.register(LogNotification.new) if(@log_maps.include? valuation.target.source);
    install_discovery(id, record, valuation.params);
    @stats.put;
  end
  
  def mass_put(id, template, expected_gets, params)
    Logger.debug { "Mass Put (id:" + id.to_s + "; template:" + template.to_s + ") with Params: " + params.to_s }
    valuation = create_valuation(template, params);
    discovery = MassPutDiscoveryMultiplexer.new(valuation, expected_gets);
    find_partition(valuation.target.source, valuation.target.key) do |partition|
      record = partition.mass_insert(id, valuation)
      if(@log_maps.include? valuation.target.source) then
        record.register(nil, LogNotification.new)
        puts "WORD!"
      end
      discovery.add_record(record);
    end
    install_discovery(id, discovery, valuation.params);
    # If we aren't waiting for anything, we need to trick the discovery multiplexer into firing a commit.
    discovery.finish_message if expected_gets <= 0;
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
        find_partition(t.source, t.key) do |partition|
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
          find_partition(t.source, t.key) do |partition|
            request.hold;
            partition.get(t, request);
          end
        else
          find_partition(t.source, t.key).get(t, request);
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
      @cmdcallbacks.delete(cmdid) if @cmdcallbacks[cmdid].finish_message;
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
  
  def preload(input_files, shifts = Hash.new)
    cnt = 0;
    in_tables = Hash.new;
    GC.disable
    @maps.each_pair do |map, partitions|
      Logger.warn { "Preloading : Map " + map.to_s + " <-- " + input_files[map.to_i-1]; }
      input = File.open(input_files[map.to_i-1]);
      input.each do |line|
        partitions.values[0].set(
          line.split(/, */).collect_index do |i,k|
            k.to_i + shifts.fetch([map, i], 0)
          end, 0, 1
        );
      end
    end
    GC.enable;
    ObjectSpace.garbage_collect;
    Logger.warn { "Preload complete; Sleeping for 1 min to allow garbage collector to run" };
    sleep 120;
    Logger.warn { "Sleep complete" };
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