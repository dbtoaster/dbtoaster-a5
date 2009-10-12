#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';
require 'template';
require 'multikeymap';
require 'versionedmap';

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
    Logger.info {"Fired Notification: " + entry.to_s + " = " + value.to_s }
    @record.discover(entry, value);
  end
end

class RemoteCommitNotification
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
      peer = MapNode::Client.connect(@destination.host, @destination.port);
      peer.push_get(@entries, @cmdid);
      peer.close();
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
    end
  end
  
  def required
    @evaluator.valuation.entries.keys;
  end
end

###################################################

class MapNodeHandler

  def initialize()
    @maps = Hash.new;
    @templates = Hash.new;
    @cmdcallbacks = Hash.new;
  end
  
  ############# Internal Accessors
  
  def find_partition(source, key, &callback)
    raise SpreadException.new("find_partition for wildcard keys requires a callback") if key.include?(-1)&& callback.nil?;
    map = @maps[source.to_i];
    if map == nil then raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.join(",") + "]; Known maps: " + @maps.keys.join(", ")); end
    map.each do |partition|
      if partition.contains? key then 
        if callback then callback.call(partition)
        else return partition 
        end
      end
    end
    raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.to_s + "]") if callback.nil?;
  end
  
  def create_partition(map, region)
    if ! @maps.has_key? map.to_i then @maps[map.to_i] = Array.new; end
    @maps[map.to_i].push(
      MapPartition.new(
        map, 
        region.collect do |r| r[0] end, 
        region.collect do |r| r[1] end
      )
    );
    Logger.debug { "Created partition " + @maps[map.to_i].to_s }
  end
  
  def install_put_template(index, cmd)
    @templates[index.to_i] = cmd;
    Logger.debug {"Loaded Put Template ["+index.to_s+"]: " + @templates[index.to_i].to_s }
  end
  
  def dump()
    @maps.values.collect do |map|
      map.collect do |partition|
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
      raise SpreadException.new("Unknown put template: " + template); 
    end
    
    valuation = TemplateValuation.new(@templates[template], param_list);
    
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
        find_partition(req.source, req.key) do |partition| 
          partition.get(req, CommitNotification.new(record)) 
        end;
      rescue Thrift::Exception => e;
        # this just means that the partition is unavailable.
      end
    end
    
    #initialize the template with values we've already received
    if (@cmdcallbacks[id] != nil) && (@cmdcallbacks[id].is_a? Array) then
      @cmdcallbacks[id].each do |response|
        response.each_pair do |t, v|
          record.discover(t,v);
        end
        record.finish_message;
      end
    end
    
    #register ourselves to receive updates in the future
    @cmdcallbacks[id] = record;
    
    ###### TODO: make record remove itself from @cmdcallbacks when it becomes ready.
  end


  def put(id, template, params)
    Logger.debug {"Put with Params: " + params.to_s }
    valuation = create_valuation(template, params.decipher);
    target = @templates[template].target.instantiate(valuation.params).freeze;
    record = find_partition(target.source, target.key).insert(target, id, valuation);
    install_discovery(id, record, valuation.params);
  end
  
  def mass_put(id, template, expected_gets, params)
    Logger.info "Mass Put with Params: " + params.to_s }
    valuation = create_valuation(template, params.decipher);
    discovery = MassPutDiscoveryMultiplexer.new(valuation, expected_gets);
    find_partition(valuation.target.source, valuation.target.key) do |partition|
      discovery.add_record(partition.mass_insert(id, valuation));
    end
    install_discovery(id, discovery, valuation.params);
  end
  
  def get(target)
    ret = Hash.new()
    target.each do |t|
      ret[t] = find_partition(t.source,t.key).get(t);
    end
    ret;
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
      puts "Error: " + ex.why;
      puts ex.backtrace.join("\n");
    end
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
  end
  
  ############# Internal Control
  
  def setup(input)
    input.each do |line|
      cmd = line.scan(/[^ ]+/);
      case cmd[0]
        when "partition" then 
          match = /Map *([0-9]+)\[([0-9:, ]+)\]/.match(line);
          raise SpreadException.new("Unable to parse partition line: " + line) if match.nil?;
          map, regions = 
            match[1], match[2];
          
          create_partition(
            map.to_i, 
            regions.split(",").collect do |r| 
              r.split("::").collect do |n| 
                n.to_i 
              end 
            end.collect do |r| [r[0], r[1]-r[0]] end
          );
        when "value" then 
          match = /Map *([0-9]+) *\[([^\]]*)\] *v([0-9]+) *= *([0-9.]+)/.match(line)
          raise SpreadException.new("Unable to parse value line: " + line) if match.nil?;
          source, keys, version, value = 
            match[1], match[2], match[3], match[4];
          find_partition(
            source.to_i, 
            keys.split(",").collect do |k| k.to_i end
          ).set(
            keys.split(",").collect do |k| k.to_i end,
            version.to_i, 
            value.to_f
          );
        when "template" then 
          cmd.shift; 
          create_put_template(cmd.shift, cmd.join(" "));
      end
    end
  end
  
end