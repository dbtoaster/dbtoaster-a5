#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';
require 'compiler';
require 'multikeymap';
require 'versionedmap';

###################################################

class CommitNotification
  def initialize(record)
    @record = record;
  end
  
  # for the semantics of Hold and Release, see RemoteCommitNotification
  def hold 
    # we should not be in here.
    raise SpreadException.new("CommitNotification created for a multi-target request");
  end
  
  def fire(entry, value)
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
    @holding = false;
  end
  
  # Hold and Release are used to deal with multitarget requests where the exact targetlist is unknown
  # After hold is called, the response will not be sent until release is called.
  def hold
    @holding = true;
  end
  
  def release
    @holding = false;
    @count = 0
    @entries.each_value do |v| 
      if v == nil then @count += 1; end; 
    end;
    checkReady;
  end
  
  private
  
  def fire(entry, value)
    @count -= 1 unless @holding || (value == nil) || (@entries[entry] != nil);
    @entries[entry] = value;
    checkReady;
  end
  
  def checkReady
    if (!@holding) && (@count <= 0) then
      peer = MapNodeInterface.new(@destination.host, @destination.port);
      peer.pushget(@entries, @cmdid);
      peer.close();
      true;
    end
  end
end

###################################################

class MassPutDiscoveryMultiplexer
  def initialize
    @records = Array.new;
  end
  
  def addRecord(record)
    @records.push(record)
  end
  
  def discover(entry, value)
    @records.each do |record| record.discover(entry, value) end;
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
  
  def findPartition(source, key)
    map = @maps[source.to_i];
    if map == nil then raise SpreadException.new("Request for unknown map"); end
    map.each do |partition|
      if partition.contains? key then return partition end;
    end
    raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.to_s + "]");
  end
  
  def createPartition(map, start, range)
    if ! @maps.has_key? map.to_i then @maps[map.to_i] = Array.new; end
    @maps[map.to_i].push(MapPartition.new(map.to_i, start, range));
    puts ("Created partition " + @maps[map.to_i].to_s);
  end
  
  def installPutTemplate(index, cmd)
    @templates[index.to_i] = cmd;
    puts ("Loaded Put Template ["+index.to_s+"]: " + @templates[index.to_i].to_s );
  end
  
  def dump
    @maps.values.collect do |map|
      map.collect do |partition|
        "partition" + partition.to_s + "\n" + partition.dump;
      end.join "\n"
    end.join "\n"
  end
  
  ############# Basic Remote Accessors

  def createValuation(template, version, paramList)
    if ! @templates.has_key? template then 
      raise SpreadException.new("Unknown put template: " + template); 
    end
    
    valuation = TemplateValuation.new(@templates[template]);
    paramList.each do |param|
      valuation.params[param.name] = 
        case param.type
          when PutFieldType::VALUE then param.value;
          when PutFieldType::ENTRY then param.entry;
        end
    end
    
    valuation.prepare(@templates[template].target.instantiate(version, valuation.params).freeze)
    
    valuation;    
  end
  
  def installDiscovery(id, record)
    #initialize the template with values we can obtain locally
    record.discoverLocal(self);
    
    #initialize the template with values we've already received
    if (@cmdcallbacks[id] != nil) && (@cmdcallbacks[id].is_a? Array) then
      @cmdcallbacks[id].each do |response|
        response.each_pair do |t, v|
          record.discover(t,v);
        end
      end
    end
    
    #register ourselves to receive updates in the future
    @cmdcallbacks[id] = record;
    
    ###### TODO: make record remove itself from @cmdcallbacks when it becomes ready.
  end


  def put(id, template, oldVersion, paramList)
    valuation = createValuation(template, oldVersion, paramList);
    target = @templates[template].target.instantiate(id, valuation.params).freeze;
    record = findPartition(target.source, target.key).insert(target, valuation);
    installDiscovery(id, record);
  end
  
  def massput(id, template, partitions, oldVersion, expectedGets, params, history)
    valuation = createValuation(template, oldVersion, params);
    discovery = MassPutDiscoveryMultiplexer.new;
    partitions.each do |partitionKey|
      record = 
        findPartition(template.target.source, partitionKey).massinsert(id, oldVersion, expectedGets, template, history);
      discovery.addRecord(record)
    end
    installDiscovery(id, discovery);
  end
  
  def get(target)
    ret = Hash.new()
    target.each do |t|
      ret[t] = findPartition(t.source,t.key).get(t);
    end
    ret;
  end
  
  ############# Asynchronous Reads

  def fetch(target, destination, cmdid)
    begin
      # When we create the notification, we need to exclude all multi-target targets, since
      # those targets will never actually be instantiated.
      request = RemoteCommitNotification.new(
        target.clone.delete_if do |entry| entry.key.index(-1) != nil end, 
        destination, cmdid
      );
      target.each do |t|
        puts "Fetch: " + t.source.to_i.to_s + "[" + t.key.to_s + "(" + t.version.to_s + ")]";
        # register will fire the trigger if the value is already defined
        # the actual message will not be sent until all requests in this 
        # command can be fulfilled.
        partition = findPartition(t.source, t.key).register(t, request);
      end
    rescue Thrift::Exception => ex
      puts "Error: " + ex.why;
    end
  end
  
  def pushget(result, cmdid)
    puts "Pushget: " + result.size.to_s + " results for command " + cmdid.to_s;
    if cmdid == 0 then
      puts("  Fetch Results Pushed: " + 
        result.keys.collect do |e| 
          e.source.to_s + "[" + e.key.to_s + "(" + e.version.to_s + ")] = " + result[e].to_s;
        end.join(", "));
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
    end
  end
  
  ############# Internal Control
  
  def setup(input)
    input.each do |line|
      cmd = line.scan(/[^ ]+/);
      case cmd[0]
        when "partition" then createPartition(cmd[1], [cmd[2].to_i], [cmd[3].to_i]);
        when "value" then findPartition(cmd[1], cmd[2]).set(cmd[2], cmd[3], cmd[4]);
        when "massversion" then findPartition(cmd[1], cmd[2]).set
        when "template" then cmd.shift; createPutTemplate(cmd.shift, cmd.join(" "));
      end
    end
  end
  
end

###################################################

class MapNodeInterface
  
  def initialize(host, port=52982)
    @port = port;
    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(host, port))
    @protocol = Thrift::BinaryProtocol.new(@transport)
    @client = MapNode::Client.new(@protocol)
    @getrequest = Array.new;
    @transport.open();
  end
    
  def close
    @transport.close;
  end
  
  def dump
    @client.dump;
  end
  
  def makeEntry(source, key, version, node = nil)
    entry = Entry.new;
    entry.source = source;
    entry.key = key;
    entry.version = version;
    entry;
  end
  
  ############# GET
  
  def queueGet(source, key, version, node = nil)
    @getrequest.push(makeEntry(source, key, version, node));
  end
  
  def issueGet
    if @getrequest.size > 0 then request = @getrequest;
                                 @getrequest = Array.new
                                 @client.get(request);
                            else Hash.new;
    end
  end
  
  def get(source, key, version)
    @getrequest = Array.new
    queueGet(source, key, version);
    issueGet();
  end
  
  ############# PUT
  
  def put(version, template, oldversion, params)
    convertedParams = 
      params.keys.collect do |key|
        field = PutField.new()
        field.name = key
        field.type = 
          case params[key]
            when Numeric, String then 
              field.value = params[key].to_f;
              puts "Value: " + key + " = " + params[key].to_f.to_s;
              PutFieldType::VALUE;
            when Entry then
              field.entry = params[key];
              puts "Entry: " + key + " = " + params[key].to_s;
              PutFieldType::ENTRY;
            else 
              raise SpreadException.new("Unknown parameter type: " + params[key].class.to_s);
          end;
        field;
      end
    @client.put(version.to_i, template.to_i, oldversion.to_i, convertedParams);
  end
  
  ############# Asynch Ops
  
  def pushget(result, cmdid)
    @client.pushget(result, cmdid)
  end
  
  def fetch(target, destination, cmdid)
    @client.fetch(target, destination, cmdid);
  end
  
end