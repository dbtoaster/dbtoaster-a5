#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';
require 'compiler';

###################################################

class CommitNotification
  def initialize(record)
    @record = record;
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
  end
  
  def fire(entry, value)
    @count -= 1 unless (value == nil) || (@entries[entry] != nil);
    @entries[entry] = value;
    if(@count <= 0) then
      peer = MapNodeInterface.new(@destination.host, @destination.port);
      peer.pushget(@entries, @cmdid);
      peer.close();
      true;
    end
  end
end

###################################################

class CommitRecord
  attr_reader :target, :value, :next;
  attr_writer :next;
  
  def initialize(target, value = nil)
    @target, @value = target, value;
    @next = nil;
    # If the value is a MapEquation, we need to evaluate 
    @required = Array.new;
    if @value.is_a? MapEquation then
      begin
        @value = @value.to_f # throws an exception if it fails.
      rescue Thrift::Exception => ex
        #if we're unable to evaluate it straight up, we need to wait.
        @required = @value.entries;
      end
    end
    @callbacks = Array.new;
  end
  
  # Unbeknownst to most users, Find is going to be a bit of a dual-purpose 
  # workhorse.  The algorithm used by find is virtually identical to that used
  # by insert (and in fact, insert would need to be called if we're going to
  # guarantee that find always returns a value); so we kinda-sorta-trick it. 
  # If value=nil, then nothing is modified (unless we're asked to return a 
  # version that doesn't exist yet).  If value != nil, then it will replace
  # the value of the record that matches target.
  def find(target, value=nil)
    if @target.version >= target.version then
      if @target.version == target.version then
        @value = value if value;
        self;
      else
        nil; # this should only happen if we get a request for a value that's been expunged already
      end
    elsif @next && @next.target.version < target.version then
      @next.find(target.version, value);
    else
      insert(target, value);
      tmp = @next;
      @next = CommitRecord.new(target, value);
      @next.next = tmp;
      @next;
    end
  end
  
  def insert(target, value)
    find(target, value);
  end
  
  def to_s
    if (@next == nil) || @next.pending then 
      "value " + 
        @target.source.to_s + " " + 
        @target.key.to_s + " " + 
        @target.version.to_s + " " + 
        @value.to_s;
    else 
      @next.to_s;
    end
  end
  
  def pending
    (@required.size > 0) && (@value != nil)
  end
  
  def ready
    !pending
  end
  
  def register(callback)
    if ready then callback.fire(@target, @value);
    else @callbacks.push(callback);
    end
  end
  
  # Discover is the callback used upon "discovery" of map values that
  # need to be filled in for the unevaluated expression in this record.
  # Discover also takes charge of firing the callbacks waiting for this
  # record to complete.
  def discover(entry, value)
    @required.delete_if do |req|
      req.discover(entry, value)
    end
    if ready then
      @value = @value.to_f
      @callbacks.each do |cb|
        cb.fire(@target, @value);
      end
      @callbacks = Array.new;
    end
  end
end

###################################################

class MapPartition
  attr_reader :mapid, :start, :range
  
  # A MapPartition is a chunk of a map (ID: mapid) holding keys in the
  # range [start, start+range).  Values are stored versioned; 
  
  def initialize(mapid, start, range)
    @mapid, @start, @range = mapid.to_i, start.to_i, range.to_i;
    
    @data = Hash.new;
  end
  
  # The semantics of GET are a little wonky for this map due to 
  # versioning.  Specifically, if the request is for an unknown version,
  # an incomplete version, or a version that has already been discarded,
  # the request will fail.  In theory, we could hold up processing 
  # until the version becomes available (assuming it does happen), but
  # this might tie up the worker thread.  If the data is available when
  # the request comes in, we can return immediately.  If not, then we 
  # register a callback (if we're using asynch get) or fail immediately
  # (otherwise).  
  def get(target)
    val = @data[target.key];
    if val == nil then
      # If the object isn't defined, we assume version 0 => 0.
      if target.version > 0 then raise SpreadException.new("Request for an unknown object"); end
      return 0;
    end
    while val.target.version < target.version
      if val.next == nil
        raise SpreadException.new("Request for unknown version");
      end
      val = val.next;
    end
    
    if val.pending
      raise SpreadException.new("Request for incomplete version");
    end
    
    return val.value.to_f;
  end
  
  def put(cmd)
  end
  
  def register(target, callback)
    #version 0 is always 0.
    if target.version <= 0 then callback.fire(target, 0); return; end;
    
    # There are several possible scenarios:
    # 1) I know about the object
    # 1.1) I know about the desired version
    # 1.1.1) I have committed the desired version
    # 1.1.2) I have not committed the desired version
    # 1.2) I don't know about the desired version
    # 2) I don't know about the object
    # In case 1.1.1, we can act immediately and respond to the callback
    # In all other 1.* cases, CommitRecord.find will return a valid record for
    # the specified target version.  This means we might get a put for a 
    # record already stored in the DB (if we've gotten requests for that record)
    # Case 2 is different from 1.* only in that we have to create the initial
    # record.
    
    record = @data[target.key];
    if record != nil then
      record = record.find(target); #guaranteed to return a value
      if(record.value == nil) then
        record.register(callback);
      else
        callback.fire(target, record.value);
      end
    else
      @data[target.key] = CommitRecord.new(target);
      @data[target.key].register(callback);
    end
  end
  
  def set(var, vers, val)
    target = Entry.new;
      target.source = @mapid;
      target.key = var.to_i;
      target.version = vers.to_i;
    record = CommitRecord.new(target, val.to_f);
    @data[target.key] = record;
  end
  
  def to_s
    "partition " + mapid.to_s + " " + start.to_s + " " + range.to_s + "\n" +
      (@data.values.collect do |entry|
        entry.to_s
      end.join "\n");
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
      if (key.to_i > partition.start.to_i) && (key.to_i - partition.start.to_i < partition.range.to_i) then
        return partition;
      end
    end
    raise SpreadException.new("Request for unknown partition");
  end
  
  def createPartition(map, start, range)
    if ! @maps.has_key? map.to_i then @maps[map.to_i] = Array.new; end
    @maps[map.to_i].push(MapPartition.new(map.to_i, start.to_i, range.to_i));
    puts ("Created partition " + map.to_s + " => [" + start.to_s + ":" + (start.to_i + range.to_i).to_s + "]");
  end
  
  def createPutTemplate(index, cmd)
    @templates[index.to_i] = MapEquation.parse(cmd);
    puts ("Loaded Put Template ["+index.to_s+"]: " + @templates[index.to_i].to_s );
  end
  
  def dump
    @maps.values.collect do |map|
      map.collect do |partition|
        partition.to_s
      end.join "\n"
    end.join "\n"
  end
  
  ############# Basic Remote Accessors

  def put(id, template, target, paramList)
    if ! @templates.has_key? template then 
      raise SpreadException.new("Unknown put template: " + template); 
    end
    
    paramMap = Hash.new;
    paramList.each_index do |i|
      param = paramList[i];
      param.type = case param.type
        when PutFieldType::VALUE then :num;
        when PutFieldType::ENTRY then param.value = param.entry; :map;
        else :num;
      end
      paramMap[i] = param;
    end
    paramMap[-1] = PutField.new;
      paramMap[-1].value = target;
      paramMap[-1].type = :map;
    
    putCommand = @templates[template].parametrize(paramMap);
    puts putCommand.to_s;
    
    record = findPartition(target).insert(target, putCommand);
    
    if (@cmdcallbacks[id] != nil) && (@cmdcallbacks[id].is_a? Array) then
      @cmdcallbacks[id].each do |response|
        response.each_pair do |t, v|
          record.discover(t,v);
        end
      end
    end
    @cmdcallbacks[id] = record;
    ###### TODO: make record remove itself from @cmdcallbacks when it becomes ready.
    
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
      request = RemoteCommitNotification.new(target, destination, cmdid);
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
    puts "Pushget: " + result.to_s;
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
      cmd = line.scan(/[^ \t]+/);
      case cmd[0]
        when "partition" then createPartition(cmd[1], cmd[2], cmd[3]);
        when "value" then findPartition(cmd[1], cmd[2]).set(cmd[2], cmd[3], cmd[4]);
        when "put" then cmd.shift; createPutTemplate(cmd.shift, cmd.join(" "));
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
  
  def put(version, template, target, *params)
    convertedParams = 
      params.collect do |param|
        field = PutField.new()
        field.type = 
          if param.is_a? Numeric then 
            field.value = param.to_f;
            puts "Value: " + param.to_f.to_s;
            PutFieldType::VALUE;
          elsif param.is_a? Entry then
            field.entry = param;
            puts "Entry: " + param.to_s;
            PutFieldType::ENTRY;
          else 
            raise SpreadException.new("Unknown parameter type");
          end;
        field;
      end
    @client.put(version, template, target, convertedParams);
  end
  
  ############# Asynch Ops
  
  def pushget(result, cmdid)
    @client.pushget(result, cmdid)
  end
  
  def fetch(target, destination, cmdid)
    @client.fetch(target, destination, cmdid);
  end
  
end
