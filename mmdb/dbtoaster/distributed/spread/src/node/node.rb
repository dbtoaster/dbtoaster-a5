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

class ForeachPutRecord
  attr_reader :version, :lastversion, :template, :next, :prev;
  attr_writer :next, :prev;
  
  def initialize(version, prev, count, template, prereqs)
    @version, @lastversion, @count, @template = version, prev, count, template;
    if prereqs.is_a? Array then
      @prereqs = Set.new;
      prereqs.each do |entry| 
        @prereqs.add(entry)
      end
    else
      @prereqs = prereqs.clone;
    end
    @next, @prev = nil, nil;
  end
  
  def receiveMessage
    @count -- unless @count <= 0;
  end
  
  def fire(entry, value)
    @prereqs.delete(entry)
  end
  
  def find(version)
    if @version >= version then self;
    elsif @next == nil then nil;
    else @next.find(version)
    end
  end
  
  def pending
    (@count > 0) && @prereqs.empty? && (@prev != nil) && (@prev.version != @lastversion) && @prev.pending;
  end
  
  def ready
    !pending;
  end
  
  def add(record)
    if record.version > @version then
      if (@next == nil) || (@next.version < record.version) then
        record.next = @next;
        record.prev = self;
        @next.prev = record;
        @next = record;
      else
        @next.add(record)
      end
      self;
    else
      record.next = self;
      @prev = record;
      record;
    end
  end
  
  def collapse
    @prev.prev = nil unless @prev == nil;
    if ready then
      @next.collapse
    else
      if @prev == nil then self else @prev; end
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
    puts "Created record requiring entries: " + @required.join(", ");
    @callbacks = Array.new;
  end
  
  
  # Find operates in two modes:
  # Under strict mode (lax = false), only the exact version is returned.  If 
  #   the exact version does not exist, we return nil.
  # Under lax mode, we return the most recent version lower than the target
  #   version.
  def find(target, lax=false)
    if @target.version >= target.version then
      if @target.version == target.version then
        self;
      else
        nil; # this only happens if we get a request for a value that has been mass-put but not changed.
      end
    elsif @next && @next.target.version <= target.version then
      @next.find(target, value);
    else
      if lax then self else nil end;
    end
  end
  
  def insert(target, value)
    if @target.version >= target.version then
      if @target.version == target.version then
        @value = value;
        self;
      else
        nil; # this should only happen if we get a request for a value that's been expunged already
      end
    elsif @next && @next.target.version <= target.version then
      @next.find(target, value);
    else
      tmp = @next;
      @next = CommitRecord.new(target, value);
      @next.next = tmp;
      @next;
    end
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
    puts "Discovered: " + entry.to_s + " = " + value.to_s;
    @required.delete_if do |req|
      req.discover(entry, value);
    end
    
    fireCallbacks;
  end
  
  def discoverLocal(handler)
    # To make discoverLocal reentrant with discover, we operate off a copy of @required
    @required.clone.each do |req|
      begin
        # If the value is known, then register will fire immediately.
        # in this case, register will call discover, which will in turn remove
        # the requirement from @required.
        # Note also that discover will fire the callbacks if it is necessary to do so.  
        handler.findPartition(req.maptarget.source, req.maptarget.key).register(req.maptarget, CommitNotification.new(self));
      rescue Thrift::Exception => e;
      end
    end
  end
  
  def fireCallbacks
    return unless ready;
    
    puts "firing " + @value.to_s + "; " + @callbacks.size.to_s + " callbacks";
    @value = @value.to_f;
    @callbacks.each do |cb|
      cb.fire(@target, @value);
    end
    @callbacks = Array.new;
  end
end



###################################################

class MapPartition
  attr_reader :mapid, :start, :range
  
  # A MapPartition is a chunk of a map (ID: mapid) holding keys in the
  # range [start, start+range).  Values are stored versioned; 
  
  def initialize(mapid, start, range)
    @mapid, @start, @range = mapid.to_i, [start].flatten, [range].flatten;
    
    if start.size != range.size then raise SpreadException.new("Creating partition with inconsistent start/range sizes") end;
    @start.freeze;
    @range.freeze;
    
    @data = Hash.new;
    @massputrecords = nil;
    
    @unreceivedputs = Hash.new;
  end
  
  def contains?(key)
    key = [key] unless key.is_a? Array;
    raise SpreadException.new("Trying to determine contains with an inconsistent key size; key:" + key.size.to_s + "; partition: " + @start.size.to_s) unless key.size == @start.size;
    key.each_index do |i|
      if (key[i] < @start[i]) || (key[i] >= @start[i] + @range[i]) then return false end;
    end 
    return true;
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
  
  # Per-map versions also make things a little wonky.  Map versions are
  # treated as barriers.  A map doesn't commit until every independent 
  # update prior to it has committed.  Among other things, this means 
  # that when we do an update, we can find the first map with a higher
  # version number that has been committed and be sure that the highest 
  # known prior version is correct.  Failing that, we need to rely on 
  # the normal versioning scheme.
  def get(target)
    
    prevmassput = if @massputrecords != nil then @massputrecords.find(target.version) else nil end;
    prevok = if prevmassput != nil then prevmassput.ready else nil end;
    
    val = @data[target.key];
    if val == nil then
      # If the object isn't defined, we assume version 0 => 0.
      # on the other hand, if we've got a committed map, then we know the value hasn't been modified.
      if target.version > 0 || !prevok then raise SpreadException.new("Request for an unknown object"); end
      return 0;
    end
    val = val.find(target, prevok);
    
    if val.pending
      raise SpreadException.new("Request for incomplete version");
    end
    
    return val.value.to_f;
  end
  
  
  def register(target, callback)
    # There are several factors of interest
    # 1) Do we know about the object
    # 2) Do we know about the desired version
    # 3) Has the desired version been committed
    # 
    # If the answer to all of these is yes, then we can act immediately and 
    # respond to the callback.  As long as (1) is true, then CommitRecord.find
    # will return a valid record for the specified target version.  Simply,
    # this means that we haven't gotten all of the gets (or the put) for that
    # put.  Even if (1) is false, that just means we need to create the initial
    # record ourselves.
    #
    # Note also that the answer to these questions can be changed by per-map
    # versioning.  Specifically, Is the requested version less than or equal to 
    # one that has already committed?  If so, we are free to use the last known 
    # value for the key in question (starting at val = 0 for version 0).
    #
    # If this is not true, the request is for a put that has not arrived yet.  
    # Unfortunately, we have no way to know at this point whether the put is 
    # going to be an individual, or a map-wide put.  Thus, we defer installing 
    # the callback until we receive the put corresponding  to the particular 
    # version or the next massput.
    
    # We first check to see if a massput exists
    massrecord = if @massputrecords == nil then nil else @massputrecords.find(target.version) end;
    prevok = if massrecord == nil then false else massrecord.ready end;

    #version 0 is always 0.
    if target.version <= 0 then callback.fire(target, 0); return; end;
    
    record = @data[target.key];
    record = record.find(target, prevok) unless record == nil;
    
    if record == nil then
      if prevok then 
        # if we don't have a matching record, but we do have a more recent committed mass put, 
        # then the value has never been modified.
        calback.fire(target, 0); 
        return; 
      else
        # if the corresponding put hasn't arrived yet, we defer installation until it has.
        @unreceivedputs[target.version] = Array.new unless @unreceivedputs.has_key? target.version; 
        @unreceivedputs[target.version].push = [target, callback];
      end
    elsif record.pending then
      record.register(callback);
    else
      callback.fire(target, record.value);
    end
  end
  
  def insert(target, value)
    record = @data[target.key];
    if(record != nil) then
      record.insert(target, value)
    else
      @data[target.key] = CommitRecord.new(target, value);
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
      if partition.contains? key then return partition end;
    end
    raise SpreadException.new("Request for unknown partition: " + source.to_s + "[" + key.to_s + "]");
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
    
    target = target.clone;
    target.version = id;
    target.freeze;
    record = findPartition(target.source, target.key).insert(target, putCommand);
    
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
      cmd = line.scan(/[^ \t]+/);
      case cmd[0]
        when "partition" then createPartition(cmd[1], cmd[2], cmd[3]);
        when "value" then findPartition(cmd[1], cmd[2]).set(cmd[2], cmd[3], cmd[4]);
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
  
  def put(version, template, target, *params)
    convertedParams = 
      params.flatten.collect do |param|
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
            raise SpreadException.new("Unknown parameter type: " + param.class.to_s);
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
