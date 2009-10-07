
require 'thrift';
require 'map_node';
require 'spread_types';
require 'compiler';
require 'multikeymap';

###################################################

class ForeachPutRecord
  attr_reader :version, :lastversion, :template, :next, :prev, :partition;
  attr_writer :next, :prev;
  
  def initialize(version, lastversion, expectedgets, template, prereqs, partition)
    @version, @lastversion, @expectedgets, @template, @partition = version, lastversion, expectedgets, template, partition;
    if prereqs.is_a? Array then
      @prereqs = Set.new;
      prereqs.each do |entry| 
        @prereqs.add(entry)
      end
    else
      @prereqs = prereqs.clone;
    end
    @next, @prev = nil, nil;
    @callbacks = Hash.new;
  end
  
  def register(target, value)
    @callbacks[target] = value;
  end
  
  def receiveMessage
    @expectedgets -= 1 unless @expectedgets <= 0;
    checkready();
  end
  
  def fire(entry, value)
    @prereqs.delete(entry)
    checkReady();
  end
  
  def find(version)
    if @version >= version then self;
    elsif @next == nil then nil;
    else @next.find(version)
    end
  end
  
  def pending
    (@expectedgets > 0) && 
      @prereqs.empty? && 
        ((@lastversion == 0) ||
          ((@prev != nil) && (@prev.version != @lastversion))
        ) && 
      @prev.pending;
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
        @next.next.checkReady;
      else
        @next.add(record)
      end
      self;
    else
      record.next = self;
      @prev = record;
      checkReady;
      record;
    end
  end
  
  def collapse
    @prev.prev = nil unless @prev == nil;
    if ready then
      @next.collapse
    else
      if @prev == nil then 
        self;
      else 
        @prev.prev = nil;
        @prev.lastversion = 0;
        @prev; 
      end
    end
  end
  
  private
  
  def checkReady
    if ready then
      @callbacks.each_pair do |target, callback|
        @partition.register(target, callback);
      end
    end
    @callbacks = Hash.new;
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
    if @value.is_a? TemplateValuation then
      begin
        @value = @value.to_f # throws an exception if it fails.
      rescue Thrift::Exception => ex
        #if we're unable to evaluate it straight up, we need to wait.
        @required = @value.entries.keys;
      end
    end
    #puts "Created record requiring entries: " + @required.join(", ");
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
        nil; # this should only happen if we get an insert past the deletion boundary
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
    @required.delete(entry)
    @value.discover(entry, value) if @value.is_a? TemplateValuation;
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
        handler.findPartition(req.source, req.key).register(req, CommitNotification.new(self));
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
    
    @data = MultiKeyMap.new(@start.size);
    @massputrecords = nil;
    
    @unreceivedputs = Hash.new;
  end
  
  def contains?(key)
    key = [key] unless key.is_a? Array;
    raise SpreadException.new("Trying to determine contains with an inconsistent key size; key:" + key.size.to_s + "; partition: " + @start.size.to_s) unless key.size == @start.size;
    key.each_index do |i|
      if (key[i].to_i < @start[i]) || (key[i].to_i >= @start[i] + @range[i]) then return false end;
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

    # Next, we see if the request is a multitarget request
    if target.key.index(-1) == nil then 
      # No -1?  Good, all keys in the target are defined... it's a single-target request.
      
      #version 0 is always 0.
      if target.version <= 0 then callback.fire(target, 0); return; end;
      
      record = @data[target.key];
      record = record.find(target, massrecord == nil || massrecord.ready) unless record == nil;
      
      if record == nil then
        if massrecord == nil then 
          # if the corresponding put hasn't arrived yet, we defer installation until it has.
          @unreceivedputs[target.version] = Array.new unless @unreceivedputs.has_key? target.version; 
          @unreceivedputs[target.version].push([target, callback]);
        elsif massrecord.ready
          # if we don't have a matching record, but we do have a more recent committed mass put, 
          # then the value has never been modified
          calback.fire(target, 0); 
          return; 
        else
          # finally, it's possible that the massput hasn't committed yet (for whatever reason)
          # in this case, defer the callback until the mass put does commit.  
          massrecord.register(target, callback)
        end
      elsif record.pending then
        record.register(callback);
      else
        callback.fire(target, record.value);
      end
    else
      # There's at least one -1 in the target.  This is a multitarget request.
      
      # To simplify our lives here, we're going to assume that the switch sends us a separate entry for 
      # each key in its recent history.  Since we use mass-put records to discard old versions anyway...
      # this seems like the thing to do.  This, in turn, means that a multi-target request is guaranteed
      # to have a mass-put entry assigned to it.  This in turn, means that there are three possibilities
      
      if massrecord == nil then
        # if the corresponding put hasn't arrived yet, we defer installation until it has.
        @unreceivedputs[target.version] = Array.new unless @unreceivedputs.has_key? target.version; 
        @unreceivedputs[target.version].push = [target, callback];
      elsif massrecord.ready
        # go go go
        callback.hold;
        @data.scan(target) do |key, value|
          callback.fire(key, value);
        end
        callback.release;
      else
        # finally, it's possible that the massput hasn't committed yet (for whatever reason)
        # in this case, defer the callback until the mass put does commit.  
        massrecord.register(target, callback)
      end
      
    end
  end
  
  def massinsert(version, lastversion, expectedgets, template, prereqs)
    record = ForeachPutRecord.new(version, lastversion, expectedgets, template, prereqs, self);
    if @massputrecords == nil then 
      @massputrecords = record;
    else
      @massputrecords = @massputrecords.add(record);
    end
  end
  
  def insert(target, value)
    if @data.has_key? target.key then
      record = @data[target.key].insert(target, value)
    else
      record = @data[target.key] = CommitRecord.new(target, value);
    end
    if @unreceivedputs.has_key? target.version then
      @unreceivedputs.each [target.version] do |targetCallback|
        register(targetCallback[0], targetCallback[1]);
      end
    end
    record;
  end
  
  def set(var, vers, val)
    target = Entry.new;
      target.source = @mapid;
      target.key = [var.to_i];
      target.version = [vers.to_i];
    insert(target, val.to_f)
  end
  
  def makeVersion(version)
    massInset(version)
  end
  
  def to_s
    mapid.to_s + " => [" + 
      (0...@start.size).collect do |i|
        @start[i].to_s + "::" + (@start[i].to_i + @range[i].to_i).to_s
      end.join(" ; ") + "]";
  end
  
  def dump
    @data.values.collect do |entry|
      entry.to_s
    end.join "\n";
  end
  
end



