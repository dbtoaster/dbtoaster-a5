
require 'thrift';
require 'map_node';
require 'spread_types';
require 'template';
require 'multikeymap';
require 'ok_mixins';

###################################################

class PutDelta
  attr_reader :value;
  alias :to_f :value;

  def initialize(value)
    @value = value;
  end
  
  def to_s 
    "<PutDelta: " + to_f.to_s + ">"
  end
end

###################################################

class PutCompleteCallback
  def initialize(partition, version)
    @partition, @version = partition, version
    Logger.debug { "Created PutCompleteCallback for version: " + version.to_s }
  end
  
  def release
  
  end
  
  def fire(entry, value)
    return if value.nil?
    Logger.debug { "Firing PutCompleteCallback for version: " + @version.to_s + "; on: " + entry.to_s }
    @partition.collapse_record_to(entry.key, @version)
  end
end

###################################################

class MassPutRecord
  attr_reader :version, :template, :next, :prev;
  attr_writer :next, :prev;
  
  def initialize(version, template, partition)
    @version, @template, @partition = 
      version, template, partition;
    
    @next, @prev = nil, nil;
    @callbacks = Hash.new;
    @gets_pending = true;
  end
  
  def register(key, callback, exemptions = Set.new)
    raise SpreadException.new("MassPutRecord.register without a callback") unless @callbacks != nil;
    if @callbacks.has_key? key then
      @callbacks[key].push([callback, exemptions]);
    else
      @callbacks[key] = [[callback, exemptions]];
    end
  end
  
  def last
    if @next then @next.last else self end;
  end
  
  def last_ready
    if @next && @next.ready then @next.last_ready else self end;
  end
  
  def first_unready_version(cmp = nil)
    v = last_ready;
    if v && v.next then
      if cmp then Math.min(cmp, v.next.version) else v.next.version end;
    else
      cmp;
    end
  end
  
  def pending
    (@gets_pending) || ((@prev != nil) && (@prev.pending));
  end
  
  def ready
    !pending;
  end
  
  def add(record)
    if @next then @next.add(record);
             else @next = record; record.prev = self;
    end
  end
  
  # put is called when a new target value is discovered.  
  def put(target, delta_value)
    raise SpreadException.new("Put for invalid target") unless @partition.mapid == target.source;
    if @partition.contains? target.key then
      @partition.insert(target, @version, PutDelta.new(delta_value))
    end
  end
  
  def complete
    @gets_pending = false;
    check_ready;
  end
  
  def check_ready
    if @callbacks && ready then
      Logger.info { "Massput v" + @version.to_s + " complete" }
      @callbacks.each_pair do |target, cblist|
        cblist.each do |callback|
          @partition.get(target, callback[0], @version, callback[1]);
        end
      end
      
      # If we've gotten this far, we've committed everything and everything is in order.  
      # We can start deleting older mass records.
      @partition.collapse_mass_records_to(@version);
      
      # This might potentially complete the next mass put.
      @next.check_ready if @next;
      
      # And ensure that we never get called again.
      @callbacks = nil;
    end
  end
  
  def to_s(recurse = false)
    unless recurse then "Mass Puts: " else ", " end +
      "v"+@version.to_s+" (template:" + @template.to_s + "; " +
      if ready then "" else " not" end +
      " ready)" + if @next then @next.to_s(true) else "" end;
  end
end

###################################################

class PutRecord
  attr_reader :target, :version, :required, :value, :next;
  attr_writer :next, :prev, :value;
  
  def initialize(target, version, value, prev = nil)
    @target, @version, @value, @callbacks = target.clone, version.to_i, value, Array.new;
    @next, @prev = nil, prev;
    
    # If the value is a MapEquation, we need to evaluate 
    @required = 
      case @value
        when TemplateValuation then @value.entries.keys;
        when PutDelta, Numeric then Array.new;
        else raise SpreadException.new("Creating PutRecord with value of unknown type: " + @value.class.to_s);
      end;
    
    unless @prev == nil then
      @prev.register(self); # this will lead to firing callbacks once @prev becomes ready
    else
      self.fire_callbacks;
    end
  end
  
  def last
    if @next then @next.last else self end;
  end
  
  def find(version)
    if (@version > version) && (@prev != nil) then @prev
    elsif (@version < version) && (@next != nil) then @next.find(version)
    else self;
    end
  end
  
  def insert(version, value)
    insertpoint = find(version);
    if insertpoint.version == version then
      raise SpreadException.new("Overwriting version : " + version.to_s + " of " + @target.to_s + "; current value: " + insertpoint.value.to_s + "; new value: " + value.to_s);
#      insertpoint.value = value;
#      insertpoint;
    else 
      newrec = PutRecord.new(@target, version, value, self);
      newrec.next = insertpoint.next;
      insertpoint.next = newrec;
      newrec;
    end
  end
  
  def to_s
    "(v" + @version.to_s + " = " + @value.to_s + "; " + 
      if ready then "ready" else "pending" end + ")" +
      if @next then ", " + @next.to_s else "" end;
  end
    
  def pending
    @required.size > 0 || (@prev != nil && @prev.pending);
  end
  
  def ready
    !pending
  end
  
  def register(callback)
    if ready then callback.fire(@target, @value);
    else callback.fire(@target, nil); @callbacks.push(callback);
    end
  end
  
  # Discover is the callback used upon "discovery" of map values that
  # need to be filled in for the unevaluated expression in this record.
  # Discover also takes charge of firing the callbacks waiting for this
  # record to complete.
  def discover(entry, value) 
    Logger.info {"Discovered: " + entry.to_s + " = " + value.to_s };
    @required.delete(entry)
    @value.discover(entry, value) if @value.is_a? TemplateValuation;
    fire_callbacks;
  end
  
  # CommitRecord can act as a pseudo-callback; This callback fires when
  # the previous record is ready.
  def fire(entry, value)
    fire_callbacks unless value.nil?;
  end
  
  def finish_message
    ready;
  end
  
  def fire_callbacks
    unless pending || @callbacks.nil? then
      Logger.info {"Completed " + @target.to_s + "v" + @version.to_s + "; Firing " + @callbacks.size.to_s + " callbacks" };
      @value = @value.to_f + if @prev.nil? then 0 else @prev.value.to_f end;
      @callbacks.each do |cb|
        cb.fire(@target, @value);
      end
      @callbacks = nil;
    end
  end
end



###################################################

class MapPartition
  attr_reader :mapid, :start, :range;
  
  # A MapPartition is a chunk of a map (ID: mapid) holding keys in the
  # range [start, start+range).  Values are stored versioned; 
  
  def initialize(mapid, start, range, patterns)
    @mapid, @start, @range = mapid.to_i, [start].flatten, [range].flatten;
    
    if start.size != range.size then raise SpreadException.new("Creating partition with inconsistent start/range sizes") end;
    @start.freeze;
    @range.freeze;
    
    @data = MultiKeyMap.new(@start.size, patterns);
    @massputrecords = nil;
  end
  
  def contains?(key)
    key = [key] unless key.is_a? Array;
    raise SpreadException.new("Trying to determine contains with an inconsistent key size; key:" + key.size.to_s + "; partition: " + @start.size.to_s) unless key.size == @start.size;
    key.each_index do |i|
      if (key[i] != -1) &&
         ((key[i].to_i < @start[i]) || 
          (key[i].to_i >= @start[i] + @range[i])) then return false end;
    end 
    return true;
  end
  
  def end
    @range.collect_pair(@start) do |r, s| s+r end;
  end
  
  # The semantics of GET are a little wonky for this map due to 
  # versioning.  Specifically, if the request is for an incomplete 
  # version, the request will fail.  In theory, we could hold up processing 
  # until the version becomes available (assuming it does happen), but
  # this might tie up the worker thread.  If the data is available when
  # the request comes in, we can return immediately.  If an asynch get
  # is required, then pass a callback parameter.
  
  # The last two parameters deserve a little extra discussion.  Both of 
  # these parameters are used exclusively by the massput commit process.
  # We don't know which values a massput writes to until it commits, 
  # callbacks are deferred until the massput actually does commit.  This
  # means that we need to be able to re-issue the query for a version that
  # has occurred at some point in the past.  That said, it's possible that
  # a wildcard get will include both a pending massput's results AND a set
  # of subsequent individual updates.  In this case, we record the newer
  # updates and exclude them from the re-issued mass-put.
  def get(target, callback = nil, version = nil, exemptions = Set.new)

    # if version != nil then we know that the relevant massput has committed, and
    # we can safely ignore the mass put records.
    lastmassrecord =
      if @massputrecords != nil && version.nil? then @massputrecords.last;
      else nil end;
    
    if target.has_wildcards? then
      # This is a multitarget request.
      
      unless callback then
        # deliver the most recent completed results now, even if they're out of date
        ret = Array.new;
        @data.scan(target.key) do |key, value|
          value = value.next while (value.next && value.next.ready);
          ret.push(value.value)
        end
        return ret;
      else
        # deliver results as they come in
        
        if lastmassrecord != nil && lastmassrecord.pending then
          # Parts of the request may be covered by a pending massput.
          # That said, parts may NOT be covered.  Let's find those first.
          
          # if we're working with massput records, we know we're not working with a re-issued query
          exemptions = Set.new;
          @data.scan(target.key) do |key, value|
            value = value.last;
            if value.version >= lastmassrecord.version then 
              value.register(callback);
              exemptions.add(key);
            end
          end
          
          lastmassrecord.register(target, callback, exemptions);
        else
          # if the last massput has committed, OR there is no last massput, we just use what we have.
          @data.scan(target.key) do |key, value|
            next if exemptions.include? key; # check the skiplist
            value = if version == nil then value.last else value.find(version) end;
            value.register(callback);
          end
          callback.release
        end
      end
    else
      # This is a single-target request  

      return if exemptions.include? target;  # check the skiplist first
      
      lastrecord = @data[target.key];
      if version == nil then
        lastrecord = lastrecord.last unless lastrecord == nil;
      else
        lastrecord = lastrecord.find(version) unless lastrecord == nil;
      end
      
      value = 
        if lastrecord == nil then
          if lastmassrecord == nil || lastmassrecord.ready then
            # Case 1: We haven't gotten ANY record of this put whatsoever.  The value's never been written
            # Case 2: This value has never been updated as the result of a mass put, thus use the default.
            0
          else
            # This value may be part of a pending mass put.  Defer the callback until the put finishes.
            lastmassrecord.register(target, callback) unless callback == nil;
            nil;
          end
        elsif lastmassrecord == nil || lastmassrecord.version < lastrecord.version then
          if lastrecord.ready then
            # This value is not part of a newer mass put and has been committed.  Return it.
            # (alternatively, 
            lastrecord.value.to_f;
          else
            # This value is not part of a newer mass put but has not been committed.  Register the callback.
            lastrecord.register(callback) unless callback == nil;
            nil;
          end
        elsif lastmassrecord.ready then
          # This record is part of a newer mass put... but the put has committed.  Use the old value
          lastrecord.value.to_f;
        else
          # This record is part of a newer, uncommitted mass put.  Defer until we know if the put triggers a change.
          lastmassrecord.register(target, callback) unless callback == nil;
          nil;
        end
        
      unless value == nil || callback == nil then
        callback.fire(target, value);
      else
        raise SpreadException.new("Request for incomplete version") unless value != nil;
      end
      return value;
    end
  end
  
  def mass_insert(version, template)
    record = MassPutRecord.new(version, template, self);
    if @massputrecords == nil then 
      @massputrecords = record;
    else
      @massputrecords = @massputrecords.add(record);
    end
    record;
  end
  
  def insert(target, version, value)
    if @data.has_key? target.key then
      record = @data[target.key].insert(version, value)
    else
      record = @data[target.key] = PutRecord.new(target, version, value);
    end
    record.register(PutCompleteCallback.new(self, version));
    record;
  end
  
  def set(var, vers, val)
    record = @data[var]
    if record then
      record.value = record.value + val.to_f;
    else 
      @data[var] = PutRecord.new(Entry.make(@mapid, var), vers, val.to_f);
    end
  end
  
  def lookup(key)
    ret = Hash.new
    @data.scan(key) do |key, value|
      value = value.last;
      ret[key] = value.value
    end
    return ret;
  end
  
  def collapse_mass_records_to(version)
    @massputrecords = @massputrecords.next while ((!@massputrecords.nil?) && (@massputrecords.version < version));
    @massputrecords.prev = nil unless @massputrecords.nil?;
  end
  
  def collapse_record_to(key, version)
    version = @massputrecords.first_unready_version(version) unless @massputrecords.nil?;
    @data.replace(key) do |k, v|
      Logger.debug { "Collapsing key: " + k.to_s + "; version: " + version.to_s + " to record " + v.find(version).to_s; }
      v.find(version);
    end
  end
  
  def to_s
    mapid.to_s + " => [" + 
      (0...@start.size).collect do |i|
        @start[i].to_s + "::" + (@start[i].to_i + @range[i].to_i).to_s
      end.join(" ; ") + "]";
  end
  
  def dump
    @massputrecords.to_s + "\n" + 
    @data.values.sort do |a, b|
      a.target.key <=> b.target.key
    end.collect do |entry|
      "Map " + entry.target.to_s + " : " + entry.to_s
    end.join("\n");
  end
  
  def empty?
    @data.empty;
  end
  
  def patterns
    @data.patterns;
  end
end



