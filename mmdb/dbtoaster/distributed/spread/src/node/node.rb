#!/usr/bin/env ruby

require 'map_node';
require 'spread_types';
require 'compiler';

###################################################

class CommitRecord
  attr_reader :target, :value, :pending, :next;
  attr_writer :next, :pending;
  
  def initialize(target, value)
    @target, @value = target, value;
    @next = nil;
    @pending = true;
  end
  
  def to_s
    if (@next == nil) || @next.pending then "value " + 
                                            target.source.to_s + " " + 
                                            target.key.to_s + " " + 
                                            target.version.to_s + " " + 
                                            value.to_s;
                                       else @next.to_s;
    end
  end
  
end

###################################################

class MapPartition
  attr_reader :mapid, :start, :range
  
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
  
  def set(var, vers, val)
    target = Entry.new;
      target.source = @mapid;
      target.key = var.to_i;
      target.version = vers.to_i;
    record = CommitRecord.new(target, val.to_f);
      record.pending = false;
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
  end
  
  ############# Internal Accessors
  
  def findPartition(source, key)
    map = @maps[source.to_i];
    if map == nil then raise SpreadException.new("Request for unknown map"); end
    map.each do |partition|
      if (key.to_i > partition.start) && (key.to_i - partition.start < partition.range)
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
  
  def dump
    @maps.values.collect do |map|
      map.collect do |partition|
        partition.to_s
      end.join "\n"
    end.join "\n"
  end
  
  ############# Basic Remote Accessors

  def put(cmd)
    
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
  
  end
  
  def pushget(result, cmdid)
  
  end
  
  ############# Internal Control
  
  def setup(input)
    input.each do |line|
      cmd = line.scan(/[^ \t]+/);
      case cmd[0]
        when "partition" then createPartition(cmd[1], cmd[2], cmd[3]);
        when "value" then findPartition(cmd[1], cmd[2]).set(cmd[2], cmd[3], cmd[4]);
      end
    end
  end
  
end



