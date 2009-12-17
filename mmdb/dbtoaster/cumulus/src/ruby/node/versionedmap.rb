
require 'config/template';
require 'node/multikeymap';
require 'util/ok_mixins';

###################################################

class PendingRecord
  attr_reader :ready, :updates;

  def initialize(partition)
    @partition = partition;
    @callbacks = Array.new;
    @updates = Array.new;
    @ready = false;
  end
  
  ### Callback Interface
  def register(*params)
    @callbacks.push(params)
  end
  
  def fire
    @callbacks.each { |cb| @partition.fire(*cb) }
  end
  
  ### Record Insertion Interface
  def discover(key, delta)
    @updates.push([key, delta]);
    self;
  end
  
  def finish
    @ready = true;
    @partition.finish_pending;
    self;
  end
end

###################################################

class MapPartition
  attr_reader :mapid, :partition, :pending_cleared
  
  # A MapPartition is a chunk of Map {mapid}, that holds keys those keys described by
  # {partition};  The partitioning scheme is hash-based, so we will see the keys where
  # {key[i].hash % (num_maps)} is equal to {partition[i]}
  #
  # Patterns are access patterns; Each pattern is an array of key dimension indices
  # that is used to generate a secondary index over the data; Wildcard reads over this 
  # map partition can only follow one of those access patterns
  
  def initialize(mapid, partition, patterns, pfiles)
    @mapid, @partition = mapid.to_i, partition;
    
    @data = MultiKeyMap.new(@partition.size, patterns, "Map" + mapid.to_s, pfiles);
    @pending = Array.new;
    @pending_cleared = 0
  end
  
  def get(target, on_entry, on_finish)
    if on_entry then
      if @pending.size > 0 
        then @pending[-1].register(target, on_entry, on_finish);
        else fire(target, on_entry, on_finish);
      end
    else
      raise SpreadException.new("Get for a wildcard target (#{target}) without a callback") if target.has_wildcards?
      @data[target];
    end
  end
  
  def set(target, value)
    @data[target] = value;
  end
  
  def update(target, delta)
    if @pending.empty? then
      @data[target] = delta + @data[target];
      nil;
    else
      declare_pending.discover(target, delta).finish;
    end
  end
  
  def declare_pending
    @pending.push(PendingRecord.new(self))[-1];
  end
  
  def finish_pending
    while (not @pending.empty?) && @pending[0].ready
      pending = @pending.shift; 
      pending.updates.each { |target, delta| set(target, delta) };
      pending.fire;
      @pending_cleared += 1;
    end
  end
  
  def fire(target, on_entry, on_finish)
    @data.scan(target) { |key, value| on_entry.call(key, value) };
    on_finish.call() if on_finish; 
  end
  
  def to_s
    mapid.to_s + " => [" + @partition.join(",") + "]";
  end
  
  def dump
    @pending.join("\n").to_s + "\n" + 
    @data.values.sort do |a, b|
      a.target.key <=> b.target.key
    end.collect do |entry|
      "Map " + entry.target.to_s + " : " + entry.to_s
    end.join("\n");
  end
  
  def backlogged?
    not @pending.empty?
  end
  
  def backlog
    @pending.size;
  end
  
  def empty?
    @data.empty;
  end
  
  def patterns
    @data.patterns;
  end
  
  def add_pattern(partition)
    @data.add_pattern(partition);
  end
  
  def sync
    @data.sync
  end
  
end