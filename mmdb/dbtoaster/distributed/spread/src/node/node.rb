#!/usr/bin/env ruby

$:.push('../common');
$:.push('../gen-rb');
$:.unshift('/usr/local/thrift/lib/rb/lib');

require 'thrift';
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
  
end

###################################################

class MapPartition
  attr_reader :mapid, :start, :range
  
  def initialize(mapid, start, range)
    @mapid, @start, @range = mapid, start, range;
    
    @committed = Hash.new;
  end
  
  def get(target)
    val = @committed[target.key];
    if val == nil then
      return 0;
    end
    while val.target.version < target.version do
      if val.next == nil then
        raise SpreadException.new("Request for unknown version");
      end
      val = val.next;
    end
    
    if val.pending then
      raise SpreadException.new("Request for incomplete version");
    end
    
    return val.value.to_f;
  end
  
  def put(cmd)
    
  end
  
end


###################################################

class MapNodeHandler

  def initialize()
    @maps = Hash.new;
  end
  
  def put(cmd)
    
  end
  
  def get(target)
  
  end
  
  
  
end

