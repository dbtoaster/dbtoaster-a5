
require 'thrift_compat';

class MultiKeyMap
  attr_reader :numkeys, :empty, :patterns;
  
  def initialize(numkeys, patterns, default = nil, wildcard = -1)
    @numkeys, @wildcard, @default = numkeys.to_i, wildcard, default;
    @patterns = patterns.delete_if { |pattern| pattern.size >= numkeys }.collect_hash do |pattern| 
      [ pattern.sort.freeze, 
        Hash.new { Array.new }
      ]
    end
    @basemap = Hash.new;
    @empty = true;
  end
  
  def add_pattern(pattern)
    unless (pattern.size >= numkeys) or (@patterns.has_key? pattern.sort) then
      @patterns[pattern.sort.freeze] = Hash.new { Array.new }
    end
  end
  
  def [](params)
    @basemap[params];
  end
  
  def []=(key, val)
    validate_params(key)
    raise "Error: Attempt to set a value for a wildcard key" if key.include? @wildcard;
    unless @basemap.has_key? key then
      @patterns.each_pair do |pattern, hsh|
        # hsh is the hash for this access pattern that stores the pattern inverses;
        # Between the pattern and its inverse, we have a fully specified key.  Break the
        # key we just added down and add it.
        # so pull out the components in the pattern and 
        hsh[pattern.collect { |k| key[k] }].push(key.clone.freeze);
      end
    end
    @empty = false;
    @basemap[key.clone.freeze] = val;
  end
  
  def has_key?(key)
    @basemap.has_key? key;
  end
  
  def values
    @basemap.values;
  end
  
  def scan(partial_key)
    find_pattern(partial_key).each do |key|
      raise SpreadException.new("Error: Index references nonexistent key " + key.join(",")) unless @basemap.has_key? key;
      yield key, @basemap[key];
    end
  end
  
  def replace(partial_key);
    find_pattern(partial_key).each do |key|
      @basemap[key] = yield key, @basemap[key];
    end
  end
  
  private #################################################
  
  def validate_params(params)
    raise SpreadException.new("MultiKeyMap: Tried to access multi-key map with non-array key '" + params.to_s + "' of type: " + params.class.to_s) unless params.is_a? Array;
    raise SpreadException.new("MultiKeyMap: Tried to access " + @numkeys.to_s + " key array with " + params.size.to_s + " keys: " + params.join(",")) unless params.size == @numkeys;
  end
  
  def find_pattern(key)
    validate_params(key)
    pattern = key.collect_index { |i, k| i if k != @wildcard }.compact.sort
    return [key] if pattern.size == @numkeys   # special case the fully specified access pattern
    return @basemap.keys if pattern.size == 0  # also special case the full map scan
    ret = @patterns[pattern];
    raise SpreadException.new("Invalid Access Pattern on key : [" + key.join(",") + "]; " + pattern.join(",")) unless ret;
    ret[pattern.collect { |k| key[k] }];
  end
end

