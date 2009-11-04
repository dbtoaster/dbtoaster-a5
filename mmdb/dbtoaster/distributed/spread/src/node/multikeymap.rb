
require 'spread_types';

class MultiKeyMap
  attr_reader :numkeys, :empty;
  
  def initialize(numkeys, patterns, default = nil, wildcard = -1)
    @numkeys, @wildcard, @default = numkeys.to_i, wildcard, default;
    @patterns = patterns.delete_if { |pattern| pattern.size >= numkeys }.collect_hash do |pattern| 
      [ pattern.sort, 
        Hash.new { Array.new }
      ]
    end
    @basemap = Hash.new(0);
    @empty = true;
  end
  
  def [](params)
    @basemap[params];
  end
  
  def []=(key, val)
    validate_params(key)
    unless @basemap.has_key? key then
      @patterns.each_pair do |pattern, hsh|
        # hsh is the hash for this access pattern that stores the pattern inverses;
        # Between the pattern and its inverse, we have a fully specified key.  Break the
        # key we just added down and add it.
        # so pull out the components in the pattern and 
        hsh[pattern.collect { |k| key[k] }].add(key.freeze);
      end
    end
    @empty = false;
    @basemap[key] = val;
  end
  
  def has_key?(key)
    @basemap.has_key? key;
  end
  
  def values
    @basemap.values;
  end
  
  def scan(partial_key)
    find_pattern(key).each do |key|
      yield key, @basemap[key];
    end
  end
  
  def replace(partial_key);
    find_pattern(key).each do |key|
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
    pattern = key.collect_index { |i, k| i += 1; i if k == wildcard }.compact.sort
    return [key] if pattern.size == @numkeys # special case the fully specified access pattern
    ret = @patterns[pattern];
    raise SpreadException.new("Invalid Access Pattern on key : [" + key.join(",") + "]") unless ret;
    ret[key.clone.delete_if { |k| k == wildcard }];
  end
end

