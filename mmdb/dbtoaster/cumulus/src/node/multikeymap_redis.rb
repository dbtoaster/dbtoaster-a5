require 'rubygems'
require 'redis'
require 'spread_types';


class MultiKeyMap_Redis
  attr_reader :numkeys, :empty, :patterns;

  #basemap stores the key value pairs
  #The same db is used to store partial_key=>list[keys] map
  #partial_keys are converted to partial_key+pattern and used as keys
  #Serialization is achieved using join(',') as there were issues with using 
  #Marshal.dump  
  def initialize(numkeys, patterns, map_name = 0, default = nil, wildcard = -1)
      @numkeys, @wildcard, @default = numkeys.to_i, wildcard, default;
      options = { :db => map_name};
      @basemap = Redis.new(options);
      @patterns = patterns.delete_if { |pattern| pattern.size >= numkeys }.collect_hash do |pattern|
        [ pattern.sort.freeze,
          pattern.sort.join(',') 
        ]

      end
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
    #key_in = Marshal.dump(key);
    key_in = key.join(',');
    validate_params(key)
    raise "Error: Attempt to set a value for a wildcard key" if key.include? @wildcard;

    if @basemap[key_in] == nil then
      @patterns.each_pair do |pattern, pattern_string|
        # hsh is the hash for this access pattern that stores the pattern inverses;
        # Between the pattern and its inverse, we have a fully specified key.  Break the
        # key we just added down and add it.
        # so pull out the components in the pattern and 
        #hsh[pattern.collect { |k| key[k] }].push(key.clone.freeze);
        set_name = "";
        set_name << pattern.collect{|k| key[k]}.join(',');
        set_name << ":";
        set_name << pattern_string.to_s;
        #puts "in set function : setname = #{set_name}, key_in = #{key_in}"
        @basemap.set_add(set_name, key_in);
        #p @basemap.set_members set_name
      end
    end
    @empty = false;
    @basemap[key_in] = val;
    #puts "patterns = #{@patterns}"
  end
  
  def has_key?(key)
    if(@basemap[key.join(',')] == nil)
      return false;
  else
    return true;
  end
end
  
  def values
    @basemap.values;
  end
  
  def scan(partial_key)
    temp = find_pattern(partial_key);
    #puts "in scan : #{temp.join(',')}"
    find_pattern(partial_key).each do |key|
      #puts "in scan loop : key = #{key}";
      raise SpreadException.new("Error: Index references nonexistent key " + key) unless @basemap[key] != nil;
      yield key, @basemap[key];
    end
  end
  
  def replace(partial_key);
    find_pattern(partial_key).each do |key|
      @basemap[key.join(',')] = yield key, @basemap[key.join(',')];
    end
  end
  
  def clear(key)
    @basemap.delete(key.join(','));
  end
  
  private #################################################
  
def validate_params(params)
  unless (params.is_a? Array)
    puts "Error : Param is not an array, param class : #{params.class()}";
  end
  unless (params.size ==@numkeys)
    puts "Error : Param of wrong length, param length : #{params.size}";
  end
end
  

def find_pattern(key)
  validate_params(key)
  pattern = key.collect_index { |i, k| i if k != @wildcard }.compact.sort
  #puts "in find_pattern : pattern = #{pattern.join(',')}";
  return [key] if pattern.size == @numkeys   # special case the fully specified access pattern
  return @basemap.keys if pattern.size == 0  # also special case the full map scan
  ret = @patterns[pattern];
  #puts "#{ret}"
  raise SpreadException.new("Invalid Access Pattern on key : [" + key.join(",") + "]; " + pattern.join(",")) unless ret;
  @basemap.set_members("#{pattern.collect{ |k| key[k]}.join(',') <<":"<< ret.to_s}");
  #ret[pattern.collect { |k| key[k] }];
  
end
end

