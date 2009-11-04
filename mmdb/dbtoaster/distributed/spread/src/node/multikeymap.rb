
require 'spread_types';

class MultiKeyMap
  attr_reader :numkeys, :empty;
  
  def initialize(numkeys, default = nil, wildcard = -1)
    @numkeys, @wildcard, @default = numkeys.to_i, wildcard, default;
    @basemap = Hash.new;
    @empty = true;
  end
  
  def [](params)
    validate_params(params)
    
    nestedvar = @basemap;
    params.each do |param|
      return @default unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    nestedvar;
  end
  
  def []=(key, val)
    validate_params(key)
    @empty = false;

    nestedvar = @basemap;
    
    lastindex = key[-1];
    
    key.slice(0...-1).each do |param|
      nestedvar[param] = Hash.new unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    nestedvar[lastindex] = val;
  end
  
  def has_key?(params)
    validate_params(params)
    
    nestedvar = @basemap;
    params.each do |param|
      return false unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    true;
  end
  
  def values
    ret = Array.new;
    scan_impl(
      [-1] * @numkeys,
      Proc.new do |key, value| ret.push(value) end,
      false
    );
    ret;
  end
  
  def scan(key, &block)
    validate_params(key)
    scan_impl(key, block, false);
  end
  
  def replace(key, &block);
    validate_params(key)
    scan_impl(key, block, true);
  end
  
  private #################################################

  def scan_impl(params, block, replace, depth = 0, nestedmap = @basemap, paramstack = Array.new, parentmap = nil)
    if depth >= @numkeys then
      # once we reach the inner depths, instead of a map, we have a value
      newval = block.call(paramstack.clone, nestedmap);
      parentmap[paramstack[-1]] = newval if replace;
    else
      if params[depth] == @wildcard then
        nestedmap.each_pair do |key, value|
          paramstack.push(key);
          scan_impl(params, block, replace, depth+1, value, paramstack, nestedmap);
          paramstack.pop;
        end
      else
        if nestedmap.has_key? params[depth] then
          paramstack.push(params[depth]);
          scan_impl(params, block, replace, depth+1, nestedmap[params[depth]], paramstack, nestedmap);
          paramstack.pop;
        end
      end
    end
  end
  
  private
  
  def validate_params(params)
    raise SpreadException.new("MultiKeyMap: Tried to access multi-key map with non-array key '" + params.to_s + "' of type: " + params.class.to_s) unless params.is_a? Array;
    raise SpreadException.new("MultiKeyMap: Tried to access " + @numkeys.to_s + " key array with " + params.size.to_s + " keys: " + params.join(",")) unless params.size == @numkeys;
  end
end

