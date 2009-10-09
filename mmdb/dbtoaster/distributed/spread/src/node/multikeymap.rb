
require 'spread_types';

class MultiKeyMap
  attr_reader :numkeys;
  
  def initialize(numkeys, default = nil, wildcard = -1)
    @numkeys, @wildcard, @default = numkeys.to_i, wildcard, default;
    @basemap = Hash.new;
  end
  
  def [](params)
    validateParams(params)
    
    nestedvar = @basemap;
    params.each do |param|
      return @default unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    nestedvar;
  end
  
  def []=(key, val)
    validateParams(key)

    nestedvar = @basemap;
    
    lastindex = key[-1];
    
    key.slice(0...-1) do |param|
      nestedvar[param] = Hash.new unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    nestedvar[lastindex] = val;
  end
  
  def has_key?(params)
    validateParams(params)
    
    nestedvar = @basemap;
    params.each do |param|
      return false unless nestedvar.has_key? param;
      nestedvar = nestedvar[param];
    end
    true;
  end
  
  def values
    ret = Array.new;
    scanImpl(
      [-1] * @numkeys,
      Proc.new do |key, value| ret.push(value); end
    );
    ret;
  end
  
  def scan(params, &block)
    validateParams(params)
    scanImpl(params, block);
  end
  
  private #################################################

  def scanImpl(params, block, depth = 0, nestedmap = @basemap, paramstack = Array.new)
    if depth >= params.size then
      block.call(paramstack.clone, nestedmap); # once we reach the inner depths, instead of a map, we have a value
    else
      if params[depth] == @wildcard then
        nestedmap.each_pair do |key, value|
          paramstack.push(key);
          scanImpl(params, block, depth+1, value, paramstack);
          paramstack.pop;
        end
      else
        if nestedmap.has_key? params[depth] then
          paramstack.push(params[depth]);
          scanImpl(params, block, depth+1, nestedmap[params[depth]], paramstack)
        end
      end
    end
  end
  
  private
  
  def validateParams(params)
    raise SpreadException.new("MultiKeyMap: Tried to access " + @numkeys.to_s + " key array with " + params.size.to_s + " keys: " + params.join(",")) unless params.size == @numkeys;
  end
end

