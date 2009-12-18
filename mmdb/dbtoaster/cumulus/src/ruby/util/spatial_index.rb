
class SpatialIndex
  attr_reader :depth;
  def initialize(boundaries)
    @depth = boundaries.size;
    @map = setup_map(boundaries)
  end
  
  def [](key)
    find(key)[1];
  end
  
  def []=(key,value)
    find(key)[1] = value;
  end
  
  def update(key = Array.new, val = nil, block = nil, &in_block)
    block = in_block if block == nil;
    if key.size >= @depth then
      val[1] = block.call(key.clone, val[1]);
    else
      val = if val then val[1] else @map end;
      val.each do |v|
        key.push(v[0]);
        update(key, v, block);
        key.pop;
      end
    end
  end
  
  def find(key)
    raise SpreadException.new("Spatial Index Key of wrong size: Real size: " + key.size.to_s + "; Index size: " + @depth.to_s) unless key.size == @depth;
    val = @map;
    last = nil;
    key.each do |k|
      low = 0;
      high = val.size;
      i = 0;
      while low < high do 
        i = ((low + high) / 2).to_i;
        if val[i][0].include? k.to_i then
          low = high = i;
        elsif k.to_i < val[i][0].begin
          if high == i then i = high = high - 1
          else high = i;
          end
        else
          if low == i then i = low = low + 1
          else low = i;
          end        
        end
      end
      raise SpreadException.new("Key: [" + key.join(",") + "] out of bounds") unless (i >= 0) && (i < val.size);
      last = val[i];
      val = val[i][1];
    end
    last;
  end
  
  def scan(key, wildcard = -1, val = @map, depth = 0)
    if depth >= key.size then
      yield val;
    else
      if key[depth] == wildcard then
        val.each { |subval| scan(key, wildcard, subval[1], depth+1) { |value| yield value } }
      else
        low = 0;
        high = val.size;
        i = 0;
        while low < high do 
          i = ((low + high) / 2).to_i;
          if val[i][0].include? key[depth].to_i then
            low = high = i;
          elsif key[depth].to_i < val[i][0].begin
            if high == i then i = high = high - 1
            else high = i;
            end
          else
            if low == i then i = low = low + 1
            else low = i;
            end        
          end
        end
        raise SpreadException.new("Key: [" + key.join(",") + "] out of bounds") unless (i >= 0) && (i < val.size);
        scan(key, wildcard, val[i][1], depth+1) { |value| yield value }
      end
    end
  end
  
  def setup_map(boundaries, depth = 0)
    if depth >= @depth then
      nil;
    else
      map = Array.new;
      boundaries[depth].each do |b|
        map.push([b, setup_map(boundaries, depth+1)]);
      end
      map;
    end
  end
  
  def collect(val = @map, key = Array.new, ret = Array.new)
    if key.size >= depth then
      ret.push(yield key, val);
    else
      val.each do |v|
        key.push(v[0]);
        collect(v[1], key, ret) { |k, v| yield k, v };
        key.pop;
      end
    end
    ret;
  end
end
