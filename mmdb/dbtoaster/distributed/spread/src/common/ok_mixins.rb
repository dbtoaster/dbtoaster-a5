
class Array
  def paired_loop(other)
    minSize = if other.size > size then size else other.size end;
    (0...minSize).each do |i|
      yield self[i], other[i];
    end
  end
  
  def collect_pair(other)
    minSize = if other.size > size then size else other.size end;
    ret = Array.new;
    (0...minSize).each do |i|
      ret.push(yield self[i], other[i]);
    end
    ret;
  end
  
  def collect_hash
    ret = Hash.new
    each do |entry| 
      kv = yield entry;
      ret[kv[0]] = kv[1];
    end;
    ret;
  end
  
  def hash_keys
    ret = Hash.new
    each do |entry|
      ret[entry] = (yield entry);
    end;
    ret;
  end
end

class String
  def is_number?
    to_f.to_s == self || to_i.to_s == self;
  end
end