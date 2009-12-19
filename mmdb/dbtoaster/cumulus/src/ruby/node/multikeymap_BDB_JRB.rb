include Java;

include_class Java::org::dbtoaster::cumulus::node::MultiKeyMapJavaImpl;

class MultiKeyMap 
  def initialize(numkeys, patterns, name = "", pfiles = [],
                 basepath = "/tmp", default = nil, wildcard = -1)
    @java_impl = MultiKeyMapJavaImpl.new(numkeys, patterns, name, default);
    @wildcard = wildcard
  end
  
  def add_pattern(pattern)
    @java_impl.add_pattern(pattern.to_java(:Integer))
  end
  
  def [](key)
    @java_impl.get(key.to_java(:Integer)) || 0.0;
  end
  
  def []=(key, val)
    @java_impl.put(key.to_java(:Integer), val);
  end
  
  def has_key?(key)
    @java_impl.has_key(key.to_java(:Integer));
  end
  
  def values
    ret = Hash.new;
    cursor = @java_impl.fullScan;
    while cursor.next;
      ret[cursor.key] = cursor.value;
    end
    cursor.close;
    ret;
  end
  
  def scan(partial_key)
    unless partial_key.include? @java_impl.wildcard then
      yield(partial_key, self[partial_key]);
      return;
    end
    partial_key = partial_key.collect { |k| k unless k == @wildcard };
    cursor = @java_impl.scan(partial_key.to_java(:Integer));
    while cursor.next;
      yield cursor.key.to_a.collect { |k| k.to_i }, cursor.value.to_f;
    end
    cursor.close;
  end
  
  def replace(partial_key)
    partial_key = partial_key.collect { |k| k unless k == @wildcard };
    cursor = @java_impl.scan(partial_key.to_java(:Integer));
    while cursor.next;
      new_val = yield cursor.key.to_a.collect { |k| k.to_i }, cursor.value.to_f;
      cursor.replace(new_val);
    end
    cursor.close;
  end
  
  def close
  end
  
  def sync
  end
  
end
