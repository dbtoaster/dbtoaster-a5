
class Array
  def each_pair(other, explode_on_different_size = false)
    raise Exception.new("Paired loop on arrays of different sizes (" + size + ", " + other.size + ")") if explode_on_different_size && (other.size != size);
    (0...Math.min(other.size, size)).each do |i|
      yield self[i], other[i];
    end
  end
  
  def collect_pair(other)
    min_size = if other.size > size then size else other.size end;
    ret = Array.new;
    (0...min_size).each do |i|
      ret.push(yield self[i], other[i]);
    end
    ret;
  end
  
  def collect_hash(&block);
    ret = Hash.new
    each do |entry| 
      if block.nil? then
        ret[entry[0]] = entry[1];
      else
        kv = block.call(entry);
        ret[kv[0]] = kv[1];
      end
    end;
    ret;
  end
  
  def reduce(&block);
    ret = Hash.new
    each do |entry| 
      kv = 
        if block.nil? then entry
        else block.call(entry) end;
      ret.assert_key(kv[0]) do Array.new end.push(kv[1]);
    end;
    ret;
  end
  
  def collect_index
    (0...size).collect do |i|
      yield i, self[i];
    end
  end
  
  def hash_keys
    ret = Hash.new
    each do |entry|
      ret[entry] = (yield entry);
    end;
    ret;
  end
  
  def find
    each do |entry|
      return true if(yield entry);
    end
    return false;
  end
  
  def find_pair(other)
    each_pair(other) do |entry, oentry|
      return true if(yield entry, oentry);
    end
    return false;
  end
  
  def merge(*others)
    min_size = Math.min(size, others.collect do |o| o.size end);
    (0...min_size).collect do |i|
      [ self[i] ].concat(others.collect do |o| o[i] end);
    end
  end
  
  def drop_front
    self.shift;
    self;
  end
  
  def reduce
    ret = Hash.new;
    each do |i|
      ret.assert_key(i[0]){ Array.new }.push(i[1])
    end
    ret;
  end
end

class Hash
  def collect
    keys.collect do |k| yield k, self[k] end;
  end

  def assert_key(key)
    return self[key] if has_key? key;
    self[key] = yield;
  end
end

class String
  def is_number?
    to_f.to_s == self || to_i.to_s == self;
  end
end

class Logger
  @@logger = nil;
  @@default_name = "Unnamed Logger";
  
  def Logger.default
    if @@logger.nil?
      @@logger = Logger.new(STDERR);
      @@logger.level = Logger::WARN;
    end
    @@logger;
  end
  
  def Logger.default_name=(default_name)
    @@default_name = default_name;
  end
  def Logger.default_name
    @@default_name || caller_info(1);
  end
  
  def Logger.caller_info(steps = 0)
    caller[1+steps].gsub(/in `([^']*)'/, "\\1").gsub(/.*\/([^\/]*)$/, "\\1")
  end
  
  def Logger.default=(logger)
    @@logger = logger;
  end
  
  def Logger.default_level=(default_level)
    default.level = default_level;
  end
  
  def Logger.fatal(progname = default_name)
    (default.level < FATAL) || default.fatal(progname) { yield }
  end
  def Logger.error(progname = default_name)
    (default.level < ERROR) || Logger.default.error(progname) { yield }
  end
  def Logger.warn(progname = default_name)
    (default.level < WARN) || Logger.default.warn(progname) { yield }
  end
  def Logger.info(progname = default_name)
    (default.level < INFO) || Logger.default.info(progname) { yield }
  end
  def Logger.debug(progname = default_name)
    (default.level < DEBUG) || Logger.default.debug(progname) { yield }
  end
  def Logger.temp(string)
    Logger.info(string, "TEMPORARY");
  end

end

module Math
  def Math.min(*params)
    params = params.flatten;
    min = if params.empty? then nil else params[0] end;
    params.each do |param|
      min = param unless param >= min;
    end
    min;
  end
  def Math.max(*params)
    params = params.flatten;
    min = if params.empty? then nil else params[0] end;
    params.each do |param|
      min = param unless param <= min;
    end
    min;
  end
end

class Range
  def <=>(other)
    return self.begin <=> other.begin;
  end
  
  def overlaps?(other)
    (self.begin < other.end) && (other.begin < self.end)
  end
end