
class Array
  def paired_loop(other)
    min_size = if other.size > size then size else other.size end;
    (0...min_size).each do |i|
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
  
  def collect_hash
    ret = Hash.new
    each do |entry| 
      kv = yield entry;
      ret[kv[0]] = kv[1];
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
end

class Hash
  def collect
    keys.collect do |k| yield k, self[k] end;
  end
end

class String
  def is_number?
    to_f.to_s == self || to_i.to_s == self;
  end
end

class Logger
  @@logger = nil;
  @@default_name = nil;
  
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
  
  def Logger.default=(logger)
    @@logger = logger;
  end
  
  def Logger.default_level=(default_level)
    default.level = default_level;
  end
  
  def Logger.fatal(progname = @@default_name)
    Logger.default.fatal(progname) { yield }
  end
  def Logger.error(progname = @@default_name)
    Logger.default.error(progname) { yield }
  end
  def Logger.warn(progname = @@default_name)
    Logger.default.warn(progname) { yield }
  end
  def Logger.info(progname = @@default_name)
    Logger.default.info(progname) { yield }
  end
  def Logger.debug(progname = @@default_name)
    Logger.default.debug(progname) { yield }
  end
  def Logger.temp(string)
    Logger.info(string, "TEMPORARY");
  end
end
