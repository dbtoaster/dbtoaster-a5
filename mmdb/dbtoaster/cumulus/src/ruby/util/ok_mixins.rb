
class Array
  def each_pair(other, explode_on_different_size = false)
    raise Exception.new("Paired loop on arrays of different sizes (" + size + ", " + other.size + ")") if explode_on_different_size && (other.size != size);
    (0...Math.min(other.size, size)).each do |i|
      yield self[i], other[i];
    end
  end
  
  def each_with_index
    each_index do |i|
      yield i, self[i];
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
      r = yield entry;
      return r if r;
    end
    return false;
  end
  
  def assert
    each do |entry|
      return false unless yield entry;
    end
    return true;
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
  
  def tabulate
    widths = [];
    each do |row|
      row.each_index do |i|
        widths[i] = Math.max(widths[i].to_i, row[i].to_s.size);
      end
    end
    collect do |row|
      row.collect_pair(widths) do |col, width|
        col.to_s + (" " * (width - col.to_s.size));
      end.join("  ")
    end.join("\n");
  end
  
  def matrix_transpose
    width = Math.max(collect { |r| r.size })
    ret = Array.new;
    (0...width).each { ret.push(Array.new) }
    each do |row|
      (0...width).each do |i|
        ret[i].push(row[i]);
      end
    end
    ret
  end
  
  def concat!
    (0...size).each { concat(shift) }
    self;
  end
  
  def each_cross_product(depth = 0, pattern = Array.new)
    if depth >= size then
      if depth == 0 then yield pattern.clone else yield end;
    else
      self[depth].each do |val|
        pattern.push(val);
        each_cross_product(depth+1, pattern) { if depth == 0 then yield pattern.clone else yield end };
        pattern.pop;
      end
    end
  end
  
  def cross_product(depth = 0, pattern = Array.new)
    ret = Array.new;
    each_cross_product { |out| ret.push(out) };
    ret;
  end
  
  def sum(&pr)
    tot = 0;
    if pr then
      each { |e| tot += pr.call(e) }
    else
      each { |e| tot += e.to_i }
    end
    tot;
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
      @@logger = Logger.new();
#      @@logger.level = Logger::WARN;
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
#    (default.level < FATAL) || default.fatal(progname) { yield }
  end
  def Logger.error(progname = default_name)
#    (default.level < ERROR) || Logger.default.error(progname) { yield }
  end
  def Logger.warn(progname = default_name)
#    (default.level < WARN) || Logger.default.warn(progname) { yield }
  end
  def Logger.info(progname = default_name)
#    (default.level < INFO) || Logger.default.info(progname) { yield }
  end
  def Logger.debug(progname = default_name)
#    (default.level < DEBUG) || Logger.default.debug(progname) { yield }
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
  def Math.sum(*params)
    tot = 0.0;
    params.each { |param| tot += param.to_f }
    tot;
  end
  def Math.avg(*params)
    sum(*params) / params.size;
  end
end

class Fixnum
  @@max_fixnum = nil;
  
  def Fixnum.max_fixnum
    # Ruby transparently upgrades Fixnums to Bignums when they would otherwise overflow a fixnum.
    # This function returns the maximum size a number can be (on this platform) and still be a fixnum.
    # In addition to the sign bit, Ruby also uses one bit to identify this as an integer rather than an object.
    # This number should be something like 1.844674407370955e+19 on a 64 bit; f'ref TPC-H maxes out at 2147483647
    @@max_fixnum || (@@max_fixnum = (2**(((1.size)*8)-2))-1);
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

class MatchData
  def map(keys)
    keys.merge(to_a.drop_front).collect_hash
  end
end

class Queue
  # Return an array of all elements pending in the queue.
  # If nothing is pending, block until something is.
  # Note: this method is NOT synchronized for multiple READERS.  
  def pop_pending 
    ret = Array.new
    ret.push(pop) if empty?;
    ret.push(pop) until empty?;
    ret;
  end
end

