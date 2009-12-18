
require 'fileutils';
require 'thrift_compat';
require 'bdb2';
require 'ok_mixins';


class MultiKeyMap
  attr_reader :numkeys, :empty, :patterns;
  
  def initialize(numkeys, patterns, name = "", pfiles = [],
                 basepath = "/tmp", default = nil, wildcard = -1)
    @numkeys, @wildcard, @default, @basepath = numkeys.to_i, wildcard, default, basepath;
    #@patterns = patterns.delete_if { |pattern| pattern.size >= numkeys };
    patterns.delete_if { |pattern| pattern.size >= numkeys };
    @empty = true;
    @name = "#{name}-#{Process.pid.to_s}";
    @pfile, @sfiles = *pfiles;
    #puts "Node #{name} pfiles, p: #{@pfile} s: #{@sfiles.join(",")}"
    initialize_db(patterns);
  end
  
  def add_pattern(pattern)
    unless (pattern.size >= numkeys) or (@patterns.has_key? pattern.sort) then
      create_secondary_index(pattern.sort.freeze);
    end
  end
  
  def [](key)
    @basemap[Marshal.dump(key)].to_f;
  end
  
  def []=(key, val)
    validate_params(key);
    #puts "#{@name}[#{key.join(",")}] = #{val}";
    raise "Error: Attempt to set a value for a wildcard key" if key.include? @wildcard;
    #We don't have to check if k exists in the map, it'd be overwritten by the new value
    @basemap.put(nil, Marshal.dump(key), val.to_s, 0);
  end
  
  def has_key?(key)
    ret = @basemap.get(nil, Marshal.dump(key), nil, 0);
    if(ret == 0)
      return true;
    else
      return false;
    end
  end
  
  def values
    @basemap.values;
  end
  
  def scan(partial_key)
    validate_params(partial_key)
    if partial_key.include? @wildcard then
      pattern = partial_key.collect_index { |i, k| i if k != @wildcard }.compact.sort
      raise SpreadException.new("MKM: #{@name} (with patterns: #{@patterns.keys.join(",")}) partial key: #{params.join(",")}") unless (@patterns.has_key? pattern);

      cur = @patterns[pattern].cursor(nil, 0);
      pattern_key = Marshal.dump(pattern.collect{|k| partial_key[k]});
      
      key, pkey, val = cur.pget(pattern_key, nil, Bdb::DB_SET);

      while key
        yield Marshal.restore(pkey), val.to_f;
        key, pkey, val = cur.pget(pattern_key, nil, Bdb::DB_NEXT_DUP);
      end
      cur.close;
    else
      yield partial_key, self[partial_key]
    end
  end
  
  def replace(partial_key)
    new_vals = Array.new;
    scan(partial_key) do |key, val|
      new_vals.push([key, (yield key, val)]);
    end
    new_vals.each do |key, val|
      self[key] = val;
    end
  end
  
  def close
    @patterns.each do |pattern|
      @patterns[pattern].close;
    end
  end
  
  def sync
    @basemap.sync
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
    return [key] if pattern.size == @numkeys   # special case the fully specified access pattern
    return @basemap.keys if pattern.size == 0  # also special case the full map scan
    ret = @patterns[pattern];
    raise SpreadException.new("Invalid Access Pattern on key : [" + key.join(",") + "]; " + pattern.join(",")) unless ret;
    ret[pattern.collect { |k| key[k] }];
  end
  
  
  def initialize_db(patterns, delete_old = true)
    Logger.warn { "Creating database for map : #{@name}" };
    @env = Bdb::Env.new(0);
    @env.cachesize = 128*1024*1024
    @env.open(@basepath, Bdb::DB_INIT_CDB | Bdb::DB_INIT_MPOOL | Bdb::DB_CREATE, 0);
    @basemap = @env.db;
    db_file = "#{@basepath}/db_#{@name}_primary.db"
    if @pfile && (File.exist? @pfile) then
      puts "Copying primary pfile #{@pfile}=>#{db_file}"
      FileUtils.cp @pfile, db_file;
      delete_old = false;
    end
    cleanup_db if delete_old;
    Logger.warn { "Creating Primary Index: #{db_file}" }
    @basemap.open(nil, "#{db_file}", nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
    at_exit { File.delete "#{db_file}" } if delete_old;
    @patterns = Hash.new;
    patterns.each do |pattern|
      create_secondary_index(pattern)
    end
  end
  
  def cleanup_db
    if (File.exist? "#{@basepath}/db_#{@name}_primary.db") then
      File.delete("#{@basepath}/db_#{@name}_primary.db")
      i = 0;
      while File.exist? "#{@basepath}/db_#{@name}_#{i}.db"
        File.delete("#{@basepath}/db_#{@name}_#{i}.db")
        i += 1;
      end
    end
  end

  def create_secondary_index(pattern, delete_old = true)
    i = @patterns.size;
    db = @env.db;
    db.flags = Bdb::DB_DUPSORT;
    s_pfile = @sfiles.fetch(i, nil) unless @sfiles.nil?;
    sdb_file = "#{@basepath}/db_#{@name}_#{i}.db"
    Logger.warn { "Creating Secondary Index: #{sdb_file} (#{pattern.join(", ")})" }
    if @pfile && s_pfile.nil? then
      raise SpreadException.new("Missing bootstrap file for secondary index #{i}.");
    elsif @pfile && s_pfile && (File.exist? s_pfile) then
      puts "Copying secondary pfile #{s_pfile}=>#{sdb_file}"
      FileUtils.cp s_pfile, sdb_file;
    end
    db.open(nil, sdb_file, nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0);
    at_exit { File.delete sdb_file } if delete_old;
    @basemap.associate(nil, db, 0, proc { |sdb, key, data| key = Marshal.load(key); Marshal.dump(pattern.collect{|k| key[k]})});
    #puts "Creating secondary key for #{@name} #{key.join(",")}; #{pattern.join(",")}"; 
    @patterns[pattern] = db;
  end
end
