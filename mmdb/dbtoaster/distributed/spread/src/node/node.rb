#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';
require 'compiler';

###################################################

class CommitRecord
  attr_reader :target, :value, :pending, :next;
  attr_writer :next, :pending;
  
  def initialize(target, value)
    @target, @value = target, value;
    @next = nil;
    @pending = true;
  end
  
  def to_s
    if (@next == nil) || @next.pending then "value " + 
                                            target.source.to_s + " " + 
                                            target.key.to_s + " " + 
                                            target.version.to_s + " " + 
                                            value.to_s;
                                       else @next.to_s;
    end
  end
  
end

###################################################

class MapPartition
  attr_reader :mapid, :start, :range
  
  def initialize(mapid, start, range)
    @mapid, @start, @range = mapid.to_i, start.to_i, range.to_i;
    
    @data = Hash.new;
  end
  
  # The semantics of GET are a little wonky for this map due to 
  # versioning.  Specifically, if the request is for an unknown version,
  # an incomplete version, or a version that has already been discarded,
  # the request will fail.  In theory, we could hold up processing 
  # until the version becomes available (assuming it does happen), but
  # this might tie up the worker thread.  If the data is available when
  # the request comes in, we can return immediately.  If not, then we 
  # register a callback (if we're using asynch get) or fail immediately
  # (otherwise).  
  def get(target)
    val = @data[target.key];
    if val == nil then
      # If the object isn't defined, we assume version 0 => 0.
      if target.version > 0 then raise SpreadException.new("Request for an unknown object"); end
      return 0;
    end
    while val.target.version < target.version
      if val.next == nil
        raise SpreadException.new("Request for unknown version");
      end
      val = val.next;
    end
    
    if val.pending
      raise SpreadException.new("Request for incomplete version");
    end
    
    return val.value.to_f;
  end
  
  def put(cmd)
  end
  
  def set(var, vers, val)
    target = Entry.new;
      target.source = @mapid;
      target.key = var.to_i;
      target.version = vers.to_i;
    record = CommitRecord.new(target, val.to_f);
      record.pending = false;
    @data[target.key] = record;
  end
  
  def to_s
    "partition " + mapid.to_s + " " + start.to_s + " " + range.to_s + "\n" +
      (@data.values.collect do |entry|
        entry.to_s
      end.join "\n");
  end
  
end

###################################################

class MapNodeHandler

  def initialize()
    @maps = Hash.new;
    @templates = Hash.new;
  end
  
  ############# Internal Accessors
  
  def findPartition(source, key)
    map = @maps[source.to_i];
    if map == nil then raise SpreadException.new("Request for unknown map"); end
    map.each do |partition|
      if (key.to_i > partition.start) && (key.to_i - partition.start < partition.range)
        return partition;
      end
    end
    raise SpreadException.new("Request for unknown partition");
  end
  
  def createPartition(map, start, range)
    if ! @maps.has_key? map.to_i then @maps[map.to_i] = Array.new; end
    @maps[map.to_i].push(MapPartition.new(map.to_i, start.to_i, range.to_i));
    puts ("Created partition " + map.to_s + " => [" + start.to_s + ":" + (start.to_i + range.to_i).to_s + "]");
  end
  
  def createPutTemplate(index, cmd)
    @templates[index.to_i] = MapEquation.parse(cmd);
    puts ("Loaded Put Template ["+index.to_s+"]: " + @templates[index.to_i].to_s );
  end
  
  def dump
    @maps.values.collect do |map|
      map.collect do |partition|
        partition.to_s
      end.join "\n"
    end.join "\n"
  end
  
  ############# Basic Remote Accessors

  def put(id, template, target, paramList)
    if ! @templates.has_key? template then raise SpreadException.new("Unknown put template: " + template); end
    
    paramMap = Hash.new;
    paramList.each_index do |i|
      param = paramList[i];
      param.type = case param.type
        when PutFieldType::VALUE then :num;
        when PutFieldType::ENTRY then param.value = param.entry; :map;
        else :num;
      end
      paramMap[i] = param;
    end
    paramMap[-1] = PutField.new;
      paramMap[-1].value = target;
      paramMap[-1].type = :map;
    
    puts "In: " + @templates[template].to_s
    puts "Out: " + @templates[template].parametrize(paramMap).to_s
  end
  
  def get(target)
    ret = Hash.new()
    target.each do |t|
      ret[t] = findPartition(t.source,t.key).get(t);
    end
    ret;
  end
  
  ############# Asynchronous Reads

  def fetch(target, destination, cmdid)
  
  end
  
  def pushget(result, cmdid)
  
  end
  
  ############# Internal Control
  
  def setup(input)
    input.each do |line|
      cmd = line.scan(/[^ \t]+/);
      case cmd[0]
        when "partition" then createPartition(cmd[1], cmd[2], cmd[3]);
        when "value" then findPartition(cmd[1], cmd[2]).set(cmd[2], cmd[3], cmd[4]);
        when "put" then cmd.shift; createPutTemplate(cmd.shift, cmd.join(" "));
      end
    end
  end
  
end

###################################################

class MapNodeInterface
  
  def initialize(host, port=52982)
    @port = port;
    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(host, port))
    @protocol = Thrift::BinaryProtocol.new(@transport)
    @client = MapNode::Client.new(@protocol)
    @getrequest = Array.new;
    @transport.open();
  end
    
  def close
    @transport.close;
  end
  
  def dump
    @client.dump;
  end
  
  def makeEntry(source, key, version, node = nil)
    entry = Entry.new;
    entry.source = source;
    entry.key = key;
    entry.version = version;
    entry;
  end
  
  ############# GET
  
  def queueGet(source, key, version, node = nil)
    @getrequest.push(makeEntry(source, key, version, node));
  end
  
  def issueGet
    if @getrequest.size > 0 then request = @getrequest;
                                 @getrequest = Array.new
                                 @client.get(request);
                            else Hash.new;
    end
  end
  
  def get(source, key, version)
    @getrequest = Array.new
    queueGet(source, key, version);
    issueGet();
  end
  
  ############# PUT
  
  def put(version, template, target, *params)
    convertedParams = 
      params.collect do |param|
        field = PutField.new()
        field.type = 
          if param.is_a? Numeric then 
            field.value = param.to_f;
            puts "Value: " + param.to_f.to_s;
            PutFieldType::VALUE;
          elsif param.is_a? Entry then
            field.entry = param;
            puts "Entry: " + param.to_s;
            PutFieldType::ENTRY;
          else 
            raise SpreadException.new("Unknown parameter type");
          end;
        field;
      end
    @client.put(version, template, target, convertedParams);
  end
  
end
