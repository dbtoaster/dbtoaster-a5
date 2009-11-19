
require 'template'
require 'ok_mixins'
require 'getoptlong'

class Config
  attr_reader :templates, :nodes, :partition_sizes, :my_port;
  attr_writer :my_name, :my_port;
  def initialize
    @nodes = Hash.new { |h,k| h[k] = { 
      "partitions" => Hash.new  { |h,k| h[k] = Array.new }, 
      "values" => Hash.new { |h,k| h[k] = Hash.new }, 
      "address" => NodeID.make("localhost") 
    } };
    @templates = Hash.new;
    @partition_sizes = Hash.new { |h,k| h[k] = Array.new };
    @my_name = nil;
    @my_port = 52982;
  end
  
  def load(input)
    curr_node = "Solo Node"
    
    input.each do |line|
      cmd = line.scan(/[^ ]+/);
      case cmd[0]
        when "node" then 
          curr_node = cmd[1].chomp;

        when "address" then
          @nodes[curr_node]["address"] = NodeID.make(*cmd[1].chomp.split(/:/));

        when "partition" then 
          match = /Map *([0-9]+)\[([0-9, ]+)\]/.match(line);
          raise SpreadException.new("Unable to parse partition line: " + line) if match.nil?;
          dummy, map, segment = *match;
          segment = segment.split(/, */).collect { |i| i.to_i }
          
          @nodes[curr_node]["partitions"][map.to_i].push(segment);
          @partition_sizes[map.to_i] = 
            segment.zip(@partition_sizes[map.to_i]).collect { |sizes| if sizes[1].nil? then sizes[0] else Math.max(*sizes) end };

        when "value" then 
          match = /Map *([0-9]+) *\[([^\]]*)\] *v([0-9]+) *= *([0-9.]+)/.match(line)
          raise SpreadException.new("Unable to parse value line: " + line) if match.nil?;
          
          dummy, source, keys, version, value = *match;
          
          @nodes[curr_node]["values"][map.to_i][keys.split(/, */).collect do |k| k.to_i end] = value.to_f;


        when "template" then 
          cmd.shift; 
          @templates[cmd.shift] = UpdateTemplate.new(cmd.join(" "));
      end
    end
  end
  
  def Config.opts
    [
      [ "-n", "--node", GetoptLong::REQUIRED_ARGUMENT ]
    ]
  end
  def parse_opt(opt, arg)
    case opt
      when "-n", "--node" then 
        match = /([a-zA-Z0-9_\-]+)@([a-zA-Z0-9._\-]+)(:([0-9]+))?/.match(arg)
        raise "Invalid Node Parameter: " + arg unless match;
        @nodes[match[1]]["address"] = NodeID.make(match[2], match[4].to_i);
    end
  end
  
  def each_partition(node)
    @nodes.fetch(node)["partitions"].each_pair do |map, plist|
      plist.each { |partition| yield map, partition }
    end
  end
  
  def each_value(node)
    @nodes.fetch(node)["values"].each_pair do |map, vlist|
      plist.each_pair { |key, value| yield map, key, value }
    end
  end
  
  def range_test(map)
    boundaries = @nodes.collect do |node, prefs|
      prefs["partitions"][map]
    end.concat!.matrix_transpose
    
    puts(boundaries.collect do |key_ranges|
      key_ranges.collect do |range|
        range.to_s;
      end.join(", ")
    end.join("\n"))
  end
  
  def partition_ranges(map)
    @nodes.collect do |node, prefs|
      prefs["partitions"][map]
    end.concat!.matrix_transpose.collect do |ranges|
      ranges.sort { |a, b| a.begin <=> b.begin }.uniq
    end
  end
  
  def node_for_entry(map, key)
    @nodes.each_pair do |node, prefs|
      if prefs["partitions"].has_key? map then
        prefs["partitions"][map].each do |range|
          return node if range.merge(key).assert { |pair| pair[0] === pair[1] }
        end
      end
    end
    return nil;
  end
  
  def address_for_node(node)
    raise SpreadException.new("Unknown Node: " + node) unless (@nodes.has_key? node)
    raise SpreadException.new("Unknown Node Address: " + node) unless (@nodes[node].has_key? "address")
    @nodes[node]["address"];
  end
  
  def map_keys(map)
    ret = nil;
    @templates.each_value do |t|
      if t.target.source == map then
        if ret then
          ret = ret.collect_pair(t.target.keys) do |old, new|
            if old.size <= new.size then old else new end;
          end
        else
          ret = t.target.keys.clone;
        end
      end 
    end
    ret;
  end
  
  def my_name
    return @my_name if @my_name;
    @nodes.each_pair do |node, info|
      return @my_name = node if (info["address"].host == `hostname`.chomp) && 
                                (info["address"].port.to_i == @my_port.to_i);
    end
    "Solo Node";
  end
  
  def my_config
    @nodes[my_name];
  end
end