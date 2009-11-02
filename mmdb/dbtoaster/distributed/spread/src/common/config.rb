
require 'template'
require 'ok_mixins'
require 'getoptlong'

class Config
  attr_reader :templates, :nodes;
  def initialize
    @nodes = Hash.new;
    @templates = Hash.new;
  end
  
  def load(input)
    curr_node = "Solo Node"
    
    input.each do |line|
      cmd = line.scan(/[^ ]+/);
      case cmd[0]
        when "node" then 
          curr_node = cmd[1].chomp;


        when "partition" then 
          match = /Map *([0-9]+)\[([0-9:, ]+)\]/.match(line);
          raise SpreadException.new("Unable to parse partition line: " + line) if match.nil?;
          dummy, map, regions = *match;
          
          node_prefs(curr_node)["partitions"].assert_key(map.to_i) { Array.new }.push(
            regions.split(",").collect do |r| 
              raise "Can't parse partition line: '"+line.chomp+"' Element '" + r + "' improperly formed" unless r.split("::").size == 2
              r.split("::").collect { |n| n.to_i }
            end.collect do |r| (r[0]...r[1]) end
          )


        when "value" then 
          match = /Map *([0-9]+) *\[([^\]]*)\] *v([0-9]+) *= *([0-9.]+)/.match(line)
          raise SpreadException.new("Unable to parse value line: " + line) if match.nil?;
          
          dummy, source, keys, version, value = *match;
          
          node_prefs(curr_node)["values"].assert_key(map.to_i) { Hash.new }[keys.split(/, */).collect do |k| k.to_i end] = value.to_f;


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
        node_prefs(match[1])["address"] = NodeID.make(match[2], match[4].to_i);
    end
  end
  
  def node_prefs(node)
    @nodes.assert_key(node) { { "partitions" => Hash.new, "values" => Hash.new } }
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
      prefs["partitions"].fetch(map){ Array.new }
    end.concat!.matrix_transpose
    
    puts(boundaries.collect do |key_ranges|
      key_ranges.collect do |range|
        range.to_s;
      end.join(", ")
    end.join("\n"))
  end
  
  def partition_ranges(map)
    @nodes.collect do |node, prefs|
      prefs["partitions"].fetch(map){ Array.new }
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
end