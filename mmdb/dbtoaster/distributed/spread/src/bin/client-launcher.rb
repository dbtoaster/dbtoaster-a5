#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'switch_node';
require 'spread_types';
require 'spread_types_mixins';
require 'node';
require 'getoptlong';

$interactive = true;
$verbose = false;
$test = false;
$cols = Hash.new;
$transforms = Hash.new;
$input = STDIN

GetoptLong.new(
  [ "--quiet"      , "-q", GetoptLong::NO_ARGUMENT ],
  [ "--use"        , "-u", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--transform"  , "-t", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--test"       , "-e", GetoptLong::NO_ARGUMENT ],
  [ "--tpch-stream", "-s", GetoptLong::OPTIONAL_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "--quiet", "-q" then $interactive = false;
    
    when "--use", "-u" then 
      match = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(arg);
      raise "Invalid use argument: " + arg unless match;
      $cols[match[1]] = match[2].split(/ *, */).collect { |i| i.to_i };
      
    when "--transform", "-t" then
      match = / *([a-zA-Z\-+_]*) *\[([0-9,]+)\](~\/([^\/]*)\/([^\/]*)\/|<([a-z]?)([0-9, ]+))/.match(arg)
      raise "Invalid transform argument: " + arg unless match;
      $transforms.assert_key(match[1]){ Hash.new }.assert_key(match[2].to_i){ Array.new }.push(
        case match[3][0]
          when "~"[0] then [:regex, Regexp.new(match[4]), match[5]]
          when "<"[0] then [:cmp, match[6], match[7].split(/ *, */)]
          else raise "Error: Unknown transform type: " + match[3]
        end)
        
    when "--test", "-e" then
      $test = true; $verbose = true;
      
    when "--tpch-stream", "-s" then
      tpch_dir = File.dirname(__FILE__) + "/../tpch-simple";
      Dir.chdir(tpch_dir)
      $input = open("|./streamgen" + if arg.nil? then "" else " " + arg end, "r");
  end
end



switch = SwitchNode::Client.connect("localhost", 52981) unless $test;

def compare_date(indices, params)
  indices = indices.collect{ |i| params[i.to_i] };
  last = indices.shift.split("-").collect { |i| i.to_s };
  raise "Not a date: " + last.join("-") unless last.size == 3;
  indices.each do |i|
    i = i.split("-").collect { |c| c.to_s };
    raise "Not a date: " + i.join("-") unless i.size == 3;
    (0...3).each do |c|
      return false if last[c] > i[c];
    end
    last = i;
  end
  return true;
end

print "\n>> " if $interactive;
$input.each do |line|
  args = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(line);
  unless args then
    puts "ERROR: can not parse '" + line + "'";
    next;
  end
  begin
    table = args[1];
    params = args[2].split(/ *, */);
    
    if $transforms.has_key? table then
      $transforms[table].each_pair do |col,tlist|
        tlist.each do |t|
          params[col.to_i] = 
            case t[0]
              when :regex then params[col.to_i].gsub(t[1], t[2]);
              when :cmp   then
                case t[1]
                  when "d" then
                    if compare_date(t[2], params) then "1" else "0" end;
                end
            end
        end
      end
    end
    
    if $cols.has_key? table then
      tmp_params = params;
      params = Array.new;
      $cols[table].each do |i|
        params.push(tmp_params[i]);
      end
    end
    
    switch.update(table, params) unless $test;
    puts table+"("+params.join(", ")+")" if $verbose;
  rescue SpreadException => e
    puts "Error: " + e.to_s;
  end
  print "\n>> " if $interactive;
end
