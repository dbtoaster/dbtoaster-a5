#!/usr/bin/env ruby

require 'getoptlong';

$hash_app = "#{File.dirname $0}/cpp_hash"

raise "cpp_hasher doesn't exist" unless FileTest.exist?($hash_app);

def cpp_hash(str)
  `#{$hash_app} "#{str.chomp.sub(/;/, "")}"`.chomp.split(/\t/)[1].to_i
end

raise "No file name specified" if(ARGV.length < 1);

rules = [];

GetoptLong.new(
  ["--hash", "-h",       GetoptLong::REQUIRED_ARGUMENT],
  ["--order", "-o",      GetoptLong::REQUIRED_ARGUMENT]
).each do |arg,val|
  case arg
    when "--hash", "-h"  then 
      hash_index = val.to_i
      rules.push(lambda { |l| l[hash_index] = cpp_hash(l[hash_index]); l })
    when "--order", "-o" then
      rules.push(lambda { |l| val.split(/,/).map {|i| l[i.to_i]} })
    else
      raise "Unknown argument #{arg}"
  end
end


File.open(ARGV.shift) do |f|
  f.each do |l|
    l = l.sub(/^ */,"").split(/ *\| */).map {|c| c.chomp};
    rules.each do |r|
      l = r.call(l);
    end
    puts l.join(",");
  end
end