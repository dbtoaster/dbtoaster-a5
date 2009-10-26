#!/usr/bin/env ruby

require 'getoptlong';
require 'slicer';
require 'ok_mixins';
require 'logger';
require 'getoptlong';

Logger.default_level = Logger::INFO;
Logger.default_name = nil;

slicer = Slicer.new;

GetoptLong.new(
  [ "-q", "--quiet", GetoptLong::NO_ARGUMENT ]
).each do |opt,arg|
  case opt
    when "-q", "--quiet" then Slicer.mode = :normal;
  end
end

ARGV.each do |arg|
  slicer.setup(File.open(arg));
end

begin
  slicer.start;
  sleep 1 while true;
rescue Exception => e
  puts e.to_s
  puts e.backtrace.join("\n");
end

