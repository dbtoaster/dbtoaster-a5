#!/usr/bin/env ruby

$: << File.dirname(File.dirname(__FILE__))+"/common" unless $:.include? File.dirname(File.dirname(__FILE__))+"/common";

require 'ok_mixins';

thriftFile = File.open(ARGV.shift).read;
otherFiles = ARGV.collect do |fname|
  File.open(fname).read;
end.join("\n");

thriftFile.scan(/service +([a-zA-Z0-9_]+) *\{([^}]+)\}/).each do |service|
  name, content = service;
  #get rid of comments
  content.gsub!(/#[^\n\r]*/, "");
  content.gsub!(/\/\/[^\n\r]*/, "");
  content.gsub!(/\/\*.*\*\//, "");
  
  funcs = 
    content.scan(/([a-zA-Z0-9_]+) *\(([^)]*)\)/).delete_if do |f| 
      f[0] == "throws" 
    end.collect_hash do |f| 
      [ f[0], 
        f[1].gsub(/[ \t\r\n]+/, " ").split(/ *, */).collect do |var| var.split(/ /)[-1] end
      ] end;

  puts "==> Checking Service: " + name;
  
  classData = 
    Regexp.new("class *" + name + "Handler").match(otherFiles);
  if classData == nil then
    puts "Error: Service: " + name + " has no handler '" + name + "Handler' defined";

    exit -1;
  end
  
  classData = classData.post_match;
  
  funcs.each_pair do |fname, fvars|
    match = 
      classData.scan(Regexp.new("def +" + fname + " *\\(([^)]*)\\)")).flatten;
    if match.size < 1 then
      puts "Error: Function: " + fname + " is not defined";
      exit -1;
    else
      params = match[0].split(/ *, */).collect do |var| var.gsub(/ *=.*/, "") end
      puts "   `---> Checking for " + fname + "(" + fvars.join(", ") + ")";
      if fvars.size != params.size then
        puts "Error: Function " + fname + " has incorrect number of variables";
        puts "  Found: " + params.join(", ");
        puts "  Expected: " + fvars.join(", ");
        exit -1;
      end
      params.paired_loop(fvars) do |left, right|
        if left != right then
          puts "Error: Variable mismatch found in function " + fname;
          puts "  Found: " + left;
          puts "  Expected: " + right;
          exit -1;
        end
      end
    end
  end
  
#  funcs.each do |f| puts f[0] + " : " + f[1].join(";") + "\n-------\n" end;
  puts "  " + name + "'s schema successfully validated";
end