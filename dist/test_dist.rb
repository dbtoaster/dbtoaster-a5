#!/usr/bin/env ruby

Dir.chdir "#{File.dirname $0}/dbtoaster";

def test_dir(dir)
  Dir.entries(dir).each do |f|
    if f[0] != "."[0] then
      f = "#{dir}/#{f}"
      if File.directory? f 
        then test_dir(f)
        elsif /\.sql$/ =~ f then test_file(f)
        else puts "Ignoring #{f}"
      end
    end
  end
end

def test_file(f)
  puts "Testing #{f}";
  system("bin/dbtoaster #{f} 2>&1 > /dev/null") or exit -1;
end

test_dir "examples/queries";