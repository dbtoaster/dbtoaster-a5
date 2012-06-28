#!/usr/bin/env ruby

require "#{File.dirname($0)}/util.rb"

$dbt_path = "#{File.dirname($0)}/../.."
$dbt = "bin/dbtoaster"
Dir.chdir $dbt_path

ARGV.each do |sql|
  if File.file? sql then
    IO.popen("#{$dbt} #{sql} -l k3", "r") do |f|
      puts sql unless (f.readlines.grep /Singleton/).empty?;
    end
  else
    puts "Not a file: #{sql}"
  end
end