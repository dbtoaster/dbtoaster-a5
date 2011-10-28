#!/usr/bin/env ruby

$row_count = 10000
$distinct_values = 1000

$row_count.times do |i|
  puts "#{rand($distinct_values)},#{rand($distinct_values)}"
end