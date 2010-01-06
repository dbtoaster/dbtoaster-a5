#!/usr/bin/env ruby

if ARGV.length != 3 then
  puts "Usage gen_repartition_spec.rb <boot spec> <# nodes> <out file>"
  exit
end

in_file = ARGV[0]
out_file= ARGV[2]
new_num_nodes = ARGV[1].to_i

f = File.open(in_file, "r")
o = File.open(out_file, "w")

spec_lines = f.readlines

(0...spec_lines.length).step(6) do |i|
  name, domvar_l, query, bindvar_l, keys_ap_l, partition_l = spec_lines[i,6]

  key_l_ap_opt = keys_ap_l.strip.split("/")
  keys = key_l_ap_opt[0].split(",")
  aps =
    if key_l_ap_opt.length > 1 then
      key_l_ap_opt[1].split("|").map do
      |x| x.split(".").map { |y| y.to_i } end
    else [] end

  pk_l, ds_l = partition_l.strip.split("/")
  partition_dims = pk_l.split(",").collect { |x| x.to_i }
  partition_dim_sizes = ds_l.split(",").collect { |x| x.to_i }
  existing_num_nodes = partition_dim_sizes.inject(1) { |acc,x| acc*x }

  partition_dims_s = partition_dims.join(",")
  aps_s = aps.collect{ |x| x.join(".") }.join("|")
  o.write("#{name.strip}/#{keys.size.to_s}/#{existing_num_nodes}/#{partition_dims_s}/#{new_num_nodes.to_s}"+(aps_s.length>0? "/#{aps_s}" : "")+"\n")
end

f.close
o.close
