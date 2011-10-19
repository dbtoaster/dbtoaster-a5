#!/usr/bin/env ruby
outfile   = "test/data/sl_servers.dat"
updates   = 100000;
num_racks = 1000;
s_per_r   = 20;

File.open outfile, "w+" do |f|
  # initialization
  servers = Hash.new;
  num_servers = num_racks*s_per_r;
  (0...num_servers).each do |sid|
    servers[sid] = 0.0;
    f.puts("1,#{(sid/s_per_r).to_i},0.0");
  end

  (0...updates).each do |i|
    sid = rand(num_servers);
    f.puts("0,#{(sid/s_per_r).to_i},#{servers[sid].to_f}");
    servers[sid] = rand;
    f.puts("1,#{(sid/s_per_r).to_i},#{servers[sid].to_f}");
  end
end