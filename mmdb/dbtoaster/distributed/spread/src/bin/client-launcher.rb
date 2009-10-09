#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';
require 'node';

client = MapNode::Client.connect('localhost');

puts "======== Client Dump ========";
puts client.dump();

puts "======== Getting =======";
puts client.get(1, 3, 1).to_s;

puts "======== Putting ========";
puts client.put(1, 1, client.makeEntry(1, 1, 1), 65.0, 42.1);

client.close();