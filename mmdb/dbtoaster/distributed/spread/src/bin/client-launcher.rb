#!/usr/bin/env ruby

require 'thrift';
require 'map_node';
require 'spread_types';

transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', 52982))
protocol = Thrift::BinaryProtocol.new(transport)
client = MapNode::Client.new(protocol)

transport.open()

request = Array.new;

entry = Entry.new;
entry.source = 1;
entry.key = 3;
entry.version = 1;
entry.node = NodeID.new; 
  entry.node.host = -2130706433; 
  entry.node.port = 52982;
request.push(entry);

puts "======== Client Dump ========";
puts client.dump();

puts "======== Requesting =======";
puts client.get(request).to_s;

transport.close();