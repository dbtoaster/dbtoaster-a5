
require 'thrift';
require 'node';
require 'template';

puts "done\nInitializing Server..."

server = MapNode::Processor.listen(52982, ARGV);

puts "done\nStarting server..."

server.serve();

puts "done\n";