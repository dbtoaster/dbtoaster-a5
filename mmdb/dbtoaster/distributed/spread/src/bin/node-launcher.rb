
require 'thrift';
require 'node';
require 'compiler';

puts "Initializing Map Node Handler..."

handler = MapNodeHandler.new();
puts ARGV.to_s;
ARGV.each do |arg|
  handler.setup(File.open(arg));
end

puts "done\nInitializing Thrift..."

processor = MapNode::Processor.new(handler)
transport = Thrift::ServerSocket.new(52982)
transportFactory = Thrift::BufferedTransportFactory.new()
server = Thrift::SimpleServer.new(processor, transport, transportFactory);

puts "done\nStarting server..."

server.serve();

puts "done\n";