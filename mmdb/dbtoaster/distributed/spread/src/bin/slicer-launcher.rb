#!/usr/bin/env ruby

require 'getoptlong';
require 'slicer';
require 'ok_mixins';
require 'logger';
require 'getoptlong';
require 'config';

Logger.default_level = Logger::INFO;
Logger.default_name = nil;

$serving = false;
$verbosity = :normal;

GetoptLong.new(
  [ "-q", "--quiet", GetoptLong::NO_ARGUMENT ],
  [       "--serve", GetoptLong::NO_ARGUMENT ]
).each do |opt,arg|
  case opt
    when "-q", "--quiet" then Logger.default_level = Logger::WARN if $verbosity == :quiet; $verbosity = :quiet;
    when       "--serve" then $serving = true;
  end
end

raise "usage: slicer.sh CONFIG" unless ARGV.size >= 1;

$config_file = ARGV[0]

$local_node, $local_server = SlicerNode::Processor.listen($config_file, $verbosity);

server_thread = Thread.new($local_server) { |server| server.serve }
Logger.info { "Sleeping for server to come up." }
sleep 0.5;

def spin_up_slicers(*slicer_nodes)
  puts "Spinning up slicers"
  slicer_nodes.collect do |node|
    [
      node, 
      Thread.new(node) do |node| 
        manager = SlicerNode::Manager.new(node, $local_node.config.spread_path, $config_file);
        manager.client.start_logging(NodeID.make(`hostname`.chomp, 52980));
        Thread.current[:client] = manager.client;
      end
    ]
  end.collect_hash do |node, init_thread|
    init_thread.join;
    puts "Slicer for #{node} is active";
    [node, init_thread[:client]]
  end
end

if $serving then
  puts "====> Server Ready <===="
else
  $pending_servers = $local_node.config.nodes.size + 1; # the +1 is for the switch
  $local_node.declare_master do |log_message, handler|
    # This block gets executed every time we get a log message from one of our clients;
    # we use it to figure out when the switch/nodes have all started so we can initialize the
    # client.  After that point, returning nil removes this block from the loop.

    if /Starting #<Thrift::NonblockingServer:/.match(log_message) then
      $pending_servers -= 1 
      Logger.info { "A server just came up; #{$pending_servers} servers left" }
    end
    if $pending_servers <= 0 then
      Logger.info { "Server Initialization complete, Starting Client..." };
      $clients[$local_node.config.switch.host].start_client
    end
    puts log_message;
    if $pending_servers > 0 then handler else nil end;
  end
  
  nodes = Set.new
  $local_node.config.nodes.each do |node, node_info|
    nodes.add(node_info["address"].host);
  end
  nodes.add($local_node.config.switch.host);
  
  $clients = spin_up_slicers(*nodes.to_a);
  $clients["localhost"] = $local_node;
  
  Logger.info { "Starting Nodes..." };
  $local_node.config.nodes.each do |node, node_info|
    Logger.info { "Starting : #{node} @ #{node_info["address"]}" };
    $clients[node_info["address"].host].start_node(node_info["address"].port);
  end
  
  Logger.info { "Starting Switch @ #{$local_node.config.switch.host}..." };
  $clients[$local_node.config.switch.host].start_switch;

  Logger.info { "Starting Monitor" }
  monitor = SlicerMonitor.new($clients.keys.delete_if { |c| c == "localhost" }.uniq);

  Logger.info { "Sleeping until finished" };
end

Signal.trap("HUP") { exit }
sleep;
