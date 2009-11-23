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
$spread_dir = File.dirname(__FILE__) + "/../..";

$local_node, $local_server = SlicerNode::Processor.listen($config_file, $spread_dir, $verbosity);

if $serving then
  puts "====> Server Ready <===="
else
  $pending_servers = $local_node.config.nodes.size + 1; # the +1 is for the switch
  $local_node.declare_master do |log_message, handler|
    $pending_servers -= 1 if /Starting #<Thrift::NonblockingServer:/.match(log_message)
    if $pending_servers <= 0 then
      Logger.info { "Server Initialization complete, Starting Client..." };
      $clients[$local_node.config.switch.host].start_client
    end
    puts log_message;
    if $pending_servers > 0 then handler else nil end;
  end
  $clients = Hash.new do |h,k|
    manager = SlicerNode::Manager.initialize(k, $spread_dir, $config_file);
    Thread.new(manager) { |manager| loop { exit unless manager.check_error; sleep 10; } };
    manager.client.start_logging(NodeID.make(`hostname`, 52980));
    h[k] = manager.client;
  end
  $clients["localhost"] = $local_node;
  
  Logger.info { "Starting Nodes..." };
  $local_node.config.nodes.each do |node, node_info|
    Logger.info { "Starting : " + node };
    $clients[node_info["address"].host].start_node(node_info["address"].port);
  end
  
  Logger.info { "Starting Switch..." };
  $clients[$local_node.config.switch.host].start_switch;
end

$local_server.serve;