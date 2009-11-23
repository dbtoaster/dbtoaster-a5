
require 'ok_mixins';
require 'thread';
require 'thrift';
require 'spread_types';
require 'config';
require 'remotemanager';
require 'slicer_node';
require 'pty';

class SlicerNodeHandler
  attr_reader :config, :verbosity, :is_master;
  attr_writer :message_handler;

  def initialize(config_file, spread_path, verbosity = :quiet)
    @config_file = config_file;
    @config = Config.new();
      @config.load(File.open(config_file));
    @monitor_intake = Queue.new;
    @spread_path = spread_path;
    @verbosity = verbosity;
    @is_master = false;
    @message_handler = nil;
    
    @monitor = Thread.new(@monitor_intake) do |intake|
      clients = Array.new;
      finished = false;
      master = false;
      until finished do
        cmd = intake.pop;
        case cmd[0]
          when :data    then clients.each { |c| c.receive_log(cmd[1]) };
          when :client  then clients.push(cmd[1]);
          when :exit    then finished = true;
          when :master  then master = true;
        end
      end
    end
  end
  
  def verbosity_flag 
    case @verbosity
      when :quiet then "-q"
      when :normal then ""
      when :verbose then "-v"
    end
  end
  
  def start_process(cmd)
    #cmd = cmd + " 1>&2";
    Logger.info { "Running: " + cmd; }
    Thread.new(cmd, @monitor_intake) do |cmd, output|
      begin
        PTY.spawn(cmd) do |stdin, stdout, pid|
          stdin.each { |line| output.push([:data, line]); }
        end
      rescue PTY::ChildExited
      end
    end
  end
  
  def start_switch()
    start_process(
      @spread_path+"/bin/switch.sh " + verbosity_flag + " " + @config_file
    )
  end
  
  def start_node(port)
    start_process(
      @spread_path+"/bin/node.sh -p " + port.to_s + " " + verbosity_flag + " " + @config_file
    )
  end
  
  def start_client()
    raise "To run the client, the configuration file must have a source line" unless @config.client_debug["sourcefile"];
    start_process(
      @spread_path+"/bin/client.sh -q -s " + 
        if @config.client_debug["ratelimit"] then "-l " + @config.client_debug["ratelimit"].to_s + " " else "" end +
        @config.client_debug["transforms"].collect { |t| "-t '" + t + "'" }.join(" ") + " " +
        @config.client_debug["projections"].collect { |pr| "-u '" + pr + "'"}.join(" ") + " " +
        @config.client_debug["upfront"].collect { |up| "--upfront " + up }.join(" ") + " " +        
        "-h " + @config.client_debug["sourcefile"].to_s
    )
  end
  
  def start_logging(target)
    @monitor_intake.push([:client, MapNode::Client.connect(target)]);
  end
  
  def receive_log(log_message)
    if @is_master then
      if @message_handler.nil? then 
        puts log_message;
      else 
        @message_handler = @message_handler.call(log_message, @message_handler)
      end
    end
  end
  
  def declare_master(&message_handler);
    @monitor_intake.push([:client, self]) unless @is_master;
    @is_master = true;
    @message_handler = message_handler;
  end
  
  def poll_stats()
    `ps aux`.split("\n").delete_if { |line| /ruby.*-launcher|redis-server/.match(line).nil? }.join("\n");
  end
  
  def shutdown()
    @monitor_intake.push([:exit]);
  end
end

module SlicerNode 
  class Manager
    def initialize(host, spread_path, config_file)
      @host = host;
      @process = RemoteProcess.new(
        spread_path + "/src/slicer.sh --serve " + config_file,
        host, 
        true
      )
      @ready = false;
    end
    
    def running
      @process.status;
    end
    
    def log
      ret = "";
      ret += @process.log.pop until @process.log.empty?
      ret;
    end
    
    def check_error
      puts log unless running;
      running;
    end
    
    def client
      @ready = /====> Server Ready <====/.match(@process.log.pop) until @ready
      if @client then @client
      else @client = Client.connect(@host)
      end
    end
  end
  
  class Client
    def close
      @iprot.trans.close;
    end
    
    def Client.connect(host, port = 52980)
      if host.is_a? NodeID then
        port = host.port;
        host = host.host;
      end
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port));
      protocol = Thrift::BinaryProtocol.new(transport);
      ret = SlicerNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
  
  class Processor
    def Processor.listen(config_file, spread_path, verbosity = :quiet, port = 52980);
      handler = SlicerNodeHandler.new(config_file, spread_path, verbosity);

      processor = SlicerNode::Processor.new(handler);
      transport = Thrift::ServerSocket.new(port);
      [handler, Thrift::NonblockingServer.new(
        processor, 
        transport, 
        Thrift::FramedTransportFactory.new,
        Thrift::BinaryProtocolFactory.new,
        1,
        nil
      )];
    end
  end
end

class SlicerMonitor
  def initialize(slicers)
    @slicers = slicers.each
  end
  
  def start
    @connections.each { |c| c.start };
    Thread.new(@connections) do |connections|
      loop do
        log = connections.collect do |c| 
          c.queue.pop_pending
        end.flatten.collect do |line|
          if /ruby .*-launcher.rb/.match(line) then
            line.chomp.gsub(
              /-I [^ ]* */, ""
            ).gsub(
              /ruby +[^ ]*\/([^\/ \-]*-launcher).rb/, "\\1"
            ).gsub(
              / (node-launcher .*-n *( [^ ]+).*)| ([^ ]+)-launcher.*/, " \\2\\3"
            ).split(" ").values_at(10, 2, 3, 5)
          end
        end.compact.tabulate
        puts log;
      end
    end
  end
end
