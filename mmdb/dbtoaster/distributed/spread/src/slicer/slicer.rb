
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

  def initialize(config_file, verbosity = :quiet)
    @config_file = config_file;
    @config = Config.new();
      @config.load(File.open(config_file));
    @monitor_intake = Queue.new;
    @spread_path = @config.spread_path;
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
      puts "Leaving monitor thread!";
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
          at_exit { Process.kill("HUP", pid) };
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
    @monitor_intake.push([:client, SlicerNode::Client.connect(target)]);
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
        "cd #{spread_path}; ./bin/slicer.sh --serve #{config_file}",
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
      Logger.warn { "Waiting for slicer to spawn on : #{host}" }
      @ready = /====> Server Ready <====/.match(@process.log.pop) until @ready
      Logger.warn { "Slicer spawned on : #{host}" }
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
    def Processor.listen(config_file, verbosity = :quiet, port = 52980);
      handler = SlicerNodeHandler.new(config_file, verbosity);

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
  def initialize(nodes, interval = 5)
    @nodes = nodes.collect do |node| 
      [ 
        conn = SlicerNode::Client.connect(node),
        out_blocker = Queue.new,
        in_blocker = Queue.new,
        Thread.new(node, conn, out_blocker, in_blocker) do |n, c, ob, ib|
          loop do
            ib.pop;
            ob.push(c.poll_stats.split("\n").collect { |l| "#{n}  #{l}"}.join("\n"))
          end
        end
      ]
    end
    @monitor = Thread.new(@nodes, interval) do |nodes, interval|
      loop do
        sleep interval;
        nodes.each { |c, ob, ib| ib.push(1) }
        log = nodes.collect { |c, ob, ib| ob.pop.split("\n") }.flatten.collect do |line|
          if /ruby .*-launcher.rb/.match(line) then
            line.chomp.gsub(
              /-I [^ ]* */, ""
            ).gsub(
              /ruby +[^ ]*\/([^\/ \-]*)-launcher.rb.*/, "\\1"
            )
          end
        end
        puts log.join("\n");
      end
    end
  end
end


