
require 'thrift'
require 'map_node'
require 'node'
require 'compiler'
require 'spread_types'

class UnitTestNode
  attr_reader :handler, :name, :port;
  
  def initialize(port, name, config = Array.new)
    @name, @port, @thread = name, port, nil;
    
    @handler = MapNodeHandler.new();
    config.each do |f|
      handler.setup(File.open(f));
    end
    @processor = MapNode::Processor.new(handler)
    @transport = Thrift::ServerSocket.new(port);
    @transportFactory = Thrift::BufferedTransportFactory.new();
    @server = Thrift::SimpleServer.new(@processor, @transport, @transportFactory);
    puts "Prepared Node " + @name.to_s
  end
  
  def start
    @thread = Thread.new(@server, @name) do |server, name|
      puts "Starting Node " + @name.to_s;
      begin
        server.serve;
      rescue Exception => e;
        puts "Error at Node " + name.to_s + ": " + e.to_s;
        puts e.backtrace.join("\n");
        exit;
      end
    end
  end
  
  def stop
    @thread.exit;
  end
  
end

###################################################

class UnitTestFetchStep
  def initialize(source, destination, target, cmdid = 0)
    @source, @destination, @cmdid = NodeID.new, NodeID.new, cmdid;
      @source.host = "localhost";
      @source.port = source.port;
      @destination.host = "localhost";
      @destination.port = destination.port;
    @target = target.collect do |t|
      t = t.split(":");
      e = Entry.new;
        e.source = t[0].to_i;
        e.key = t[1].to_i;
        e.version = unless t.size < 3 then t[2] else 1; end
        e.node = @source;
      e;
    end
  end
  
  def fire
    node = MapNodeInterface.new(@source.host, @source.port);
    node.fetch(@target, @destination, @cmdid);
    node.close();
  end
  
  def to_s
    @target.each do |e|
      e.source.to_s + "[" + e.key.to_s + "(" + e.version.to_s + ")]";
    end.join(", ") + " @"
      @source.host.to_s + ":" + @source.port.to_s + " -> "
      @destination.host.to_s + ":" + @destination.port.to_s;
  end
end

###################################################

class UnitTestHarness 
  def initialize()
    @nodes = Array.new;
    @testcmds = Array.new;
    @indexmap = Hash.new;
    @puttemplates = Hash.new;
  end
  
  def addNode(name = @nodes.size.to_s)
    node = UnitTestNode.new(52982 + @nodes.size, name);
    @indexmap[name] = @nodes.size.to_i;
    puts name.to_s + " : " + @indexmap[name].to_s;
    @nodes.push(node);
    node;
  end
  
  def start()
    @nodes.each do |node|
      node.start();
    end
  end
  
  def setup(input)
    cmdbuffer = Array.new;
    linebuffer = nil;
    nodename = nil;
    input.each do |line|
      cmd = line.scan(/[^ \t\n\r]+/);
      case cmd[0]
        when "node" then 
          addNode(nodename).handler.setup(linebuffer) unless linebuffer == nil;
          if cmd.size > 1 then nodename = cmd[1] else nodename = @nodes.size.to_s; end
          linebuffer = Array.new;
        when "put" then
          cmd.shift; 
          @puttemplates[cmd.shift] = cmd.join(" ");
        when "fetch" then cmdbuffer.push(cmd);
        else 
          linebuffer.push(line) unless linebuffer == nil;
      end
    end
    addNode(nodename).handler.setup(linebuffer) unless linebuffer == nil;
    @nodes.each do |node|
      puts "Loading put templates into Node " + node.name;
      @puttemplates.each_pair do |id, cmd|
        node.handler.createPutTemplate(id, cmd);
      end
    end
    cmdbuffer.each do |cmd|
      case cmd[0]
        when "fetch" then
          cmd.shift; 
          puts "Fetch: " + cmd.join(" ") + " / " + @indexmap[cmd[0]].to_s + ", " + @indexmap[cmd[1]].to_s;
          @testcmds.push(UnitTestFetchStep.new(@nodes[@indexmap[cmd.shift]], @nodes[@indexmap[cmd.shift]], cmd));
      end
    end
  end
  
  def dump()
    @nodes.each do |node|
      puts "---" + node.name + "---";
      puts node.handler.dump();
    end
    puts "--------";
  end
  
  def run()
    @testcmds.each do |cmd|
      puts "Executing: " + cmd.to_s;
      cmd.fire;
    end
  end
  
end
