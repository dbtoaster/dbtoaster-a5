
require 'thrift'
require 'map_node'
require 'node'
require 'compiler'
require 'spread_types'
require 'spread_types_mixins'

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
  
  def dump
    "---" + @name.to_s + "---\n" + @handler.dump;
  end
  
  def to_s
    @name.to_s
  end
end

###################################################

class UnitTestFetchStep
  def initialize(cmdid, source, destination, target)
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
        e.version = unless t.size < 3 then [t[2].to_i] else [1]; end
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
    @target.collect do |e|
      e.source.to_s + "[" + e.key.to_s + "(" + e.version.to_s + ")]"
    end.join(", ") + "@" +
      @source.host.to_s + ":" + @source.port.to_s + " -> " +
      @destination.host.to_s + ":" + @destination.port.to_s;
  end
end

class UnitTestPutStep
  def initialize(command, nodeMap, templateMap)
    parsed = 
      /([0-9]+) *<- *([0-9]+) *@ *([0-9a-zA-Z]+) *: * Template *([0-9]+) *\(([0-9a-zA-Z, ]*)\)/.match(command)
    raise SpreadException.new("Invalid put step string: " + command) if parsed == nil || parsed.to_a.size != 6;
    parsed = parsed.to_a;
    parsed.shift; #first result is the overall match
    
    @cmdid, @oldversion, @destport, @template, @params = 
      parsed[0], parsed[1], nodeMap[parsed[2]].port.to_i, parsed[3], Hash.new;
    
    templateParams = templateMap[@template.to_i].paramlist.clone;
    parsed[4].split(/, */).each do |param|
      @params[templateParams.shift] = param unless templateParams.size <= 0;
    end
  end
  
  def fire
    node = MapNodeInterface.new("localhost", @destport);
    node.put(@cmdid, @template, @oldversion, @params);
    node.close();
  end
  
  def to_s
    @cmdid.to_s + " <- " + @oldversion.to_s + " @ [localhost:" + @destport.to_s + "] : Template " + 
      @template.to_s + "(" + 
      @params.keys.collect do |key| key.to_s + ":" + @params[key].to_s end.join(", ") + ")";
  end
end

class UnitTestDumpStep
  def initialize(nodes)
    @nodes = nodes;
  end
  
  def fire
    @nodes.each do |unitNode|
      node = MapNodeInterface.new("localhost", unitNode.port);
      puts node.dump;
      node.close();
    end
  end
  
  def to_s
    "Dump: " + @nodes.join(", ");
  end
end

class UnitTestSleepStep
  def initialize(time)
    @time = time.to_i;
  end 
  
  def fire
    sleep @time;
  end
  
  def to_s
    "Sleep: " + @time.to_s + " s";
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
    @indexmap[name] = node;
    puts "New Node " + name.to_s + " : " + @nodes.size.to_s + " @ port " + node.port.to_s;
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
      #puts "GOT: " + line;
      if line[0] != '#' then 
        cmd = line.scan(/[^ \r\n]+/);
        case cmd[0]
          when "node" then 
            addNode(nodename).handler.setup(linebuffer) unless linebuffer == nil;
            if cmd.size > 1 then nodename = cmd[1] else nodename = @nodes.size.to_s; end
            linebuffer = Array.new;
          when "template" then
            cmd.shift; 
            @puttemplates[cmd.shift.to_i] = UpdateTemplate.new(cmd.join(" "));
          when "fetch", "put", "dump", "sleep" then cmdbuffer.push(cmd);
          else 
            linebuffer.push(line) unless linebuffer == nil;
        end
      end
    end
    addNode(nodename).handler.setup(linebuffer) unless linebuffer == nil;
    @nodes.each do |node|
      puts "Loading put templates into Node " + node.name;
      @puttemplates.each_pair do |id, cmd|
        node.handler.installPutTemplate(id, cmd);
      end
    end
    cmdbuffer.each do |cmd|
      case cmd[0]
        when "fetch" then
          cmd.shift; 
          puts "Fetch: " + cmd.join(" ");
          @testcmds.push(UnitTestFetchStep.new(cmd.shift.to_i, @indexmap[cmd.shift], @indexmap[cmd.shift], cmd));
        when "put" then
          cmd.shift;
          @testcmds.push(UnitTestPutStep.new(cmd.join(" "), @indexmap, @puttemplates));
        when "dump" then
          cmd.shift;
          if cmd.size > 0 then
            @testcmds.push(UnitTestDumpStep.new(cmd.collect do |node| @indexmap[node] end));
          else
            @testcmds.push(UnitTestDumpStep.new(@nodes));
          end
        when "sleep" then
          @testcmds.push(UnitTestSleepStep.new(cmd[1]));
      end
    end
  end
  
  def dump()
    @nodes.each do |node|
      puts node.dump;
    end
  end
  
  def run()
    @testcmds.each do |cmd|
      puts "Executing: " + cmd.to_s;
      cmd.fire;
    end
  end
  
end
