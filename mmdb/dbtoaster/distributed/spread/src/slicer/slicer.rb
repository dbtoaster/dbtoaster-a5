
require 'ok_mixins'

class SlicerProcess
  attr_reader :ready;
  attr_writer :ready;
  
  def initialize(cmd, host)
    @cmd, @host = cmd, host, nil;
  end
  
  def start
    process = IO.popen("ssh -t -t " + @host, "w+");
    thread = Thread.new(process, @cmd, self) do |ssh, cmd, manager|
      Logger.info { "SSH pid " + ssh.pid.to_s + " starting: " + cmd.chomp; }
      ssh.write(cmd.chomp + " 2>&1\n");
      ssh.each do |line|
        manager.ready = true if line.include? "Starting #<Thrift::NonblockingServer";
        print line;
      end
      Logger.info { "SSH pid " + ssh.pid.to_s + " complete"; }
    end
    at_exit do 
      Logger.info { "Killing SSH to " + @host + " and waiting..."; }
      Process.kill("HUP", process.pid); thread.join; 
      Logger.info { "... done"; }
    end
    self;
  end
  
  def SlicerProcess.base_path
    "dbtoaster/distributed/spread";
  end
end

class SlicerNode
  attr_reader :name, :host, :port, :path
  def initialize(cmd)
    match = /node *([a-zA-Z0-9\+-_]+) *@ *([a-zA-Z0-9.\-]+):([0-9]+)/.match(cmd)
    raise "Invalid node command: " + cmd unless match;
    match, @name, @host, @port = match.to_a;
  end
  
  def process(config)
    SlicerProcess.new(
      SlicerProcess.base_path + "/bin/node.sh" + 
      " -n " + @name.to_s + 
      " -p " + @port.to_s + 
      " " + config.join(" "), 
      @host.to_s
    );
  end
  
  def to_s
    @name + "@" + @host + ":" + port;
  end
end

class Slicer
  attr_reader :mode;
  attr_writer :mode;
  
  def initialize()
    @processes = Array.new;
    @nodes = Array.new;
    @config = Array.new;
    @transforms = Array.new;
    @projections = Array.new;
    @switch = "localhost";
    @mode = :quiet;
    @source = nil;
  end
  
  def setup(input)
    input.each do |line|
      line.chomp!;
      next if line[0] == "#"[0];
      case line.split(/ +/)[0];
        when "node" then @nodes.push(SlicerNode.new(line));
        when "config" then @config.push(SlicerProcess.base_path + "/data/" + line.split(/ +/)[1]);
        when "switch" then @switch = line.split(/ +/)[1]; raise "Invalid switch cmd: " + line unless @switch;
        when "transform" then @transforms.push(line.gsub(/^transform */, ""));
        when "project" then @projections.push(line.gsub(/^project */, ""));
        when "source" then @source = line.gsub(/^source */, "");
      end
    end
  end
  
  def mode_flag
    case @mode
      when :quiet   then "-q "
      when :verbose then "-v "
      else ""
    end
  end
  
  def start
    @config << self.mode_flag
    servers = 
      @nodes.collect do |n|
        n.process(@config).start;
      end <<
      SlicerProcess.new(
        SlicerProcess.base_path + "/bin/switch.sh " +
        @nodes.collect { |n| "-n " + n.to_s }.join(" ") + " " +
        @config.join(" "),
        @switch
      )#.start;
    sleep 0.1 while servers.find { |s| !s.ready };
    Logger.info { "Servers started; starting client" }
    SlicerProcess.new(
      SlicerProcess.base_path + "/bin/client.sh -q " + 
      @transforms.collect { |t| "-t '" + t + "'" }.join(" ") + " " +
      @projections.collect { |pr| "-u '" + pr + "'"}.join(" ") + " " +
      "-h " + @source,
      @switch
    )#.start;
  end

end