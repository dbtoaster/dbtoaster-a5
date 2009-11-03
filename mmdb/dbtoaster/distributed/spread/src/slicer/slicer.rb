
require 'ok_mixins'
require 'thread'

class SlicerProcess
  attr_reader :ready, :queue;
  attr_writer :ready;
  
  def initialize(cmd, host, queue = false)
    @cmd, @host = cmd, host;
    @queue = Queue.new if queue;
  end
  
  def start
    process = IO.popen("ssh -t -t " + @host, "w+");
    thread = Thread.new(process, @cmd, self) do |ssh, cmd, manager|
      Logger.info { "SSH pid " + ssh.pid.to_s + " starting: " + cmd.chomp; }
      ssh.write(cmd.chomp + " 2>&1\n");
      ssh.each do |line|
        manager.ready = true if line.include? "Starting #<Thrift::NonblockingServer";
        if @queue then
          queue.push(line)
        else
          print line;
        end
      end
      Logger.info { "SSH pid " + ssh.pid.to_s + " complete"; }
    end
    at_exit do 
      Process.kill("HUP", process.pid); thread.join; 
      Logger.info { "Killed SSH to " + @host; }
    end
    self;
  end
  
  def SlicerProcess.base_path
    "dbtoaster/distributed/spread";
  end
end

class SlicerMonitor
  def initialize(hosts)
    @connections = hosts.collect do |h|
      SlicerProcess.new("export PS1='';\nwhile true; do sleep 5; ps auxww | grep ruby | grep -v grep; done", h, true);
    end
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
              /^.*(oak5.+)$/, "\\1" # XXX temporary hack to get rid of the bash prompt at the start
            ).gsub(
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
      ).start;

    sleep 0.1 while servers.find { |s| !s.ready };

    monitor = SlicerMonitor.new(@nodes.collect { |n| n.host } << @switch);

    client_cmd = 
      SlicerProcess.base_path + "/bin/client.sh -q -s -l 200 " + 
      @transforms.collect { |t| "-t '" + t + "'" }.join(" ") + " " +
      @projections.collect { |pr| "-u '" + pr + "'"}.join(" ") + " " +
      "-h " + @source
    
    query_cmd = 
      SlicerProcess.base_path + "/bin/query.sh -l sum " +
        @nodes.collect { |n| "-n " + n.to_s }.join(" ") + " " +
        @config.join(" ");

    Logger.info { "Servers started; starting clients" }
    monitor.start
    SlicerProcess.new(client_cmd, @switch).start;
    SlicerProcess.new(query_cmd, @switch).start;
  end

end