require 'thread'

class RemoteProcess
  attr_reader :ready, :log;
  attr_writer :ready;
  
  include CLogMixins;
  
  def initialize(cmd, host, queue)
    @cmd, @host = cmd, host;
    @log = Queue.new if queue;
    process = IO.popen("ssh -t -t " + @host, "w+");
    debug { "Starting remote process: #{cmd}" }
    @thread = Thread.new(process, @cmd, @log) do |ssh, cmd, log|
      info { "SSH pid " + ssh.pid.to_s + " starting: " + cmd.chomp; }
      ssh.write(cmd.chomp + " 2>&1\n");
      ssh.each do |line|
        if @log then
          log.push(line)
        end
        print line;
      end
      debug { "SSH pid " + ssh.pid.to_s + " complete"; }
    end
    at_exit do 
      Process.kill("HUP", process.pid); 
      debug { "Killed SSH to " + @host; }
    end
  end
  
  def status
    if @thread.status then true else false end;
  end
end