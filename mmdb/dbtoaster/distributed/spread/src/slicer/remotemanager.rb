require 'thread'

class RemoteProcess
  attr_reader :ready, :log;
  attr_writer :ready;
  
  def initialize(cmd, host, queue)
    @cmd, @host = cmd, host;
    @log = Queue.new if queue;
    
    process = IO.popen("ssh -t -t " + @host, "w+");
    @thread = Thread.new(process, @cmd, @log) do |ssh, cmd, log|
      Logger.info { "SSH pid " + ssh.pid.to_s + " starting: " + cmd.chomp; }
      ssh.write(cmd.chomp + " 2>&1\n");
      ssh.each do |line|
        if @log then
          log.push(line)
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
  end
  
  def status
    if @thread.status then true else false end;
  end
end