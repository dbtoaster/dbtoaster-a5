#!/usr/bin/env ruby

require 'net/smtp'

query_cnt = Hash.new { |h,k| h[k] = 0 }


$log_targets = [
  {
    :type     => :email,
    :address  => "oliver.kennedy@epfl.ch",
    :detail   => :status
  },
  {
    :type     => :email,
    :address  => "yanif@cs.jhu.edu",
    :detail   => :error_detail
  },
  {
    :type     => :growl,
    :sticky   => false,
    :appname  => "DBTNightly",
    :detail   => :error
  },
  {
    :type     => :file,
    :dir      => "/Users/xthemage/Dropbox/DBT_Nightly",
    :detail   => :full
  },
  {
    :type     => :console,
    :detail   => :status
  }
];

$time = 3;# :00 AM

class BareLogger
  attr_reader :detail;
  
  def initialize(info)  
    @detail = BareLogger.detail_level(info[:detail]);
  end
  
  def BareLogger.detail_level(detail)
    case detail
      when :full         then 0
      when :status       then 1
      when :error_detail then 2
      when :error        then 3
                         else 0
    end
  end
  
  def flush
    #optional subclass override
  end
  
  def msg(msg, level = :status)
    self.handle_msg msg if (BareLogger.detail_level level) >= @detail;
  end
end

class BufferedLogger < BareLogger
  def initialize(info)
    super
    @buffer = [];
  end
  
  def handle_msg(msg)
    @buffer.push msg
  end
  
  def flush
    self.handle_flush(@buffer.join("\n")) if @buffer.length > 0;
    @buffer = [];
  end
end

class ConsoleLogger < BareLogger
  def handle_msg(msg)
    puts msg
  end
end

class FileLogger < BufferedLogger
  def initialize(info)
    super
    @dir = info[:dir];
    @buffer = [];
    puts "Setting up File Logger writing to #{@dir}"
  end
  
  def handle_flush(data)
    File.open("#{@dir}/#{Time.now.to_i}.log", "w+") do |f|
      f.puts data
    end
  end
end

class EmailLogger < BufferedLogger
  
  def initialize(info)
    super
    @address = info[:address];
    puts "Setting up Email Logger for #{@address}"
  end
  
  def handle_flush(data)
    sender  = "xthemage@mac.com"
    full    = "DBT Nightly Tester"
    subject = "DBT Nightly Test Report for #{Time.now}"
    IO.popen("sendmail -f \"#{sender}\" -F \"#{full}\" #{@address}", "w+") do |sm|
      sm.puts("Subject: #{subject}");
      sm.puts("To: #{@address}");
      sm.puts("");
      sm.puts(data);
    end
  end
end

class GrowlLogger < BareLogger

  def initialize(info)
    super
    @flags = " "
    @flags += "-s " if(info.has_key?(:sticky) && info[:sticky]);
    @flags += "-n #{info[:appname]}" if info.has_key? :appname;
    puts "Setting up Growl Logger"
  end
  
  def handle_msg(msg)
    IO.popen("growlnotify#{@flags} \"DBToaster Test\"", "w+") do |growl|
      growl.print(msg)
    end
  end

end    

$loggers = $log_targets.map do |target|
  case target[:type]
    when :email   then EmailLogger.new   target
    when :growl   then GrowlLogger.new   target
    when :console then ConsoleLogger.new target
    when :file    then FileLogger.new    target
    else puts "Unknown log type: #{target[:type]}"
  end
end

def msg(m, level = :status, prefix = "#{Time.now}: ") 
  $loggers.each { |l| l.msg("#{prefix}#{m}", level); };
end

def flush_logs
  $loggers.each { |l| l.flush; };
end


next_test_time = Time.now()
next_test_time -= (next_test_time.hour - $time) * 60 * 60 + 
                   next_test_time.min * 60 + 
                   next_test_time.sec;

loop do
  
  next_test_time += 60 * 60 * 24;
  msg "Sleeping until #{next_test_time}";
  sleep(next_test_time - Time.now) while(Time.now < next_test_time);
  
  msg "Updating Repository"
  system('svn up');
  
  msg "Reloading Test Suite"
  $tests = [
    {
      :name  => "Compiler Internals",
      :cmd   => "make test",
      :short => "internals"
    }
  ] + File.open("test/nightly_workload") do |f|
    f.readlines.map { |l| l.chomp }.delete_if { |l| l == "" }.
      map do |l| 
        shortname = "#{l.sub(/^ */,"").split(/ /)[0]}"
        query_cnt[shortname] = qid = query_cnt[shortname] + 1;
        {
          :name  => "Query '#{shortname}' test #{qid} (C++)",
          :cmd   => "test/scripts/query_test.rb #{l}",
          :short => shortname
        }
      end
  end
  
  msg "Compiling"
  system('make clean; make');
  msg "Running Tests"
  $tests.each do |t|
    msg "Running Test: #{t[:name]} (#{t[:cmd]})"
    IO.popen(t[:cmd]) do |out|
      log_data = out.readlines.join("");
      pid,status = Process.wait2(out.pid);
      if(status.to_i != 0) then
        msg "Error with '#{t[:name]}'", :error
        msg log_data, :error_detail, ""
      else
        msg log_data, :full, ""
      end
    end
  end
  
  flush_logs
end
