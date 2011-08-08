#!/usr/bin/env ruby

$tests = [
  "Core Compiler" => ["make test", "compiler"],
  "Queries:C++"   => ["test/scripts/query_test.rb -a", "queries_cpp"]
];

$time = 3;# :00 AM
$logdir = "~/Dropbox/DBT_Nightly"

next_test_time = Time.now()
next_test_time -= (next_test_time.hour - $time) * 60 * 60 + 
                   next_test_time.min * 60 + 
                   next_test_time.sec;

loop do
  
  puts "#{Time.now}: Updating Repository"
  system('svn up');
  puts "#{Time.now}: Compiling"
  system('make');
  puts "#{Time.now}: Running Tests"
  $tests.each do |name => cmd|
    puts "#{Time.now}: Running Test: #{t}"
    IO.popen(cmd[0]) do |out|
      if(Process.wait(out.pid) != 0) then
        logfile = "#{$logdir}/#{next_test_time}_#{cmd[1]}"
        "  ... error.  Logging to #{logfile}"
        File.open(logfile, "w+") do |log|
          log.puts(out.readlines.join(""));
        end
      end
    end
  end
  
  next_test_time += 60 * 60 * 24;
  
end
