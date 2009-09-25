
require 'unit';

puts "Initializing Unit Test..."

unitTest = UnitTestHarness.new;
ARGV.each do |f|
  unitTest.setup(File.open(f));
end

puts "Starting Nodes...";

unitTest.start();
sleep 1;

puts "Running Test...";

unitTest.run();

sleep;