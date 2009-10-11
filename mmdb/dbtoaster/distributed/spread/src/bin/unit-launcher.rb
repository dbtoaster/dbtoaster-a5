
require 'unit';

Logger.default.level = Logger::INFO

puts "=========== Initializing Unit Test ==========="

unitTest = UnitTestHarness.new;
ARGV.each do |f|
  unitTest.setup(File.open(f));
end

puts "=========== Starting Nodes ===========";

unitTest.start();

Logger.info("Waiting 1 sec for nodes to come up...", "unit-launcher.rb");
sleep 1;

puts "=========== Executing Node Dump ===========";

unitTest.dump();

puts "=========== Running Test ===========";

unitTest.run();

sleep;