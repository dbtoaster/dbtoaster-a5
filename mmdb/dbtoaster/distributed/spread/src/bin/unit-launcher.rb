
require 'unit';

Logger.default.level = Logger::INFO;

puts "=========== Initializing Unit Test ==========="

unit_test = UnitTestHarness.new;
ARGV.each do |f|
  case f
    when "-v" then Logger.default.level = Logger::DEBUG;
    else unit_test.setup(File.open(f));
  end
end

puts "=========== Starting Nodes ===========";

unit_test.start();

Logger.info("Waiting 1 sec for nodes to come up...", "unit-launcher.rb");
sleep 1;

puts "=========== Executing Node Dump ===========";

unit_test.dump();

puts "=========== Running Test ===========";

unit_test.run();

sleep;