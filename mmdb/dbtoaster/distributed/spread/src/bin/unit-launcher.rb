
require 'unit';

Logger.default_level = Logger::INFO;
Logger.default_name = "Tosthaus Unit Test";

Logger.info { "=========== Initializing Unit Test ===========" }

unit_test = UnitTestHarness.new;
ARGV.each do |f|
  case f
    when "-v" then Logger.default_level = Logger::DEBUG;
    else unit_test.setup(File.open(f));
  end
end

Logger.info { "=========== Starting Nodes ===========" }

unit_test.start();

Logger.info { "Waiting 1 sec for nodes to come up..." }
sleep 1;

Logger.info { "=========== Executing Node Dump ===========" }

unit_test.dump();

Logger.info { "=========== Running Test ===========" }

unit_test.run();

sleep;