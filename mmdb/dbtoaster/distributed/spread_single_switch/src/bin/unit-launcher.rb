
require 'unit';
require 'getoptlong';

Logger.default_level = Logger::INFO;
Logger.default_name = "sliceDBread Unit Test";

puts "=========== Initializing Unit Test ===========";

unit_test = UnitTestHarness.new;

opts = GetoptLong.new(
  [ "-q", "--quiet",   GetoptLong::NO_ARGUMENT ],
  [ "-v", "--verbose", GetoptLong::NO_ARGUMENT ],
  [ "-e", "--expect",  GetoptLong::REQUIRED_ARGUMENT ],
  [ "-c", "--log_caller",  GetoptLong::NO_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "-q", "--quiet"   then Logger.default_level = Logger::WARN;
    when "-v", "--verbose" then Logger.default_level = Logger::DEBUG;
    when "-e", "--expect"  then unit_test.expect(File.open(arg));
    when "-c", "--caller"  then Logger.default_name = nil;
  end
end

ARGV.each do |f|
  unit_test.setup(File.open(f));
end

puts "=========== Starting Nodes ==========="

unit_test.start();

Logger.info { "Waiting 1 sec for nodes to come up..." }
sleep 1;

puts "=========== Executing Node Dump ==========="

unit_test.dump();

puts "=========== Running Test ==========="

if unit_test.run() then
  puts "=========== Test Failed! ==========="
  exit -1;
else 
  puts "=========== Test Passed! ==========="
end
