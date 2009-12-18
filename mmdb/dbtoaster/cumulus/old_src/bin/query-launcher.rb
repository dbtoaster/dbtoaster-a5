
require 'thrift_compat';
require 'ok_mixins'
require 'query'
require 'config'
require 'getoptlong'
require 'readline'

config = Config.new;

stty_save = `stty -g`.chomp
trap('INT') { system('stty', stty_save); exit }
Logger.default_level = Logger::INFO;
Logger.default_name = nil;

$loopquery = nil;

GetoptLong.new(
  *(
    Config.opts << 
    [ "-r", "--randomquery", GetoptLong::REQUIRED_ARGUMENT ] <<
    [ "-l", "--loopquery"  , GetoptLong::REQUIRED_ARGUMENT ] <<
    [ "-q",                  GetoptLong::NO_ARGUMENT ]
  )
).each do |opt, arg|
  case opt
    when "-r", "--randomquery" then
    when "-l", "--loopquery"   then $loopquery = arg;
    when "-q"                  then Logger.default_level = Logger::WARN;
    else config.parse_opt(opt, arg);
  end
end

ARGV.each do |file|
  config.load(File.open(file));
end

engine = MapQuery.new(config, 1, config.map_keys(1).collect { |k| k.downcase });

if $loopquery then
  trials = 0; step_count = 0;
  result = nil;
  step = time = Time.now;
  loop do
    result = engine.interpret($loopquery);
    trials += 1;
    if (trials += 1) % 10 == 0 then
      now = Time.now;
      puts "Query Engine: " + trials.to_s + " queries; " + ((now - time).to_f / trials.to_f).to_s + " avg time per query; " + ((now - step).to_f / (trials - step_count)).to_s + " windowed time per query; last result: " + result.to_s;
      if (now - step) < 20 then
        puts "Query Engine going too fast; delaying for " + (20 - (now - step)).to_s + " sec";
        sleep(20 - (now - step));
        step = Time.now;
        time += step - now;
      else
        step = now;
      end
      step_count = trials;
    end
  end
else
  puts "Aggregate Keys: " + engine.keys.join(", ");
  while line = Readline.readline("> ", true)
    puts engine.interpret(line).to_s;
  end
end


#puts(MapNode::Client.connect("localhost").aggreget( [ MapEntry.make(1, [-1]) ], AggregateType::AVG ).collect { |e, v| e.to_s + " => " + v.to_s }.join("\n"))
