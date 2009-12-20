
require 'getoptlong';
require 'config/config';

$stdout.sync = true;
$interactive = true;
$verbose = false;
$test = false;
$cols = Hash.new;
$transforms = Hash.new;
$input = $stdin
$stats_every = -1;
$ratelimit = nil;
$upfront_cols = Array.new;

GetoptLong.new(
  [ "--quiet"      , "-q", GetoptLong::NO_ARGUMENT ],
  [ "--use"        , "-u", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--transform"  , "-t", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--test"       , "-e", GetoptLong::NO_ARGUMENT ],
  [ "--tpch-stream", "-h", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--verbose"    , "-v", GetoptLong::NO_ARGUMENT ],
  [ "--stats"      , "-s", GetoptLong::OPTIONAL_ARGUMENT ],
  [ "--dump"       , "-d", GetoptLong::OPTIONAL_ARGUMENT ],
  [ "--ratelimit"  , "-l", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--upfront"    ,       GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt, arg|
  case opt
    when "--quiet", "-q" then 
      $interactive = false; $verbose = false;
    
    when "--use", "-u" then 
      match = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(arg);
      raise "Invalid use argument: " + arg unless match;
      $cols[match[1]] = match[2].split(/ *, */).collect { |i| i.to_i };
      
    when "--transform", "-t" then
      match = / *([a-zA-Z\-+_]*) *\[([0-9,]+)\](~\/([^\/]*)\/([^\/]*)\/|<([a-z]?)([0-9, ]+)|#|%([0-9]+)|@([0-9]+)\/([0-9]+))/.match(arg)
      raise "Invalid transform argument: " + arg unless match;
      $transforms.assert_key(match[1]){ Hash.new }.assert_key(match[2].to_i){ Array.new }.push(
        case match[3][0]
          when "~"[0] then [:regex, Regexp.new(match[4]), match[5]]
          when "<"[0] then [:cmp, match[6], match[7].split(/ *, */)]
          when "#"[0] then [:intify, Hash.new, 0, match[1]+":"+match[2]]
          when "%"[0] then [:mod, match[8].to_i]
          when "@"[0] then [:rotate, match[9].to_i, match[10].to_i]
          else raise "Error: Unknown transform type: " + match[3]
        end)
        
    when "--test", "-e" then
      $test = true; $verbose = true;
    
    when "--verbose", "-v" then
      $verbose = true;
      
    when "--upfront" then
      puts "Starting by fully loading all " + arg + " rows";
      $upfront_cols.push(arg);
      
    when "--tpch-stream", "-h" then
      puts "Generating stream command";
      cmd = $config.spread_path + "/bin/tpch.sh -d " + arg + " " + 
        $cols.keys.collect { |t| "--" + t + ($upfront_cols.include?(t) ? " upfront" : "") }.join(" ");
      puts "Reading: " + cmd
      $input = IO.popen(cmd, "r+")
    
    when "--stats", "-s" then
      $stats_every = (if arg.nil? || arg == "" then 1000 else arg end).to_i;
      puts "Will print stats every : " + $stats_every.to_s;
    
    when "--dump", "-d" then
      unless arg.nil? || arg == "" then
        arg = arg.split(":");
        dest = java.net.InetSockAddress(arg[0], if arg.size > 1 then arg[1] else 52982 end);
        puts Java::org::dbtoaster::cumulus::node::MapNode::getClient(dest).dump
      else
        dest = java.net.InetSockAddress("localhost", 52981);
        puts Java::org::dbtoaster::cumulus::chef::ChefNode::getClient(dest).dump
      end
      exit(0);
    
    when "--ratelimit", "-l" then
      $ratelimit = arg.to_f;
  end
end

$ratelimit = $stats_every.to_f / $ratelimit if $ratelimit;

switch_addr = java.net.InetSocketAddress.new(`hostname`.chomp, 52981);
switch = Java::org::dbtoaster::cumulus::chef::ChefNode::getClient(switch_addr) unless $test;

def compare_date(indices, params)
  indices = indices.collect{ |i| params[i.to_i] };
  last = indices.shift.split("-").collect { |i| i.to_s };
  raise "Not a date: " + last.join("-") unless last.size == 3;
  indices.each do |i|
    i = i.split("-").collect { |c| c.to_s };
    raise "Not a date: " + i.join("-") unless i.size == 3;
    (0...3).each do |c|
      return false if last[c] > i[c];
    end
    last = i;
  end
  return true;
end

$starttime = nil;
$count = 0;
print "\n>> " if $interactive;
$input.each do |line|
  $starttime = Time.now unless $starttime;
  if line.chomp == "dump" then
    puts switch.dump();
  else
    args = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(line);
    unless args then
      puts "ERROR: can not parse '" + line.chomp + "'";
      next;
    end
    begin
      table = args[1];
      params = args[2].split(/ *, */);
      
      if $transforms.has_key? table then
        $transforms[table].each_pair do |col,tlist|
          tlist.each do |t|
            params[col.to_i] = 
              case t[0]
                when :regex then params[col.to_i].gsub(t[1], t[2]);
                when :cmp   then
                  case t[1]
                    when "d" then
                      if compare_date(t[2], params) then "1" else "0" end;
                  end
                when :intify then
                  t[1].assert_key(params[col.to_i]) { puts "Domain of " + t[3] + " now at " + (t[2]+1).to_s; t[2] += 1; }.to_s;
                when :mod then
                  (params[col.to_i].to_i % t[1].to_i).to_s;
                when :rotate then
                  ((params[col.to_i].to_i % t[1].to_i) * (t[2].to_i / t[1].to_i) + (params[col.to_i].to_i / t[1].to_i).to_i).to_s;
                  
              end
          end
        end
      end
      
      if $cols.has_key? table then
        tmp_params = params;
        params = Array.new;
        $cols[table].each do |i|
          params.push(tmp_params[i]);
        end
      end
      success = false
      #backoff = 0.125
      until success do
        #begin
          switch.update(table, params) unless $test;
          success = true;
        #rescue SpreadException => e;
        #  raise e unless e.retry;
        #  puts "Backoff requested, sleeping for #{backoff}"
        #  sleep backoff;
        #  backoff *= 2;
        #end
      end
      puts table+"("+params.join(", ")+")" if $verbose;
    rescue SpreadException => e
      puts "Error: " + e.to_s;
    end
  end
  print "\n>> " if $interactive;
  if $stats_every >= 0 then
    $count += 1;
    if $count % $stats_every == 0 then
      diff = (Time.now - $starttime)
      puts diff.to_s + " seconds; " + ($stats_every.to_f / diff.to_f).to_s + " updates per sec; " + $count.to_s + " updates total";
      puts switch.dump if ($verbose && !$test);
      $starttime = Time.now;
      if $ratelimit then
        $limittime = $starttime + $ratelimit unless $limittime;
        diff = $limittime - Time.now;
        sleep(diff) if diff > 0;
        $limittime = Time.now + $ratelimit;
      end
    end
  end
end

