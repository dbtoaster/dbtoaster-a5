require 'getoptlong'
require 'config/config'

$config_file = nil

$hadoop_cmd = "hadoop"
$input_path = nil
$output_path = nil
$default_chunk_size = 10000
$chunk_sizes = Hash.new

$stats_every = -1;

opts = GetoptLong.new(
  [ "-c", "--config-file",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-l", "--localprops",        GetoptLong::REQUIRED_ARGUMENT ],
  [ "-i", "--source-path",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-o", "--hadoop-dfs-path",   GetoptLong::REQUIRED_ARGUMENT ],
  [ "-s", "--chunk-size",        GetoptLong::REQUIRED_ARGUMENT ],
  [ "-h", "--hadoop-bin-path",   GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt, arg| 
  case opt
    when "-c", "--config-file"     then $config_file = arg; 
    when "-l", "--localprops"      then $config.load_local_properties(arg)
    when "-i", "--source-path"     then $input_path = arg;
    when "-o", "--hadoop-dfs-path" then $output_path = arg;
    when "-s", "--chunk-size"      then
      if arg =~ /,/ then
        $chunk_sizes = Hash[*arg.split(",").collect { |s| ts=s.split(":"); [ts[0], ts[1].to_i] }.flatten]
      else
        $default_chunk_size = arg.to_i;
      end
    when "-h", "--hadoop-bin-path" then $hadoop_cmd = File.join(arg, $hadoop_cmd); 
  end
end

if $config_file.nil? then 
  puts "No config file specified"
  exit(1)
end

$config.load($config_file)
$input_path = $config.client_debug["sourcedir"] if $input_path.nil?;
$output_path = $input_path if $output_path.nil?;

$rel_cols = Hash.new
$transforms = Hash.new

$input = nil
$outputs = Hash.new

$config.client_debug["projections"].each do |p|
  match = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(p);
  raise "Invalid projection: " + p unless match;
  puts "Project: #{match[1]} #{match[2]}"
  $rel_cols[match[1]] = match[2].split(/ *, */).collect { |i| i.to_i };
end

$config.client_debug["transforms"].each do |t|
  match = / *([a-zA-Z\-+_]*) *\[([0-9,]+)\](~\/([^\/]*)\/([^\/]*)\/|<([a-z]?)([0-9, ]+)|#|%([0-9]+)|@([0-9]+)\/([0-9]+))/.match(t)
  raise "Invalid transform: " + t unless match;
  $transforms.assert_key(match[1]){ Hash.new }.assert_key(match[2].to_i){ Array.new }.push(
    case match[3][0]
      when "~"[0] then [:regex, Regexp.new(match[4]), match[5]]
      when "<"[0] then [:cmp, match[6], match[7].split(/ *, */)]
      when "#"[0] then [:intify, Hash.new, 0, match[1]+":"+match[2]]
      when "%"[0] then [:mod, match[8].to_i]
      when "@"[0] then [:rotate, match[9].to_i, match[10].to_i]
      else raise "Error: Unknown transform type: " + match[3]
    end)
end

# Set up chunk sizes.
$rel_cols.keys.each { |t| $chunk_sizes[t] = $default_chunk_size unless $chunk_sizes.key?(t) }

# Open input subprocess for reading tpch data
CLog.debug { "Generating stream command" }
cmd = $config.spread_path + "/bin/tpch.sh -d " + $input_path + " " + 
  $rel_cols.keys.collect { |t| "--" + t + " upfront"}.join(" ");
CLog.debug { "Reading: " + cmd }
$input = IO.popen(cmd, "r+");

# Open output subprocesses for writing data to HDFS
$chunk_counters = Hash[*$rel_cols.keys.collect { |t| [t, 0] }.flatten]
$chunk_lines = Hash[*$rel_cols.keys.collect { |t| [t, 0] }.flatten]
$outputs = Hash[*$rel_cols.keys.collect { |t| [t, nil] }.flatten]

def next_chunk_file(table)
  chunk_dir = File.join(table, "chunks");
  chunk_file = "#{table}.#{$chunk_counters[table]}"

  hdfs_path = File.join($output_path, chunk_dir);
  hdfs_file = File.join(hdfs_path, chunk_file)
  hdfs_test_cmd = "#{$hadoop_cmd} fs -test -d #{hdfs_path}"
  hdfs_mkdir_cmd = "#{$hadoop_cmd} fs -mkdir #{hdfs_path}"
  hdfs_put_cmd = "#{$hadoop_cmd} fs -put - #{hdfs_file}";
  
  unless system(hdfs_test_cmd) then
    CLog.info { "Creating HDFS directory #{hdfs_path}"}
    system(hdfs_mkdir_cmd) 
  end
  $outputs[table].close unless $outputs[table].nil?;
  $outputs[table] = IO.popen(hdfs_put_cmd, "w+");
  $outputs[table].sync = true;
  $chunk_counters[table] += 1;
  CLog.info { "Created chunk file for #{table}: #{hdfs_file}" }
end

$starttime = nil;
$count = 0;
$input.each do |line|
  $starttime = Time.now unless $starttime;
  args = / *([a-zA-Z\-+_]+) *\(([^)]*)\)/.match(line);

  unless args then
    puts "ERROR: can not parse '" + line.chomp + "'";
    next;
  end

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
  
  if $rel_cols.has_key? table then
    tmp_params = params;
    params = Array.new;
    $rel_cols[table].each { |i| params.push(tmp_params[i]); }
  end

  # Create chunks on HDFS
  next_chunk_file(table) if ( ($chunk_lines[table] % $chunk_sizes[table]) == 0 );
  $outputs[table].write(params.join("|"));
  $chunk_lines[table] += 1;

  if $stats_every >= 0 then
    $count += 1;
    if $count % $stats_every == 0 then
      diff = (Time.now - $starttime)
      puts "tpch_hadoop: " + diff.to_s + " seconds; " + ($stats_every.to_f / diff.to_f).to_s + " records per sec; " + $count.to_s + " records total";
      $starttime = Time.now;
    end
  end
end

# Cleanup and report.
# Note: needs to sleep after flushing, before closing, because of double pipe.
$outputs.each_pair { |t,p| puts "Closing pipe for #{t}"; p.flush; sleep 2; p.close }
$chunk_lines.each_pair { |k,v| puts "#{k}: #{v} lines"}
  
sleep 5;
