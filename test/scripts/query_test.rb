#!/usr/bin/env ruby
require "#{File.dirname($0)}/util.rb"
require "#{File.dirname($0)}/db_parser.rb"
require 'getoptlong'
require 'tempfile'

$dbt_path = "#{File.dirname($0)}/../.."
$dbt = "bin/dbtoaster"
$ocamlrunparam = "b,l=20M"
Dir.chdir $dbt_path

raise "DBToaster is not compiled" unless (File.exists? $dbt)

def results_file(path, delim = /,/, reverse = false)
  File.open(path).readlines.
    delete_if { |l| l.chomp == "" }.  
    map do |l|
      k = l.split(delim).map { |i| 
        case i 
            when /([0-9]+)\-([0-9]+)\-([0-9]+)/ then $1.to_i*10000+$2.to_i*100+$3.to_i;
            when /[\-\+]?[0-9]+\.[0-9]*e?[\-\+]?[0-9]*/ then i.to_f;
            when /[a-zA-Z][a-zA-Z0-9_# ]*/ then i;
            when /[\-\+]?[0-9]+/ then i.to_i;
            else i
        end 
      }
      [k, k.pop];
    end.map { |k,v| if reverse then [k.reverse,v] else [k,v] end }.to_h
end

def upcast_int_to_float(i)
  if i.is_a? Integer then i.to_f else i end
end

class GenericUnitTest
  attr_reader :runtime, :opts;

  def query=(q)
    @qname = q
    qdat = File.open("test/unit/queries/#{q}") do |f| 
      eval(f.readlines.join(""), binding) 
    end
    @dataset = if $dataset.nil? then "standard" else $dataset end
    @qpath = qdat[:path];
    
    raise "Invalid dataset" unless qdat[:datasets].has_key? @dataset;
    @toplevels = qdat[:datasets][@dataset][:toplevels].to_a.map do |name,info|
      info[:expected] =
        case info[:type]
          when :singleton then upcast_int_to_float(info[:expected])
          when :onelevel  then info[:expected].to_a.map do |k,v|
            [ k.map { |k_elem| upcast_int_to_float(k_elem) }, 
              upcast_int_to_float(v) ]
          end.to_h
        end
      [name, info]
    end.to_h
    @toplevels
    
    if qdat[:datasets][@dataset].has_key? :subs then    
      qfile = Tempfile.new(["query_test",".sql"]);
      subs = qdat[:datasets][@dataset][:subs]
      at_exit { qfile.close! }
      qfile.puts(
        File.open(@qpath) do |f| 
          f.readlines.map do |l|
            subs.fold(l) { |curr_l,s| curr_l.gsub(*s) };
          end.join(""); 
        end
      );
      qfile.flush;
      @qpath = qfile.path;
    end

    @compiler_flags =
      (qdat.has_key? :compiler_flags) ? qdat[:compiler_flags] : [];
  end
    
  def query
    File.open(@qpath) { |f| f.readlines.join("") };
  end
  
  def diff(e, r)
    if (e == r)                            then "Same"
    elsif e == nil && r.abs < $precision   then "Close"
    elsif r == nil && e.abs < $precision   then "Close"
    elsif e == nil                         then "Different"
    elsif r == nil                         then "Different"
    elsif ((e+r).abs > 1) && (((e-r)/(e+r)).abs < $precision)
                                           then "Close"
    elsif ((e+r).abs <= 1) && ((e-r) < $precision )
                                           then "Close"
    else                                        "Different"
    end
  end
  
  def correct?(query = nil)
    if query.nil? then 
      not @toplevels.keys.find { |query| not correct? query }
    else
      raise "Invalid toplevel query #{query}" unless @toplevels.has_key? query
      
      query = @toplevels[query]
      case query[:type]
        when :singleton then 
          (diff(query[:expected], query[:result]) != "Different")
        when :onelevel then
          expected = query[:expected];
          result = query[:result]
          raise "Got nil result" if result.nil?;
          raise "Metadata has nil expected results" if expected.nil?;
          not ((expected.keys + result.keys).uniq.find do |k|
            diff(expected[k], result[k]) == "Different"
          end)
        else raise "Unknown query result type '#{query[:type]}'"
      end
    end
  end
  
  def dbt_base_cmd
    [ $dbt, @qpath ] +
      (if $depth.nil? then [] else ["--depth", $depth ] end) + 
      @compiler_flags + $compiler_args +
      ($debug_flags.map { |f| ["-d", f]}.flatten(1)) +
      ($opts.map { |f| ["-f", f]}.flatten(1))
  end
  
  def results(query = @toplevels.keys[0])
    raise "Invalid toplevel query #{query}" unless @toplevels.has_key? query
    query = @toplevels[query]
    expected = query[:expected];
    result = query[:result];
    case query[:type]
      when :singleton then [["*", expected, result, diff(expected, result)]]
      when :onelevel then
        p expected;
        p result;
        (expected.keys + result.keys).uniq.sort.
          map do |k| 
            [ k.join("/"), 
              expected[k], 
              result[k], 
              diff(expected[k], result[k])
            ]
          end
      else raise "Unknown query type '#{query[:type]}'"
    end
  end
end

class CppUnitTest < GenericUnitTest
  def run
    unless $skip_compile then
      compile_cmd = 
        "OCAMLRUNPARAM='#{$ocamlrunparam}';" +
        (dbt_base_cmd + [
        "-l","cpp",
        "-o","bin/queries/#{@qname}.cpp",
        "-c","bin/queries/#{@qname}"
      ]).join(" ") + "  2>&1";
      starttime = Time.now
      system(compile_cmd) or raise "Compilation Error";
      print "(Compile: #{(Time.now - starttime).to_i}s) "
      $stdout.flush;
    end
    return if $compile_only;
    starttime = Time.now;
    IO.popen("bin/queries/#{@qname} #{$executable_args.join(" ")}",
             "r") do |qin|
      output = qin.readlines;
      endtime = Time.now;
      output = output.map { |l| l.chomp }.join("");
      @runtime = (endtime - starttime).to_f;
      
      @toplevels.keys.each do |q| 
        if /<_#{q}[^>]*>(.*)<\/_#{q}>/ =~ output then
          q_results = $1
          case @toplevels[q][:type]
            when :singleton then @toplevels[q][:result] = q_results.to_f
            when :onelevel then  @toplevels[q][:result] = CppDB.new(q_results)
            else nil
            
          end
        else raise "Runtime Error: No result for query #{q}"
        end
      end
    end
  end
  
  def to_s
    "C++ Code Generator"
  end
end

class ScalaUnitTest < GenericUnitTest
  def run
    unless $skip_compile then
    dir = File.dirname("bin/queries/#{@qname}")
    FileUtils.mkdir_p dir unless File::exists? dir;
	  File.delete("bin/queries/#{@qname}.jar") if 
	       File::exists?("bin/queries/#{@qname}.jar");
      compile_cmd = 
        "OCAMLRUNPARAM='#{$ocamlrunparam}';" +
        (dbt_base_cmd + [
        "-l","scala",
        "-o","bin/queries/#{@qname}.scala",
        "-c","bin/queries/#{@qname}",
      ]).join(" ") + "  2>&1";
      starttime = Time.now
      system(compile_cmd) or raise "Compilation Error";
      print "(Compile: #{(Time.now - starttime).to_i}s) "
      $stdout.flush;
    end
    return if $compile_only;
    starttime = Time.now;
    IO.popen("scala -classpath \"bin/queries/#{@qname}.jar:lib/dbt_scala/dbtlib.jar\" org.dbtoaster.RunQuery", "r") do |qin|
      output = qin.readlines;
      endtime = Time.now;
      output = output.map { |l| l.chomp.strip }.join("");
      @runtime = (endtime - starttime).to_f;
      
      @toplevels.keys.each do |q| 
        if /<#{q}[^>]*>(.*)<\/#{q}>/ =~ output then
          result = $1;
          case @toplevels[q][:type]
            when :singleton then @toplevels[q][:result] = result.strip.to_f;
            when :onelevel then @toplevels[q][:result] = CppDB.new(output);
            else nil
          end
        else raise "Runtime Error"
        end;
      end
    end
  end

  def to_s
    "Scala Code generator"
  end
end

class InterpreterUnitTest < GenericUnitTest
  def run
    cmd = "OCAMLRUNPARAM='#{$ocamlrunparam}';"+
       "#{dbt_base_cmd.join(" ")}"+
       " -d SINGLE-LINE-MAP-OUTPUT"+
       " -r 2>&1";
    IO.popen(cmd, "r") do |qin|
      starttime = Time.now;
      @runtime = "unknown";
      qin.each do |l|
        case l
          when /Processing time: ([0-9]+\.?[0-9]*)(e-?[0-9]+)?/ then 
            @runtime = "#{$1}#{$2}".to_f
            print "(Compile: #{(Time.now - starttime - @runtime).to_i}s) "
          when /([a-zA-Z0-9_-]+): (.*)$/ then
            query = $1;
            results = $2;
            raise "Runtime Error: #{results}" if query == "error"
            raise "Unexpected result '#{query}'" unless 
                @toplevels.has_key? query
            case @toplevels[query][:type]
              when :singleton then @toplevels[query][:result] = results.to_f
              when :onelevel then
                @toplevels[query][:result] = OcamlDB.new(results, false)
            end
        end
      end
    end
  end

  def to_s
    "OcaML Interpreter"
  end
end

tests = [];
$opts = []; 
$debug_flags = [];
$skip_compile = false;
$precision = 1e-4;
$strict = false;
$ret = 0;
$depth = nil;
$verbose = false;
$compile_only = false;
$dataset = nil;
$compiler_args = [];
$executable_args = [];
$dump_query = false;
$log_detail = false;

GetoptLong.new(
  [ '-f',                GetoptLong::REQUIRED_ARGUMENT],
  [ '-t', '--test',      GetoptLong::REQUIRED_ARGUMENT],
  [ '--skip-compile',    GetoptLong::NO_ARGUMENT],
  [ '-p', '--precision', GetoptLong::REQUIRED_ARGUMENT],
  [ '-d',                GetoptLong::REQUIRED_ARGUMENT],
  [ '--depth',           GetoptLong::REQUIRED_ARGUMENT],
  [ '-v', '--verbose',   GetoptLong::NO_ARGUMENT],
  [ '--strict',          GetoptLong::NO_ARGUMENT],
  [ '--compile-only',    GetoptLong::NO_ARGUMENT],
  [ '--dataset',         GetoptLong::REQUIRED_ARGUMENT],
  [ '--trace',           GetoptLong::OPTIONAL_ARGUMENT],
  [ '--dump-query',      GetoptLong::NO_ARGUMENT],
  [ '--memprofiling',    GetoptLong::NO_ARGUMENT]
).each do |opt, arg|
  case opt
    when '-f' then $opts.push(arg)
    when '--skip-compile' then $skip_compile = true;
    when '-p', '--precision' then $precision = 10 ** (-1 * arg.to_i);
    when '-t', '--test' then 
      case arg
        when 'cpp'         then tests.push CppUnitTest
        when 'interpreter' then tests.push InterpreterUnitTest
        when 'scala'       then tests.push ScalaUnitTest
        when 'all'         then tests = [CppUnitTest, InterpreterUnitTest, ScalaUnitTest]
      end
    when '-d' then $debug_flags.push(arg)
    when '--depth' then $depth = arg
    when '--strict' then $strict = true;
    when '-v', '--verbose' then $verbose = true;
    when '--compile-only' then $compile_only = true;
    when '--dataset' then $dataset = arg;
    when '--trace' then $executable_args += ["-t", "QUERY_1_1", "-s", 
                                             if arg.nil? then "100" 
                                                         else arg end ];
                        $log_detail = true;
    when '--dump-query' then $dump_query = true;
    when '--memprofiling' then 
      $compiler_args += ["-l", "cpp:prof", "-g", "^-ltcmalloc"]
  end
end

tests.uniq!
tests = [ScalaUnitTest, InterpreterUnitTest] if tests.empty?;

queries = ARGV

queries.each do |tquery| 
  tests.each do |test_class|
    t = test_class.new
    opt_terms = 
      (if $debug_flags.empty? then [] 
                              else [["debug flags",$debug_flags]] end)+
      (if $opts.empty? then [] else [["optimizations",$opts]] end)+
      (if $depth.nil? then [] else [["depth", ["#{$depth}"]]] end)+
      (if $compile_only then [["compilation", ["only"]]] else [] end)+
      (if $dataset.nil? then [] else [["dataset", [$dataset]]] end)+
      (if $compiler_args.empty? then [] 
                                else [["compilation args", 
                                      ["'#{$compiler_args.join(" ")}'"]]] 
                                end)+
      (if $executable_args.empty? then [] 
                                  else [["runtime args", 
                                         ["'#{$executable_args.join(" ")}'"]]] 
                                  end)
    opt_string =
      if opt_terms.empty? then ""
      else " with #{opt_terms.map{|k,tm|"#{k} #{tm.join(", ")}"}.join("; ")}"
      end
    print "Testing query '#{tquery}'#{opt_string} on the #{t.to_s}: ";
    STDOUT.flush;
    t.query = tquery
    puts t.query if $dump_query;
    begin
      t.run
      unless $compile_only then
        if t.correct? then
          puts "Success (#{t.runtime}s)."
          puts(([["Key", "Expected", "Result", "Different?"], 
                 ["", "", ""]] + t.results).tabulate) if $verbose;
        else
          puts "Failure: Result Mismatch (#{t.runtime}s)."
          puts(([["Key", "Expected", "Result", "Different?"], 
                 ["", "", ""]] + t.results).tabulate);
          $ret = -1;
          exit -1 if $strict;
        end
      else
        puts "Success"
      end
    rescue Exception => e
      puts "Failure: #{e}";
      puts e.backtrace.join("\n");
      $ret = -1;
      exit -1 if $strict;
    end
  end
end
  
exit $ret;
