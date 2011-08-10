#!/usr/bin/env ruby
require "#{File.dirname($0)}/util.rb"
require 'getoptlong'

$dbt_path = "#{File.dirname(File.dirname(File.dirname($0)))}"
$dbt = "#{$dbt_path}/dbtoaster"

raise "DBToaster is not compiled" unless (File.exists? $dbt_path)

def results_file(path, delim = /,/)
  File.open(path).readlines.
    delete_if { |l| l.chomp == "" }.  
    map do |l|
      k = l.split(delim).map { |i| i.to_f }
      [k, k.pop];
    end.to_h
end

$optimizations = {
  "runtime-bigsums"   => "-d runtime-bigsums",
  "dup-ivc"           => "-d dup-ivc"
}

$queries = {
  "rst" => {
    :path => "test/sql/simple/rst.sql",
    :type => :singleton,
    :answer => 18753367048934.0
  },
  "vwap" => {
    :path => "test/sql/finance/vwap.sql",
    :type => :singleton,
    :answer => 28916017900.0
  },
  "pricespread" => {
    :path => "test/sql/finance/pricespread.sql",
    :type => :singleton,
    :answer => 76452380068302.0,
    :valid_opts => ["dup-ivc"]
  },
  "missedtrades" => {
    :path => "test/sql/finance/missedtrades.sql",
    :type => :onelevel,
    :answer => results_file("test/results/missedtrades.csv"),
    :valid_opts => ["dup-ivc"]
  },
  "axfinder" => {
    :path => "test/sql/finance/axfinder.sql",
    :type => :onelevel,
    :answer => results_file("test/results/axfinder.csv"),
    :valid_opts => ["runtime-bigsums", "dup-ivc"]
  },
  "tpch3" => {
    :path => "test/sql/tpch/query3.sql",
    :type => :onelevel,
    :answer => results_file("test/results/tpch/query3_100M.csv")   
#    {
#      [ 0.0, 19941212.0, 3430.0 ] => 4726.6775,
#      [ 0.0, 19950217.0, 4423.0 ] => 3055.9365,
#      [ 0.0, 19950123.0, 2883.0 ] => 36666.9612,
#      [ 0.0, 19941124.0, 3492.0 ] => 43716.0724,
#      [ 0.0, 19941126.0, 998.0  ] => 11785.5486,
#      [ 0.0, 19950208.0, 1637.0 ] => 164224.9253,
#      [ 0.0, 19941211.0, 5191.0 ] => 49378.3094,
#      [ 0.0, 19941223.0, 742.0  ] => 43728.048
#    }
  },
#  "tpch5" => {
#    :path => "test/sql/tpch/query5.sql",
#    :type => :onelevel,
#    :answer => results_file("test/results/tpch/query11.csv")
#  },
  "tpch11" => {
    :path => "test/sql/tpch/query11a.sql",
    :type => :onelevel,
    :answer => results_file("test/results/tpch/query11.csv")
  },
  "tpch17" => {
    :path => "test/sql/tpch/query17.sql",
    :type => :singleton,
    :answer => 898778.73
  },
  "tpch18" => {
    :path => "test/sql/tpch/query18.sql",
    :type => :onelevel,
    :answer => results_file("test/results/tpch/query18.csv")
  },
  "tpch18simple" => {
    :path => "test/sql/tpch/query18simple.sql",
    :type => :singleton,
    :answer => 6005.0
  },
  "tpch22" => {
    :path => "test/sql/tpch/query22.sql",
    :type => :onelevel,
    :answer => results_file("test/results/tpch/query22.csv")
  },
  "clusteravailable" => {
    :path => "test/sql/clusteravailable.sql",
    :type => :onelevel,
    :answer => results_file("test/results/clusteravailable.csv")
  }
};

class GenericUnitTest
  attr_reader :runtime, :opts;
  attr_writer :opts;

  def query=(q, qdat = $queries[q])
    @qname = q;
    @qtype = qdat[:type];
    @qpath = qdat[:path];
    @expected = qdat[:answer];
    @result = Hash.new;
  end
    
  def query
    File.open(@qpath).readlines.join("");
  end
  
  def correct?
    case @qtype
      when :singleton then (diff(@expected, @result) != "Different")
      when :onelevel then
        not (@expected.keys + @result.keys).uniq.find do |k|
          (@expected.has_key? k) && 
          (@result.has_key? k) && 
          (diff(@expected[k], @result[k]) == "Different")
        end
      else raise "Unknown query type '#{@qtype}'"
    end
  end
  
  def diff(e, r)
    if (e == r)                            then "Same"
    elsif e == nil                         then "Different"
    elsif r == nil                         then "Different"
    elsif ((e-r) / (e+r)).abs < $precision then "Close"
    else                                        "Different"
    end
  end
  
  def results
    case @qtype
      when :singleton then [["*", @expected, @result, diff(@expected, @result)]]
      when :onelevel then      
        (@expected.keys + @result.keys).uniq.sort.
          map do |k| 
            [ k.join("/"), 
              @expected[k], 
              @result[k], 
              diff(@expected[k], @result[k])
            ]
          end
      else raise "Unknown query type '#{@qtype}'"
    end
  end
end

class CppUnitTest < GenericUnitTest
  def run
    unless $skip_compile then
      compile_cmd = ([
        $dbt, 
        "-l","cpp",
        "-o","#{$dbt_path}/bin/#{@qname}.cpp",
        "-c","#{$dbt_path}/bin/#{@qname}"
      ] + ($debug_flags.map { |f| ["-d", f]}.flatten(1)) + [
        @qpath
      ]).join(" ") + " " + @opts + "  2>&1";
      starttime = Time.now
      system(compile_cmd) or raise "Compilation Error";
      print "(Compile: #{(Time.now - starttime).to_i}s) "
      $stdout.flush;
    end
    starttime = Time.now;
    IO.popen("#{$dbt_path}/bin/#{@qname} -q", "r") do |qin|
      output = qin.readlines.map { |l| l.chomp }.join("")
      endtime = Time.now;
      @runtime = (endtime - starttime).to_f;
      if(/<QUERY_1_1[^>]*>(.*)<\/QUERY_1_1>/ =~ output) then
        output = $1;
        case @qtype
          when :singleton then @result = output.to_f
          when :onelevel then
            tok = Tokenizer.new(output, /<\/?[^>]+>|[^<]+/);
            @result = Hash.new;
            loop do
              tok.tokens_up_to(/<item[^>]*>/);
              break unless /<item[^>]*>/ =~ tok.last;
              fields = Hash.new("");
              curr_field = nil;
              tok.tokens_up_to("</item>").each do |t|
                case t
                  when /<\/.*>/ then curr_field = nil;
                  when /<(.*)>/ then curr_field = $1;
                  else 
                    if curr_field then 
                      fields[curr_field] = fields[curr_field] + t 
                    end
                end
              end
              keys = fields.keys.clone;
              keys.delete("__av");
              @result[
                keys.
                  map { |k| k[3..-1].to_i }.
                  sort.
                  map { |k| fields["__a#{k}"].to_f }
              ] = fields["__av"].to_f unless fields["__av"].to_f == 0.0
            end
          else nil
        end
      else puts output; raise "Runtime Error"
      end;
    end
  end
  
  def to_s
    "C++ Code Generator"
  end
end

class InterpreterUnitTest < GenericUnitTest
  def run
    IO.popen("#{$dbt} -r #{@qpath} #{@opts} 2>&1"+
             ($debug_flags.map {|f|" -d #{f}"}.join("")), "r") do |qin|
      output = qin.readlines.join("")
      if /Processing time: ([0-9]+\.?[0-9]*)/ =~ output
        then @runtime = $1.to_f
        else @runtime = "unknown"
      end
      raise "Runtime Error" unless (/QUERY_1_1: (.*)$/ =~ output);
      output = $1
      case @qtype
        when :singleton then @result = output.to_f
        when :onelevel then
          tok = Tokenizer.new(
            output, 
            /->|\[|\]|;|-?[0-9]+\.?[0-9]*|<pat=[^>]*>/
          );
          tok.clear_whitespace;
          tree = TreeBuilder.new;
          while(tok.more?) do
            case tok.next
              when "[" then 
                tree.push;
              when "]" then
                if tok.next == "->" 
                  then tree.insert tok.next.to_f 
                end
                tree.pop;
              when /[0-9]+\.[0-9]*/ then
                tree.insert tok.last.to_f
            end
          end
          @result = 
            tree.to_a.pop.map { |row| row.map { |v| v.to_f } }.
            map { |k| v = k.pop; [k, v] }.
            delete_if { |k,v| v == 0 }.
            to_h
        else nil
      end
    end
  end

  def to_s
    "OcaML Interpreter"
  end
end

tests = [];
$debug_flags = [];
queries = nil;
$skip_compile = false;
$precision = 1e-4;
$strict = false;
$ret = 0;
test_optimizations = []

GetoptLong.new(
  [ '-a', '--all',       GetoptLong::NO_ARGUMENT],
  [ '-o',                GetoptLong::REQUIRED_ARGUMENT],
  [ '--all-queries',     GetoptLong::NO_ARGUMENT],
  [ '--all-opts',        GetoptLong::NO_ARGUMENT],
  [ '-t', '--test',      GetoptLong::REQUIRED_ARGUMENT],
  [ '--skip-compile',    GetoptLong::NO_ARGUMENT],
  [ '-p', '--precision', GetoptLong::REQUIRED_ARGUMENT],
  [ '-d',                GetoptLong::REQUIRED_ARGUMENT],
  [ '--strict',          GetoptLong::NO_ARGUMENT]
).each do |opt, arg|
  case opt
    when '-a', '--all' then 
      queries = $queries.keys
      test_optimizations = $optimizations.keys
    when '--all-queries' then queries = $queries.keys
    when '--all-opts' then test_optimizations = $optimizations.keys
    when '-o' then test_optimizations.push arg;
    when '--skip-compile' then $skip_compile = true;
    when '-p', '--precision' then $precision = 10 ** (-1 * arg.to_i);
    when '-t', '--test' then 
      case arg
        when 'cpp'         then tests.push CppUnitTest
        when 'interpreter' then tests.push InterpreterUnitTest
        when 'all'         then tests = [CppUnitTest, InterpreterUnitTest]
      end
    when '-d' then $debug_flags.push(arg)
    when '--strict' then $strict = true;
  end
end

tests.uniq!
tests = [CppUnitTest] if tests.empty?;

queries = ARGV if queries.nil?

queries.each do |tquery| 
  if $queries.has_key? tquery then
    tests.each do |test_class|
      $queries[tquery].
        fetch(:valid_opts, []).
        intersect(test_optimizations).
        power_set.each do |test_opts|
          t = test_class.new
          opt_string = test_opts.empty? ? "" : 
            " with optimization#{test_opts.s?} #{test_opts.join(", ")}"
          print "Testing query '#{tquery}'#{opt_string} on the #{t.to_s}: "; 
          STDOUT.flush;
          t.query = tquery
          t.opts = test_opts.
            map { |opt| $optimizations.fetch(opt, "") }.
            join(" ")
          begin 
            t.run
            if t.correct? then
              puts "Success (#{t.runtime}s)."
            else
              puts "Failure: Result Mismatch"
              puts(([["Key", "Expected", "Result", "Different?"], 
                     ["", "", ""]] + t.results).tabulate);
              $ret = -1;
              exit -1 if $strict;
            end
          rescue Exception => e
            puts "Failure: #{e}";
            $ret = -1;
            exit -1 if $strict;
          end
      end
    end
  else
    puts "Unknown query #{tquery}"
  end
end

exit $ret;
