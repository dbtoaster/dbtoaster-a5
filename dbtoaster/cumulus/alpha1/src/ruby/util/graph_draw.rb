
require 'util/ok_mixins';
require 'tempfile';
require 'getoptlong';
require 'readline';

module GraphSource
  attr_reader :headings, :last, :source;
  
  def select(selector)
    @selector = selector;
  end
  
  def rewind
    @source.pre_rewind if @source.respond_to?(:pre_rewind);
    @source.rewind;
    @source.post_rewind if @source.respond_to?(:post_rewind);
  end
  
  def next
    @last = super;
  end
  
  def each
    while @last = self.next
      yield last;
    end
  end
  
  def collect
    ret = Array.new;
    each { |c| ret.push(yield c); }
    ret;
  end
  
  def to_a
    ret = Array.new;
    each { |c| ret.push(c) }
    ret;
  end

  def select(inputs, outputs = nil)
    SelectFilter.new(self, inputs, outputs);
  end
  
  def aggregate(aggregates, group_by = [])
    Aggregate::Filter.new(self, aggregates, group_by);
  end
  
  def data(separator="\t")
    collect { |vals| vals.join(separator) }.join(",");
  end
  
  def tempfile(separator = "\t")
    file = Tempfile.new("graphdraw");
    each { |vals| puts vals.join(separator); file.puts(vals.join(separator)); }
    file.flush;
    file;
  end
end

class IOSource
  attr_writer :headings;
  
  include GraphSource;
  
  def initialize(source, splitter = /\w+/, read_headings = false)
    @source = source;
    @source = File.open(@source) if @source.is_a? String;
    @splitter = splitter;
    @headings = @source.readline.split(@splitter).collect_index { |i,c| [c,i] }.collect_hash if read_headings;
  end
  
  def next;
    line = @source.readline;
    line.split(@splitter) if line;
  end
end

class LogSource
  include GraphSource
  
  def initialize(source, tick_matcher = /.*/)
    @tick_matcher = tick_matcher;
    @source = source;
    @source = File.open(@source) if @source.is_a? String;
    @events = Hash.new;
  end
  
  def add_event(matcher, var_names = nil, &block)
    raise "add_event requires either a column list or an event handler block" if (!var_names.is_a? Array) && block == nil;
    @events[matcher] = block || var_names;
  end
  
  def process
    @checkpoint_count = 0;
    @checkpoints = Hash.new { |h,k| h[k] = Array.new(@checkpoint_count, 0) };
    variables = Hash.new { |h,k| h[k] = 0 };
    while line = @source.gets
      @events.each_pair do |matcher, event|
        if (match = matcher.match(line)) then
          match = match.to_a;
          match.shift; #get rid of the global match
          case event
            when Array then variables.merge!(event.zip(match).collect_hash)
            when Proc  then event.call(variables, match)
          end
        end
      end
      if @tick_matcher.match(line) then
        variables.each_pair { |var,val| @checkpoints[var].push(val) }
        @checkpoint_count += 1;
      end
    end
    @headings = @checkpoints.keys;
  end
  
  def next
    return nil unless @line_no + 1 <= @checkpoint_count;
    line = @line_no;
    @line_no += 1;
    @headings.collect { |k| @checkpoints[k][line] }
  end
  
  def post_rewind
    process;
    @line_no = 0;
  end
  
  alias :finish_setup :post_rewind;
end

##########################################

class SelectFilter
  include GraphSource;

  def initialize(source, inputs, outputs = nil)
    @source = source;
    @inputs = inputs.clone;
    @headings = outputs ? outputs.clone.freeze : inputs.clone.freeze;
    @inputs.collect! { |col| if col.is_a? Integer then col else @source.headings.index col end }
  end
  
  def next
    line = @source.next;
    @inputs.collect { |col| line[col] } if line;
  end
end

module Aggregate
  AGGREGATES = 
    [ :sum =>
        [ 0.0,
          Proc.new { |old, curr| old + curr },
          nil
        ],
      :max => 
        [ -1.0/0.0,
          Proc.new { |old, curr| Math.max(old, curr) },
          nil
        ],
      :min =>
        [ 1.0/0.0,
          Proc.new { |old, curr| Math.min(old, curr) },
          nil
        ],
      :count =>
        [ 0.0,
          Proc.new { |old, curr| old + 1 },
          nil
        ],
      :avg =>
        [ 0.0,
          Proc.new { |old, curr| [old[0] + curr, old[1] + 1] },
          Proc.new { |final| final[0] / final[1] }
        ]
    ];
  
  class Term
    attr_reader :input, :name;
    def initialize(type, input, output = nil)
      @process = AGGREGATES[type];
      @input = @name = input;
      @output = output ? output : (type.to_s + "(" + input + ")");
    end
    
    def obtain_column(headings)
      @input = headings.index(@name) unless @name.is_a? Integer;
    end
    
    def transform(old, curr)
      old = @process[0] unless old;
      @process[1].call(old, curr.to_i);
    end
    
    def finalize(final)
      @process[2] ? @process[2].call(final) : final;
    end
  end
  
  class Filter
    include GraphSource;
    
    def initialize(source, aggregates, group_by = [])
      @points = Hash.new { |h,k| h[k] = Array.new(group_by.size, nil) }
      @headings = group_by.collect { |agg| k.name }.concat(aggregates.keys)
      @aggregates.each { |agg| agg.obtain_column(source.headings) }
      
      post_rewind;
    end
    
    def next
      @points[@index+=1] unless @index >= @points.size;
    end
    
    def post_rewind
      @source.each do |vals|
        target = @points[group_by.collect { |col| vals[col] }];
        aggregates.each_with_index do |agg, index|
          target[index] = agg.transform(target[index], vals[agg.input]);
        end
      end
      
      @points.to_a.collect { |vals| vals.flatten };
      @index = -1;
    end
  end
end

##########################################

module Gnuplot
  single_quote = Proc.new { |v| "'#{v}'"};
  double_quote = Proc.new { |v| "\"#{v}\""};
  range_quote  = Proc.new { |v| raise "Invalid Range: #{v}" unless v.is_a? Range; "[#{ v.first }:#{ v.last }]"; }
  
  VARIABLES = Hash.new;
  def Gnuplot.axis_variable(name, processor, internal_name = nil)
    internal_name = name unless internal_name;
    ["x", "y", "x2", "y2"].each do |axis| 
      VARIABLES["#{axis}_#{internal_name}"] = ["#{axis}#{internal_name}", processor];
    end
  end
  def Gnuplot.basic_variable(name, processor, internal_name = nil)
    internal_name = name unless internal_name;
    VARIABLES["#{name}"] = [internal_name, processor];
  end
  basic_variable("terminal", nil         , "format"  );
  basic_variable("output"  , single_quote, "out_file");
  axis_variable("label" , double_quote);
  axis_variable("range" , range_quote );
  axis_variable("tics"  , nil );
  
  COMMANDS = VARIABLES.keys.collect_hash do |var| 
    [var, Proc.new { |backend,cmd| backend.send(var.to_sym, cmd) }];
  end
  
  class Backend
    VARIABLES.each_pair do |name, method|
      define_method("#{name}=".to_sym) { |val| set(method[0], val, method[1]) }
    end
    
    def initialize
      @directives = Array.new;
      @first_graph = true;
    end
    
    def []=(var, val)
      set(var, val);
    end
    
    def set(var, val, processor = double_quote)
      directive("set #{var} #{processor ? processor.call(val) : val};")
    end
    
    def directive(dir)
      @directives.push(dir);
    end
    
    def draw(graph)
      @command = File.popen("gnuplot", "w+");
      if @command then
        @directives.each { |directive| @command.puts(directive) };
      end
      sources = graph.flush_to_files;
      plot_cmd = 
        "plot " + 
        graph.views.collect do |view|
          "'#{sources[view.source_name].path}' using #{view.cols.collect{|c|c.to_i+1}.join(":")} title \"#{view.name}\""
        end.join(", ") + ";"
        
      puts plot_cmd;
      @command.puts(plot_cmd);
    end
    
    def backend_commands
      COMMANDS;
    end
  end
end

##########################################

class GraphView
  attr_reader :source_name, :name, :col_names;
  
  def initialize(source_name, source_data, name, x, y, xdev = nil, ydev = nil)
    @source_name = source_name;
    @source_data = source_data;
    @name = name;
    @col_names = [x,y];
    if xdev && ydev then @col_names.push(xdev); @col_names.push(ydev) end
  end
  
  def cols
    puts @source_data.headings.join(",");
    @col_names.collect { |name| @source_data.headings.index(name) unless name.is_a? Integer };
  end
  
end

class Graph
  attr_reader :backend, :sources;
  attr_writer :backend;
  
  def initialize(backend = Gnuplot::Backend.new)
    @sources = Hash.new;
    @views = Hash.new;
    @backend = backend;
    @last_source = nil;
  end
  
  def add_source(source, name = @sources.size.to_s)
    @sources[name] = source;
    @last_source = name;
  end
  
  def add_view(name, x, y, xdev = nil, ydev = nil, source_name = @last_source)
    @views[name] = GraphView.new(source_name, self[source_name], name, x, y, xdev, ydev);
  end
  
  def views
    if @views.empty? then
      @sources.keys.collect { |s| GraphView.new(s,self[s],s,0,1); } 
    else
      @views.values
    end
  end
  
  def [](source_id)
    @sources[source_id];
  end
  
  def flush_to_files(separator = "\t");
    @sources.to_a.collect_hash do |source|
      [source[0], source[1].tempfile(separator)]
    end
  end
  
  def draw
    backend.draw(self);
  end
end

# $quiet = false;
# $graph = Graph.new;
# $last_source = nil;
# $last_view = nil;
# 
# GetoptLong.new(
#   [ "-q", "--quiet", GetoptLong::NO_ARGUMENT ],
#   [ "-", "--quiet", 
# ).each do |opt, arg|
#   case opt
#     when "-q", "--quiet" then $quiet = true;
#   end
# end
# 
# at_exit { puts ""; } unless $quiet;
# 
# while line = Readline.readline($quiet ? nil : "> ", true);
#   begin
#     line = line.split("/ +/");
#     cmd = line.shift;
#     case cmd
#       when "source" then 
#         raise "Error: source name file" unless line.size >= 2;
#         name = line.shift; 
#         $graph.add_source(line.join(" "), name);
#       when "view" then 
#         raise "Error: view source name x_col y_col [x_dev y_dev]" unless (line.size == 4) || (line.size == 6);
#         $graph.add_view(*line);
#       else 
#         if $graph.backend.backend_commands.has_key? cmd then
#           $graph.backend.backend_commands[cmd].call(line, $graph.backend);
#         else
#           raise "Error: Unknown command: '#{cmd}'";
#         end
#     end
#   rescue Exception => e
#     puts e.to_s;
#     exit(-1) if $quiet;
#   end
# end

