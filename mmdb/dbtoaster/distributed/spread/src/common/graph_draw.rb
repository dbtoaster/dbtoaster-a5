
require 'ok_mixins'
require 'tempfile'

class Graph
  attr_reader :lines, :subgraphs, :fs, :keys,  :style;
  attr_writer :subgraphs, :fs, :axis, :range, :directives;
  
  attr_reader :name;
  attr_writer :name;
  
  def initialize(input, name = "Untitled Graph", fs = / +/, keys = Array.new)
    @fs, @keys, @name = fs, keys, name;
    @style = "linespoints"
    @axis = [1,1];
    @range = [nil, nil, nil, nil];
    @directives = Array.new;
    @lines = 
      case input
        when File   then input.readlines
        when String then File.open(input).readlines
        when Array  then 
          input.delete_if do |i|
            if i.is_a? Graph then
              @subgraphs = Array.new unless @subgraphs;
              @subgraphs.push(i);
              true
            else false
            end
          end
      end.collect do |l|
        l.chomp;
      end
  end
  
  def [](subgraph)
    raise "Error: No subgraphs in graph" unless @subgraphs;
    return @subgraphs.find { |sg| sg if sg.name == subgraph }
  end
  
  def clone
    ret = Graph.new(@lines.clone, @name, @fs, if @keys then @keys.clone end);
    ret.subgraphs = @subgraphs.collect { |sg| sg.clone } if @subgraphs;
    ret.style = @style;
    ret.axis = @axis.clone;
    ret.range = @range.clone;
    ret.directives = @directives.clone;
    ret;
  end
  
  def filter(regex = nil, &block)
    if @subgraphs then @subgraphs.each { |sg| sg.filter(regex, block) }
    else
      if regex.nil? then
        @lines = @lines.delete_if { |l| not block.call(l) }
      elsif regex.is_a? Hash then
        @subgraphs = Array.new;
        regex.each_pair do |k, r| 
          @subgraphs.push(Graph.new(@lines.collect { |l| l if r.match(l) }.compact, k, @fs, @keys))
        end
      elsif regex.is_a? Array then
        @subgraphs = Array.new;
        regex.each do |r| 
          @subgraphs.push(Graph.new(@lines.collect { |l| l if r[1].match(l) }.compact, r[0], @fs, @keys))
        end
      else
        @lines.delete_if { |l| not regex.match(l) }
      end
    end
    self;
  end
  
  def limit(col, range)
    col = @keys.index(col) if col.is_a? String;
    if @subgraphs then @subgraphs.each { |sg| sg.limit(col, range) }
    else
      @lines.delete_if { |l| not range === l.split(@fs)[col].to_i };
    end
  end
  
  def pivot(col, agg)
    col = @keys.index(col) if col.is_a? String;
    @lines = @subgraphs.collect do |sg|
      sg.name + " " + 
        case agg
          when :avg then sg.compute_average(col).to_s;
          when :max then sg.compute_max(col).to_s;
        end
    end
    @subgraphs = nil;
  end
  
  def adjust(col) 
    col = @keys.index(col) if col.is_a? String;
    if @subgraphs then @subgraphs.each { |sg| sg.adjust(col) { |val| yield val } }
    else
      @lines = @lines.collect do |l| 
        l = l.split(@fs);
        l[col] = yield l[col];
        l.join(" ");
      end
    end
  end
  
  def compute_average(col)
    Math.avg(*@lines.collect { |l| l.split(@fs)[col].to_i });
  end
  
  def compute_max(col)
    Math.max(*@lines.collect { |l| l.split(@fs)[col].to_i });
  end
  
  def make_counter(regex, sep=" ", counter_last = nil)
    if @subgraphs then @subgraphs.each { |sg| sg.make_counter(regex, sep, counter_last) }
    else
      counter_last = "0";
      @lines.collect! do |line|
        if match = regex.match(line) then
          counter_last = (if match.size > 1 then match[1] else (counter_last.to_i) + 1 end).to_s
        end
        if counter_last then
          counter_last + sep + line;
        end
      end.compact!
    end
    self;
  end
  
  def gsub(regex, subst="\\1 \\2")
    if @subgraphs then @subgraphs.each { |sg| sg.gsub(regex, sub) }
    else
      @lines.collect! { |line| line.gsub(regex, subst) };
    end
    self;
  end
  
  def average(col, subgraphs = @subgraphs.collect { |sg| sg.name }, name = nil)
    avglines = subgraphs.collect { |sg| self[sg].lines }
    avglines = avglines.shift.merge(*avglines).collect do |lset| 
      lset = lset.collect { |l| l.split(@fs) }.matrix_transpose.collect_index do |i, l| 
        if i != col then l[0] else Math.avg(*l) end;
      end.join(" ");
    end
    if name then
      @subgraphs.delete_if { |sg| subgraphs.include? sg.name }
      @subgraphs.push(Graph.new(avglines, name, @fs, @keys));
    else
      @subgraphs = nil;
      @lines = avglines;
    end
    self;
  end
  
  def keys=(keys)
    @keys = keys.clone;
    @subgraphs.each { |sg| sg.keys=(keys) } if @subgraphs;
    self
  end
  def style=(style)
    @style = style;
    @subgraphs.each { |sg| sg.style=(style) } if @subgraphs;
    self;
  end
  
  def extract(*elements)
    if @subgraphs then @subgraphs.each { |sg| sg.extract(elements) }
    else
      elements.flatten!
      @lines = @lines.collect_index do |i, l|
        l = l.split(fs);
        l = elements.collect do |e| 
          case e
            when Numeric  then l[e]
            when "Line #" then i.to_s
            when String   then raise "Invalid Key: " + e unless keys.include? e; l[keys.index(e)]
          end
        end
        l.join(" ");
      end
    end
    @keys = elements.collect do |e|
      case e
        when Numeric then @keys[e];
        else              e;
      end
    end
    self;
  end
  
  def set_axis(axis)
    raise "Invalid Axis ID" unless axis_names.has_key? axis;
    @axis[ (axis_names[axis] / 2).to_i ] = ""+axis.to_s.slice(1,1);
    self;
  end
  
  def set_range(axis, range = nil)
    raise "Invalid Axis ID" unless axis_names.has_key? axis;
    @range[ axis_names[axis] ] = range;
  end
  
  def label(axis, label)
    raise "Invalid Axis ID" unless axis_names.has_key? axis;
    keys[ (axis_names[axis] / 2).to_i ] = label;
    self;
  end
  
  def title(t)
    @name = t;
    self;
  end
  
  def dump(prefix = "")
    if @subgraphs then
      @subgraphs.collect { |sg| prefix + "========= " + sg.name + " ==========\n" + sg.dump(prefix + "  ") };
    elsif @lines.is_a? Array then
      @lines.collect { |l| prefix + l }.join("\n");
    end
  end
  
  def directive(dir)
    @directives.push(dir)
    self;
  end
  
  def generate_plot(text, out_file = nil, out_format = "pdf enhanced")
    gp = File.popen("gnuplot", "w+");
    if out_file then
      gp.write("set terminal " + out_format + ";\n");
      gp.write("set output '" + out_file + "';\n");
    end
    axis_names.each_pair do |name, index|
      unless name.to_s[1] == "1"[0] then
        if @range[index] then
          gp.write("set " + name.to_s + "range [" + @range[index].begin.to_s + ":" + @range[index].end.to_s + "];\n");
          if name.to_s[1] == "2"[0] then
            gp.write("set " + name.to_s + "tics autofreq;\n");
            gp.write("set " + name.to_s.slice(0,1) + "tics nomirror;\n");
          end
        end
      end
    end
    if @keys.size >= 2 then
      gp.write("set xlabel \"" + @keys[0] +"\";\n")
      gp.write("set ylabel \"" + @keys[1] +"\";\n")
    end
    gp.write(@directives.join(";\n") + ";\n") unless @directives.empty?;
    gp.write("plot " + text + ";\n");
    gp.close;
    system("open " + out_file) if out_file && (`uname`.chomp == "Darwin");
  end
  
  def graph_data_nonrecursive
    tmp = Tempfile.new("graph_draw_data");
    tmp.write(@lines.join("\n"));
    tmp.flush;
    text = 
      "'" + tmp.path.to_s + "' " + 
      "title \"" + @name.to_s + "\" " +
      "axis x" + @axis[0].to_s + "y" + @axis[1].to_s + " " +
      "with " + @style;
  end
  
  def graph_data(type = :basic, params = Hash.new(0))
    if @subgraphs then
      case type
        when :basic then @subgraphs.collect { |sg| sg.graph_data(type) }.join(", ");
        when :bar   then @subgraphs.collect { |sg| ret = sg.graph_data(type, params); params[:shift] = params[:shift] + params[:box_width]; ret; }.join(", ");
      end
    else
      case type
        when :basic then
          graph_data_nonrecursive
        when :bar   then
          lines = @lines;
          @style = "boxes fs pattern"
          @lines = @lines.collect_index do |i, line|
            (params[:field_width]*i+params[:shift]).to_s + " " + line.split(@fs)[1]
          end
          ret = graph_data_nonrecursive
          @lines = lines;
          ret;
      end
    end
  end
  
  def bar_graph_keys
    if @subgraphs then
      ret = @subgraphs[0].bar_graph_keys
      ret[1] *= @subgraphs.size;
      ret;
    else
      [ @lines.collect { |line| line.split(@fs)[0] }, 1 ];
    end
  end
  
  def graph(out_file = nil)
    generate_plot(graph_data, out_file);
  end
  
  def graph_bar(out_file = nil)
    old_directives = @directives.clone;
    old_x_range = @range[0];
    old_style = @style;
    
    line_names, width = *bar_graph_keys;
    box_width = 0.75
    field_width = 0.5 + box_width*width
    puts field_width.to_s;

    directive("set style data boxes");
    directive("set boxwidth " + box_width.to_s);
    directive("set xtic rotate by -25");

    directive("set xtics (" +
      line_names.collect_index do |i, lname|
        "\"  " + lname+"\" " +(i*field_width + 0.1+(field_width/2.0)).to_s
      end.join(", ") + ")"
    );
    @range[0] = (0..(0.5+field_width*line_names.size))
    generate_plot(graph_data(:bar, {:shift => 0.9, :box_width => box_width, :field_width => field_width }), out_file);
    
    
    @range[0] = old_x_range;
    @directives = old_directives;
  end
  
  def axis_names
    { :x => 0, :y => 2, :x1 => 0, :x2 => 1, :y1 => 2, :y2 => 3 }
  end
end





