
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
  
  def make_counter(regex, sep=" ", counter_last = nil)
    if @subgraphs then @subgraphs.each { |sg| sg.make_counter(regex, sep, counter_last) }
    else
      @lines.collect! do |line|
        if match = regex.match(line) then
          counter_last = match[1];
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
  
  def graph(out_file = nil, out_format = "pdf enhanced", generate = true)
    if @subgraphs then
      text = @subgraphs.collect { |sg| sg.graph(out_file, out_format, false) }.join(", ");
    else
      tmp = Tempfile.new("graph_draw_data");
      tmp.write(@lines.join("\n"));
      tmp.flush;
      text = 
        "'" + tmp.path.to_s + "' " + 
        "title \"" + @name.to_s + "\" " +
        "axis x" + @axis[0].to_s + "y" + @axis[1].to_s + " " +
        "with " + @style;
    end
    if generate then
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
      if keys.size >= 2 then
        gp.write("set xlabel \"" + keys[0] +"\";\n")
        gp.write("set ylabel \"" + keys[1] +"\";\n")
      end
      gp.write(@directives.join(";\n") + ";\n") unless @directives.empty?;
      gp.write("plot " + text + ";\n");
      gp.close;
      system("open " + out_file) if out_file && (`uname`.chomp == "Darwin");
    end
    text;
  end
  
  def axis_names
    { :x => 0, :y => 2, :x1 => 0, :x2 => 1, :y1 => 2, :y2 => 3 }
  end
end

