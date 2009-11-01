
require 'ok_mixins'
require 'tempfile'

class Graph
  attr_reader :lines, :subgraphs, :fs, :keys, :name;
  attr_writer :subgraphs, :fs, :name;
  
  attr_reader :style;
  attr_writer :style;
  
  def initialize(input, name = "Untitled Graph", fs = / +/, keys = Array.new)
    @fs, @keys, @name = fs, keys, name;
    @style = "linespoints"
    @lines = 
      case input
        when File   then input.readlines
        when String then File.open(input).readlines
        when Array  then input
      end.collect do |l|
        l.chomp;
      end
  end
  
  def clone
    ret = Graph.new(@lines.clone, @name, @fs, if @keys then @keys.clone end);
    ret.subgraphs = @subgraphs.collect { |sg| sg.clone } if @subgraphs;
    ret.style = @style;
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
  
  def keys=(keys)
    @keys = keys.clone;
    @subgraphs.each { |sg| sg.keys=(keys) } if @subgraphs;
    self
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
  
  def label(axis, label)
    keys[ { :x => 0, :y => 1 }[axis] ] = label;
    self;
  end
  
  def dump(prefix = "")
    if @subgraphs then
      @subgraphs.collect { |sg| prefix + sg.name + "\n" + sg.dump(prefix + "  ") };
    elsif @lines.is_a? Array then
      @lines.collect { |l| prefix + l }.join("\n");
    end
  end
  
  def graph(out_file = nil, out_format = "pdf enhanced", generate = true)
    if @subgraphs then
      text = @subgraphs.collect { |sg| sg.graph(out_file, out_format, false) }.join(", ");
    else
      tmp = Tempfile.new("graph_draw_data");
      tmp.write(@lines.join("\n"));
      tmp.flush;
      text = "'" + tmp.path.to_s + "' title \"" + @name.to_s + "\" with " + @style;
    end
    if generate then
      gp = File.popen("gnuplot", "w+");
      if out_file then
        gp.write("set terminal " + out_format + ";\n");
        gp.write("set output '" + out_file + "';\n");
      end
      if keys.size >= 2 then
        gp.write("set xlabel \"" + keys[0] +"\";\n")
        gp.write("set ylabel \"" + keys[1] +"\";\n")
      end
      gp.write("plot " + text + ";\n");
      gp.close;
      system("open " + out_file) if out_file;
    end
    text;
  end
end

