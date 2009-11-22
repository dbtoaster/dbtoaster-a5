
require 'ok_mixins';
require 'template'

class DBToaster
  attr_reader :compiled, :templates, :map_info, :test_directives, :slice_directives, :persist, :switch, :preload;

  def initialize(toaster_cmd = "./dbtoaster.top -noprompt 2> /dev/null", toaster_dir = File.dirname(__FILE__) + "/../../../../prototype/compiler/alpha3")
    @nodes = Array.new;
    @partition_directives = Hash.new;
    @test_directives = Array.new;
    @slice_directives = Array.new;
    @keys = Hash.new;
    @persist = false;
    @compiled = nil;
    @switch = "localhost";
    @preload = nil;
    
    local_dir = Dir.getwd()
    Dir.chdir(toaster_dir)
    puts toaster_dir+"/"+toaster_cmd;
    @DBT = open("|"+toaster_cmd, "w+");
    @DBT.write("open DBToasterTop;;\n");
    @DBT.write("compile_sql_to_spread \"");
    Dir.chdir(local_dir);
  end
  
  ########################################################
  ## Pre-compilation accessors to load data in.
  ########################################################
  
  def load(sql_lines)
    @DBT.write(
      sql_lines.collect do |l|
        if l[0..1] == "--" then l = l.split(" "); parse_arg(l.shift, l.join(" ")); nil
        else l.chomp end;
      end.compact.join(" ") 
    );
    self;
  end
  
  def parse_arg(opt, arg)
    case opt
      when "--node"      then 
        @nodes.push(/ *([a-zA-Z0-9_]+) *@ *([a-zA-Z_\-0-9\.]+)(:([0-9]+))?/.match(arg).map(["name", "address", "dummy", "port"]));
      when "--switch"    then @switch = arg;
      when "--partition" then 
        match = /Map *([a-zA-Z0-9_]+) *on *(.*)/.match(arg);
        raise "Invalid partition directive: " + arg unless match;
        @partition_directives[match[1]] = match[2].split(/, /).collect_hash do |column|
          sub_match = /([0-9]+) *(into *([0-9]+) *pieces|weight *by *([0-9]+))/.match(column)
          [sub_match[1].to_i, if sub_match[3].nil? then [:weight, sub_match[4].to_i] else [:exact, sub_match[3].to_i] end];
        end
      when "--test"      then @test_directives.push(arg);
      when "--slice"     then @slice_directives.push(arg);
      when "--persist"   then @persist = true;
      when "--key"       then 
        match = / *([^ \[]+) *\[ *([^\]]+) *\] *<= *([^ \[-]+) *\[ *([^\]]+) *\]/.match(arg)
        raise "Invalid key argument: " + arg unless match;
        @keys.assert_key(match[1]){ Hash.new }.assert_key(match[2]){ Hash.new }[match[3]] = [match[4]];
  #    else raise "Unknown option: " + opt;
      when "--preload"   then @preload = arg;
    end
  end
  
  ########################################################
  ## The compilation process
  ########################################################

  def toast(opts = Hash.new)
    @DBT.write("\";;\n");
    @DBT.close_write();
    data = @DBT.readlines;
    @DBT.close;
    
    begin    
      # line 1 is the annoying-ass header.  Delete it.  
      # Then, pull out the useful contents of list[****]
      # the contents, delimited by /"; *\n? *"/ can then be pulled out and each represents a line of template.
      # replace the "\t"s with actual tabs, and then ensure that each map reference includes at least one (constant if necessary) index.
      @compiled = data.collect do |l|
        l.chomp
      end.join("").gsub(/^.*string list[^\[]*\[([^#]*)"\].*/, "\\1").split("\";").collect do |l|
        l.gsub(/^ * *"([^"]*) *\n?/, "\\1\n").gsub(/\\t/, "	").gsub(/\[\]/, "[1]").gsub(/^ *\+/, "");
      end
      
      if @compiled.size < 2 then
        raise "Error compiling, no compiler output:" + data.join("");
      end
      
      # Make sure we have SOME nodes.
      raise "SQL file defines no nodes!  Either invoke toaster with --node, or include a --node directive in the SQL file" unless @nodes.size > 0;
  
      toast_templates;
      toast_schemas;
      toast_maps;
      @DBT = nil;
    rescue Exception => e
      puts "Error toasting.  Compiler output: \n" + data.join("");
      raise e;
    end
    self;
  end
  
  def toast_templates
    index = 0;
    @templates = @compiled.collect do |l|
#      puts "Loading template: " + l;
      UpdateTemplate.new(l, index += 1);
    end.delete_if do |template|
      template.relation[0] == "-"[0];
    end
  end
  
  def toast_schemas
    @schemas = @templates.collect_hash do |t|
      [ t.relation, t.paramlist ];
    end
    
    # Do we need to normalize the schemas here?
  end
  def toast_maps
    # Now that we're done parsing inputs, we can do some munging of the partition and 
    # domain directives.  Specifically, for each map referenced in the templates, we
    # want to figure out the map's domain, and come up with a set of dimensions to 
    # partition the map over (we currently only partition over one dim).  If necessary
    # we resort to defaults so that each map has a value.
    #
    # We get the map names from UpdateTemplate, which keeps a hash map of name => ID
    @map_info =
      UpdateTemplate.map_names.collect do |map,info|
        total_product = 1;
        weight_count = 0;
        setup_partition = @partition_directives.fetch(map) { { 0 => [:weight, 1] } }
        setup_partition.each_pair { |col, dist| weight_count += 1 if dist[0] == :weight; total_product *= dist[1].to_i };
        
        split_per_weight = (@nodes.size.to_f / total_product.to_f).to_i;
        split_per_weight = split_per_weight ** (1.0/weight_count.to_f) if weight_count > 0;
        
        
        final_partitions = (0...info["params"].to_i).collect do |col|
          ptype, magnitude = *setup_partition.fetch(col, [:exact, 1]);
          
          case ptype
            when :weight then (0 ... (magnitude * split_per_weight))
            when :exact  then (0 ... magnitude)
          end.to_a
        end.cross_product;
        raise SpreadException.new("Incorrect number of partitions created: Expected: " + @nodes.size.to_s + "; Generated: " + final_partitions.size.to_s) unless final_partitions.size == @nodes.size;
        
        [ info["id"].to_i,
          { "map"        => map, 
            "id"         => info["id"].to_i, 
            "num_keys"   => info["params"].to_i,
            "partition"  => final_partitions,
            "reads_from" => Array.new,
            "writes_to"  => Array.new,
            "discarded"  => false
          }
        ]
      end.collect_hash
  end  
    
  ########################################################
  ## Utility functions
  ########################################################
  
  def template_dag
    @templates.collect do |template|
      [ template.target.source.to_i, template ]
    end.compact.reduce
  end
  
  def map_depth(map)
    @map_info[map].assert_key("depth") do 
      Math.max(@map_info[map]["reads_from"].collect { |read| map_depth(read) }.push(0));
    end
  end
  
  ########################################################
  ## Accessors for the compiled output
  ########################################################
  
  def each_node
    @nodes.each_index do |node_index|
      node = @nodes[node_index];
      partitions = Hash.new { |h, k| h[k] = Array.new };
      @map_info.each_value do |map|
        unless map["discarded"] then
          partitions[map["id"].to_i].push(map["partition"][node_index])
        end
      end
      yield node["name"], partitions, node["address"], (node["port"] || 52982);
    end
  end
  
  def each_template
    @templates.each_index do |i|
      yield i, @templates[i];
    end
  end
  
  def each_map
    @map_info.each_pair do |map, info|
      yield map, info
    end
  end
  
  def success?
    @DBT.nil?
  end
end