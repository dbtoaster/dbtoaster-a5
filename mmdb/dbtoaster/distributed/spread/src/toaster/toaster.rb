
require 'ok_mixins';
require 'template'

class DBToaster
  attr_reader :compiled, :templates, :map_info, :test_directives, :slice_directives, :persist, :switch, :preload, :map_formulae;

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
    @schemas = nil;
    @map_aliases = Hash.new;
    @map_formulae = nil;
    
    local_dir = Dir.getwd()
    Dir.chdir(toaster_dir)
    puts toaster_dir+"/"+toaster_cmd;
    @DBT = open("|"+toaster_cmd, "w+");
    @DBT.write("open DBToasterTop;;\n");
    @DBT.write("#print_depth 10000;;\n");
    @DBT.write("#print_length 10000;;\n");
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
      when "--alias"     then @map_aliases = Hash[*arg.split(",").collect{ |x| x.strip }]
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

      generate_map_queries(data);
      
      @DBT = nil;
    rescue Exception => e
      puts "Error toasting.  Compiler output: \n" + data.join("");
      raise e;
    end
    self;
  end
  
  def toast_templates
    index = 0;

    templates_by_relation = Hash.new { |h, k| h[k] = Array.new }

    @compiled.collect do |l|
#      puts "Loading template: " + l;
      UpdateTemplate.new(l, index += 1);
    end.delete_if do |template|
      template.relation[0] == "-"[0];
    end.each do |template|
      rel_temps = templates_by_relation[ [template.relation, template.target.source] ];
      if rel_temps.empty? || (not rel_temps[0].conditions.conditions.empty?) then
        rel_temps.unshift(template);
      elsif (not template.conditions.conditions.empty?) then
        rel_temps.push(template);
      else
        rel_keys = rel_temps[0].entries.collect{ |e| e.key }.flatten.uniq
        new_key_names = template.entries.collect{ |e| e.key }.flatten.uniq.collect_hash do |key|
          if template.paramlist.include? key then 
            [key, rel_temps[0].paramlist[template.paramlist.index(key)]]
          else
            name = key;
            i = 1;
            while rel_keys.include? name
              name = "#{key}_#{i}"
              i += 1;
            end
            [key, name];
          end
        end
        template.expression.rename(new_key_names);
        rel_temps[0].add_expression(template.expression);
      end
    end
    
    @templates = templates_by_relation.collect { |rel, tlist| tlist }.flatten
    
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

  # Generate bootstrap queries
  def generate_map_queries(compiler_output)
    compiled_maps = compiler_output.select do |l|
      l =~ /Adding compilation of/
    end.collect do |l|
      n = l.chomp.gsub(/Adding compilation of ([a-zA-Z0-9]+): .*/, "\\1")
    end
    
    @map_formulae = Hash[*compiler_output.select do |l|
        if l =~ /Creating map/ then 
          n = l.chomp.gsub(/Creating map ([a-zA-Z0-9]+) .*/, "\\1")
          not(compiled_maps.index(n).nil?)
        else false end 
      end.collect do |l|
        
        n,t = l.chomp.gsub(/Creating map ([a-zA-Z0-9]+) for term (.*)/, "\\1 \\2").split(" ",2)

        puts n+": "+t
        
        # Substitutions for variables -> params (i.e. dom vars)
        key_param_subs = Hash.new
        
        # Separate aggregate vs. relational part
        agg,rel_t = t.gsub(/AggSum\((.*)\)/, "\\1").split(",", 2)
        
        t_rels = []
        t_preds = []
        rel_t.split(" and ").each do |t|
          if t =~ /\(/ then t_rels.push(t) else t_preds.push(t) end 
        end

        rels = t_rels.collect do |r|
          # Separate relation names and fields
          rel_n, rel_f = r.gsub(/([a-zA-Z0-9]+)\(([^\)]*)\)/,"\\1 \\2").split(" ",2)
          rel_alias = @map_aliases.key?(rel_n) ? @map_aliases[rel_n] : nil
          fields_and_prefixes = rel_f.split(",").collect do |f|
              x=f.strip;
              prefix = if (p_idx = x.index('__')).nil? then "" else x[0,p_idx] end; 
              [x,prefix]
            end
          
          # Check if prefix matches schema alias, otherwise add predicate
          invalid_indexes = []
          domains = Hash.new
          fields_and_prefixes.each_index do |i|
            f, p = fields_and_prefixes[i]
            if p != rel_alias then invalid_indexes.push(i) end
          end;
          predicates =
            invalid_indexes.collect do |i|
              f,p = fields_and_prefixes[i]
              orig_f = if @schemas.key? rel_n then @schemas[rel_n][i] else "xxx" end;

              (if f =~ /^x_/ then
                attr_idx = f.index('__')-1
                dom_rel_start = f[0,attr_idx].rindex('_')+1
                dom_rel_alias = f[dom_rel_start..attr_idx]
                dom_attr = f[attr_idx+1..-1]
                dom_rel = if @map_aliases.value?(dom_rel_alias)
                  then @map_aliases.index(dom_rel_alias)
                  else dom_rel_alias end

                puts dom_rel
                puts dom_rel_alias

                dom_idx = @schemas[dom_rel].index(dom_rel_alias+dom_attr)
                

                param = "dom_" + dom_rel_alias + dom_attr
                if domains.key?(dom_rel) then
                  domains[dom_rel].push(dom_idx)
                else
                  domains[dom_rel] = [dom_idx]
                end
                # Substitute both the LHS and RHS of the constraint for keys
                key_param_subs[f] = param
                key_param_subs[orig_f] = param
                ":"+param
              else f.sub(/__/, ".") end) + " = " + orig_f.sub(/__/, ".")
            end
          
          # Return [relation name, [fields, predicates, domains]]
          [rel_n, fields_and_prefixes.collect { |f,p| f }, predicates, domains]
      end;
      where_clause = 
        (rels.select { |x| x[2].length > 0 }.collect { |x| x[2] }.join(" and "));
      
      # Map params, which should contain only groupby cols, and domain vars.
      map_id = @map_info.select { |k,v| v["map"] == n }.first[0]
      keys = @templates.select { |t| t.target.source == map_id }.
        first.target.keys.collect do |k|
          if key_param_subs.key?(k) then key_param_subs[k] else k end
        end

      # Map access patterns
      ap = @templates.collect do |t|
          t.access_patterns(map_id).compact.join(",")
        end.select{|x| x.length > 0}.join("|")

      # Group bys
      params = rels.collect do |x|
        x[3].to_a.collect { |r,i| i.collect { |j| "dom_"+@schemas[r][j] }  }
      end.flatten
      
      group_bys = keys.reject { |k| params.include?(k) }

      keys_s = keys.collect do |k|
        if group_bys.include?(k) then "Q."+group_bys.index(k).to_s else k end
      end.join(",")
      
      # Domains (i.e. relations + positions) for map params
      param_sources = rels.collect do |x|
          x[3].to_a.collect { |r,i| i.collect { |j|
            r+"."+(j.to_s)+"=>"+"dom_"+@schemas[r][j] }  }
        end.to_a.select{|x| x.length > 0}.join(",")

      # SQL query
      query = 
        "select "+
          (group_bys.length > 0? group_bys.join(",").gsub(/__/,".")+"," : "") +
          "sum("+agg.gsub(/__/,".")+")"+
        " from "+(rels.collect do |x|
          x[0]+(@map_aliases.key?(x[0]) ? " "+@map_aliases[x[0]] : "")
        end.join(", ")) +
        (where_clause.length > 0 || t_preds.length > 0?
          (" where " + where_clause + t_preds.join(" and ")) : "") +
        (group_bys.length > 0?
          (" group by " + group_bys.join(",").gsub(/__/,".")) : "")

      # Binding vars for parameterized SQL query
      bindvars =
        rels.collect do |x|
          x[3].to_a.collect { |r,i| i.collect { |j| "dom_"+@schemas[r][j] } }
        end.flatten.join(",");

      [n,[param_sources, query, bindvars, keys_s+(ap.length > 0? "/"+ap : "")].join("\n")]
    end.flatten]
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
