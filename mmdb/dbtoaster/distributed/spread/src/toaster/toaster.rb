
require 'ok_mixins';
require 'template'

class DBToaster
  attr_reader :compiled, :templates, :map_info, :test_directives, :slice_directives, :persist;

  def initialize(toaster_cmd = "./dbtoaster.top -noprompt 2> /dev/null", toaster_dir = File.dirname(__FILE__) + "/../../../../prototype/compiler/alpha3")
    @nodes = Array.new;
    @partition_directives = Hash.new;
    @domain_directives = Hash.new;
    @test_directives = Array.new;
    @slice_directives = Array.new;
    @keys = Hash.new;
    @persist = false;
    @compiled = nil;
    
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
        @nodes.push(/ *([a-zA-Z0-9_]+) *@ *([a-zA-Z_\-0-9]+)(:([0-9]+))?/.match(arg).map(["name", "address", "dummy", "port"]));
      when "--partition" then 
        arg = arg.split(":");
        @partition_directives[arg[0]] = arg[1];
      when "--domain"    then
        arg = arg.split("=");
        @domain_directives[arg[0]] = arg[1].split(/, */).collect { |d| if d == "*" then nil else d end };
      when "--test"      then @test_directives.push(arg);
      when "--slice"     then @slice_directives.push(arg);
      when "--persist"   then @persist = true;
      when "--key"       then 
        match = / *([^ \[]+) *\[ *([^\]]+) *\] *<= *([^ \[-]+) *\[ *([^\]]+) *\]/.match(arg)
        raise "Invalid key argument: " + arg unless match;
        @keys.assert_key(match[1]){ Hash.new }.assert_key(match[2]){ Hash.new }[match[3]] = [match[4]];
  #    else raise "Unknown option: " + opt;
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
      toast_dag;
      toast_keys if opts.fetch(:toast_keys, true);
      toast_domains;
      @DBT = nil;
    rescue Exception => e
      #puts "Error toasting.  Compiler output: \n" + data.join("");
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
        domain = @domain_directives.fetch(map) { Array.new(info["params"], nil) };
        raise "Domain with invalid dimension.  Map: " + map + "; Expected: " + info["params"].to_s + "; Saw: " + domain.size.to_s unless info["params"].to_i == domain.size;
        
        split_partition = @partition_directives.assert_key(map) do
          # This would be a good place to figure out if we have any keys we can split on.  Hint, hint.
          0
        end;
        raise "Split Partition too big.  Asked to split on: " + split_partition.to_s + "; Size: " + domain.size.to_s unless (split_partition.to_i < domain.size) || (domain.size == 0);
        
        [ info["id"].to_i,
          { "map"        => map, 
            "id"         => info["id"].to_i, 
            "in_domain"  => domain.collect{|d| d.to_i}, 
            "domain"     => domain,
            "partition"  => split_partition.to_i,
            "reads_from" => Array.new,
            "writes_to"  => Array.new,
            "discarded"  => false
          }
        ]
      end.collect_hash
  end  
  
  def toast_dag
    # Now compute the message passing DAG from the templates
    @map_info.each_value do |info|
      @templates.each do |template|
        if template.target.source == info["id"] then
          # If the template's target is the map we're looking at, figure out what it reads from.
          template.entries.each do |entry|
            info["reads_from"].push(entry.source);
          end
        end
      end
    end
    
    # And the inverse DAG
    @map_info.each_pair do |map, info|
      info["reads_from"].each do |read|
        @map_info[read]["writes_to"].push(map);
      end
    end
    
    # Precompute the DAG depth of each map
    @map_info.each_key do |map| map_depth(map) end
  end
  
  def toast_keys
    # Cascade foreign key dependencies; If in R[A,B] x S[B,C] x T[C,D]
    # both B and C are keys, used as foreign keys in S and T respectively
    # then we need the closure of the dependencies
    @key_deps = Hash.new
    @keys.each_pair do |map, key_list|
      key_list.each_value do |refs|
        refs.each_pair do |ref_map, ref_keys|
          @key_deps.assert_key(map){ Hash.new }.assert_key(ref_map){ Array.new }.concat(ref_keys.clone)
        end
      end
    end
    inserting = true;
    while inserting
      inserting = false;
      @key_deps.each_pair do |map,reference_list|
        puts "Checking M: " + map;
        reference_list.each_key do |ref_map|
          puts "    Checking RM: " + ref_map;
          if @key_deps.has_key? ref_map then
            @key_deps[ref_map].each_pair do |ref_ref_map, ref_ref_keys|
              ref_ref_keys.each do |ref_ref_key|
                puts "      Checking RK: " + ref_ref_key
                if !@key_deps[map].assert_key(ref_ref_map){ Array.new }.include? ref_ref_key then
                  inserting = true;
                  puts "        Creating new dependency " + map + "<= " + ref_ref_map + "[" + ref_ref_key + "]";
                  @key_deps[map][ref_ref_map].push(ref_ref_key)
                end
              end
            end
          end
        end
      end
    end

    useless_maps = [nil];
    until useless_maps.empty? do
      
      # We might be able to eliminate some update rules based on foreign 
      # key dependencies (which we assume all cascade).  Specifically, 
      # consider an update rule for relation X.  If relation X has one 
      # or more keys, then (as the result of foreign key constraints) 
      # some of the rules generated by toaster will reference maps 
      # (indexed by the same key) that will always read 0.  This occurs 
      # when all of the rules that update the map being read from, are 
      # triggered by relations that include foreign key constraints on 
      # the key.
      
      # NOTE: For simplicity, we assume in this implementation that the
      # updates are of the form A*B*C*...  That is, if any entry reads 0
      # then the entire update is moot.
      
      # Start by recreating the DAG based on the templates.
      templates_by_writer = template_dag;
      
      templates_by_writer.each_pair do |map, templates|
        puts "Maps written by template: " + map.to_s + " <= " + templates.collect { |template| template.index.to_s }.join(", ");
      end
      
      # Now, identify any delete-worthy entries.
      @templates.delete_if do |template|
        puts "Checking to see if I can remove template " + template.index.to_s + " for relation " + template.relation ;
        if (@key_deps.has_key? template.relation) && (template.entries.size > 0) then
          # If this update involves any entries, then see if we can find 
          # one that matches our criterion for removal
          removable = 
            template.entries.find do |e|
              # The entry is always 0 if for each template that writes to the 
              # map in question, one of the current rule's relation's keys is
              # used to index into that map as a foreign key.
              
              # See if we can find a template that violates this constraint
              puts "  Checking entry: " + e.to_s;
              not templates_by_writer[e.source.to_i].find do |writer|
                # See if we can find a key that causes the template NOT to
                # violate the constraint
                puts "    Checking potential writer (" + writer.index.to_s + "): " + writer.relation.to_s + " => " + writer.target.to_s;
                (!@key_deps[template.relation].has_key? writer.relation) ||
                  (!@key_deps[template.relation][writer.relation].find { |k| writer.target.keys.include? k })
              end
            end
          puts "  I decided I can remove template " + template.index.to_s if removable;
          removable;
        end
      end
      
      # Finally, we may have removed the need for some or all maps.
      useless_maps = 
        @map_info.values.collect do |map_info|
          if map_info["discarded"] || map_info["map"] == "q" then nil
          else
            if @templates.find do |template|
              template.entries.find do |entry|
                entry.source.to_i == map_info["id"].to_i;
              end
            end then nil else map_info["id"].to_i end          
          end
        end.compact;
      puts "I found some maps I don't care about any more: " + useless_maps.collect{ |m| @map_info[m]["map"] }.join(", ") unless useless_maps.empty?;
      @templates.delete_if { |template| useless_maps.include? template.target.source.to_i }
      useless_maps.each { |m| @map_info[m]["discarded"] = true }
      
      # aaaaand... we may have some more dependencies to get rid of... so try again.
    end #until useless_maps.empty? 
  end
  
  def toast_domains
    # Start by extracting each map's domains from the template's inputs
    @templates.collect do |template|
      if @domain_directives.has_key? template.relation then template else nil end;
    end.compact.each do |template|
      template.target.keys.each_with_index do |i, key|
        if template.paramlist.include? key then
          if @map_info[template.target.source]["domain"][i] == nil && @domain_directives[template.relation][template.paramlist.index(key)] then
            #puts @map_info[template.target.source]["map"].to_s + "[" + key.to_s + ":" + i.to_s + "] = " + @domain_directives[template.relation][template.paramlist.index(key)].to_s
            @map_info[template.target.source]["domain"][i] = @domain_directives[template.relation][template.paramlist.index(key)].to_i;
          elsif @map_info[template.target.source]["domain"][i] && !@domain_directives[template.relation][template.paramlist.index(key)]
            raise "Error: Incompatible domain for map " + @map_info[template.target.source]["map"].to_s + ", key " + i.to_s + "; Template " + template.index.to_s + " says it is " + @domain_directives[template.relation][i].to_s + ", but prior declarations said " + @map_info[template.target.source]["domain"][i].to_s unless @domain_directives[template.relation][i].to_i == @map_info[template.target.source]["domain"][i].to_i;
          end
        end
      end
    end
    
    # Now propagate those domains through the templates.
    changing = true;
    while changing
      changing = false;
      
      @templates.each do |template|
        template.target.keys.each_with_index do |i, key|
          # Only need to propagate loop variables that haven't been given a domain yet.
          if @map_info[template.target.source]["domain"][i].nil? then
            #puts "Checking for updates to " + @map_info[template.target.source]["map"] + "[" + key + "] from template " + template.index.to_s
            template.entries.each do |entry|
              #puts "  " + @map_info[entry.source]["map"] + "[" + entry.keys.join(",") + "]";
              if entry.keys.include? key then
                ei = entry.keys.index(key)
                #puts "    Found! (@" + ei.to_s + "; " + @map_info[entry.source]["domain"].join(",") + ")";
                unless @map_info[entry.source]["domain"][ei].nil? then
                  #puts @map_info[template.target.source]["map"].to_s + "[" + key.to_s + ":" + i.to_s + "] = " + @map_info[entry.source]["domain"][ei].to_s;
                  @map_info[template.target.source]["domain"][i] = @map_info[entry.source]["domain"][ei];
                  changing = true;
                end
              end
            end
          end
        end
      end
    end
    
    # Finally, assign defaults to all other domains
    @map_info.each_value do |info|
      info["domain"].collect! do |d|
        if d.nil? then 2147483647 else d.to_i end;
      end
    end
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
      partitions = Hash.new;
      @map_info.each_value do |map|
        unless map["discarded"] then
          if (map["domain"].size == 0) then
            if node_index == 0 then 
              partitions.assert_key(map["id"].to_i) { Array.new }.push([(0...2)]);
            end
          else
            partitions.assert_key(map["id"].to_i) { Array.new }.push(
              map["domain"].collect_index do |i, d|
                if i == map["partition"] then step = (d / @nodes.size); ((step * node_index)...((step * (node_index+1))))
                else (0...d)
                end
              end
            );
          end
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