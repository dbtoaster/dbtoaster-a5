require "util/ok_mixins"

class TemplateEntry
  attr_reader :source, :keys;
  # index is used by a hack involved in trigger compilation (see switch/maplayout.rb)
  attr_reader :index;
  attr_writer :index;
  alias :key :keys;
  
  def initialize(template, source, *keys)
    @source = source.to_i;
    @keys = keys.flatten.collect do |key|
      case key
        when String then 
          if key.to_i.to_s == key then 
            key.to_i 
          else 
            TemplateVariable.new(key, template);
          end;
        else key
      end
    end
  end
  
  def instantiate(params = nil)
    MapEntry.new(
      @source, 
      instantiated_key(params)
    );
  end
  
  def clone(params = nil)
    TemplateEntry.new(
      nil, # Shouldn't be any strings in the paramlist.
      @source, 
      instantiated_key(params)
    );
  end
  
  def instantiated_key(params = nil)
    @keys.collect do |k|
      case k
        when TemplateVariable then k.to_f(params).to_i
        else k;
      end
    end
  end
  
  def partition(params = nil, partition_size = $config.partition_sizes[@source])
    if(params == nil)
      Array.new(@keys.size, -1);
    else
      NetTypes.computePartition(
        instantiated_key(params),
        partition_size
      ).to_a;
    end
  end
  
  def weak_match?(entry, params = nil)
    return false unless entry.source == @source;
    @keys.each_pair(entry.key) do |a, b|
      case a
        # If the variable isn't bound, let it through
        when TemplateVariable then val = a.to_f(params); return false if (val != -1) && (val != b.to_i);
        else return false if a.to_i != b.to_i;
      end
    end
    return true;
  end
  
  def requires_loop?
    @keys.assert { |dim| dim.is_a? Numeric || dim.type == :param };
  end
  
  def to_s(params = nil)
    "Map " + @source.to_s + "[" + 
      @keys.collect do |key| 
        case key
          when TemplateVariable then key.to_f(params) == -1 ? key.name : key.to_f(params);
          else key.to_s;
        end
      end.join(",") + "]";
  end
  
  def TemplateEntry.parse(template, entry)
    decode(template, Tokenizer.new(entry, /Map|[0-9]+|\[|\]|[a-zA-Z][a-zA-Z0-9_]*|,/));
  end
  
  def TemplateEntry.decode(template, tokenizer)
    # Expects tokens of the form "Map" "source" "[" "key" ("," "key" (...)) "]"
    
    source = # In some cases we get the full expression, in others we don't get "Map".
      if tokenizer.next == "Map" then tokenizer.next else tokenizer.last end;
    
    tokenizer.assert_next("[");
    keys = 
      tokenizer.tokens_up_to("]").delete_if do |key|
        key == ","
      end.collect do |key|
        case key
          when String then if key.to_i.to_s == key.to_s then key.to_i else key end;
          else key;
        end
      end

    source = UpdateTemplate.get_map(source, Math.max(keys.size, 1)).to_s unless source.is_number?
    
    TemplateEntry.new(template, source, keys)  
  end
  
  def ==(other)
    case other
      when TemplateEntry then
        return false unless ((other.source != @source) && (keys.length != other.keys.length));
        @keys.each_index do |i|
          return false if @keys[i] != other.keys[i];
        end
      when MapEntry then
        return false unless ((other.source != @source) && (keys.length != other.key.length));
        @keys.each_index do |i|
          return false if @keys[i] != other.key[i];
        end
      else return false;
    end
    return true;
  end
end

###################################################

class TemplateValuation
  attr_reader :params, :template;
  
  def initialize(template, params, entries = nil)
    @template, @params = template, params.to_a;
    @instance = Array.new(template.entries.size, 0);
    @entry_values = template.entries.collect { |entry| [entry, Array.new] };
#    if template.paramlist.length + template.varlist.length > @params.length then
#      @params = @params.concat(Array.new(template.paramlist.length + template.varlist.length - @params.length, nil))
#    end
  end
  
  def [](entry)
    entry_column = @entry_values[entry];
    instance = @instance[entry];
    entry_element = entry_column[1][instance];
    raise "Incomplete valuation: missing #{entry} (in instance [#{@instance.join(",")}], values: [#{entry_column[1].join(",")}])" unless entry_element;
#    raise "Incomplete valuation";
    entry_element ? entry_element[1] : nil;
  end
  
  def discover(entry, value)
    found = false;
    @entry_values.each do |parametrization|
      found = true;
      parametrization[1].push([entry,value]) if(parametrization[0].weak_match?(entry, @params));
    end
    CLog.warn { "Discovered #{entry} = #{value}, but no match in: #{@entry_values.collect {|e| e[0]}.join(",")}" } unless found;
  end
  
  def ready?
    @entry_values.assert { |entry| entry[1].size > 0 }
  end
  
  def to_f
    @template.to_f(self)
  end
  
  def to_s
    @template.expression.to_s(self);
  end
  
  def target
    @target = @template.target.instantiate(@params) if @target.nil?
    @target;
  end
  
  def foreach
    orig_instance = @instance;
    @entry_values.collect { |parametrization| (0...parametrization[1].size) }.each_cross_product do |parametrization|
      @instance = parametrization;
      
      # extract loop variables from the currently active entry set
      @template.loopvarlist.each do |loopentry|
        loopentry[1].each do |replacement|
          @params[replacement[1].ref] = self[loopentry[0]].key[replacement[0]];
        end
      end
      
      # and instantiate the value 
      yield @template.target.instantiate(@params), @template.to_f(self);
    end
    @instance = orig_instance;
  end
end

###################################################

class TemplateExpression
  def initialize(op, left = nil, right = nil)
    @op, @left, @right = op, left, right;
  end
  
  def entries(list = Array.new)
    case @op
      when :plus, :mult, :sub, :div then @left.entries(@right.entries(list));
      when :map                     then list.push(@left);
      when :val                     then list;
      else            raise SpreadException.new("Unknown Expression operator (entries): " + @op.to_s);
    end
  end
  
  # See TemplateValuation.initialize for a description of what loop_vars does.
  def loop_vars(template)
    template.entries.collect do |entry|
      # see if any keys have not been bound (either explicitly, or via known_vars)
      entry unless entry.keys.assert { |key| (key.is_a? Numeric) || (key.type == :param) }
    end.compact.collect do |entry|
      [
        template.entries.index(entry), 
        entry.key.collect_index { |k| [i,k] if (k.is_a? TemplateVariable) && (key.type == :var) }.compact
      ]
    end
  end
  
  def to_f(params = nil)
    case @op
      when :plus then @left.to_f(params) + @right.to_f(params);
      when :mult then @left.to_f(params) * @right.to_f(params);
      when :sub  then @left.to_f(params) - @right.to_f(params);
      when :div  then @left.to_f(params) / @right.to_f(params);
      when :val  then
        case @left
          when TemplateVariable then @left.to_f(params ? params.params : nil)
          else                       
            if @right.nil? then @left.to_f else 
              raise SpreadException("Unknown parameter value") if params.nil?;
              params.params[@right];
            end
        end
      when :map  then params[@right];
      else            raise SpreadException.new("Unknown Expression operator (to_f): " + @op.to_s);
    end
  end
  
  def to_s(params = nil)
    case @op
      when :plus      then "(" + @left.to_s(params) + ")+(" + @right.to_s(params) + ")";
      when :mult      then "(" + @left.to_s(params) + ")*(" + @right.to_s(params) + ")";
      when :sub       then "(" + @left.to_s(params) + ")-(" + @right.to_s(params) + ")";
      when :div       then "(" + @left.to_s(params) + ")/(" + @right.to_s(params) + ")";
      when :val       then
        case @left
          when TemplateVariable then if params.to_f(@left) == -1 then params.name else params.to_f(@left) end;
          else @left.to_s
        end
      when :map       then @left.to_s(params ? params.params : params);
      else            raise SpreadException.new("Unknown Expression operator (to_s): " + @op.to_s);
    end
  end
  
  def TemplateExpression.decode_var(template, tokenizer)
    case tokenizer.next
      when "(" then decode(template, tokenizer);
      when "+","*","/",")" then 
        raise SpreadException.new("Parse Error: Found "+tokenizer.last+" instead of rval");
      when "-" then
        raise SpreadException.new("Parse Error: Found - instead of rval") unless tokenizer.next.is_number?;
        TemplateExpression.new(:val, tokenizer.last.to_i * -1);
      when "Map" then
        entry = TemplateEntry.decode(template, tokenizer);
        template.entries.push(entry);
        TemplateExpression.new(:map, entry, template.entries.size-1);
      else
        if tokenizer.last.is_number? then 
          TemplateExpression.new(:val, tokenizer.last.to_f)
        else
          param = tokenizer.last.to_s
          param_idx = template.paramlist.index(param)
          raise SpreadException.new("Parse Error: No field "+param+" found in "+template.paramlist.join(",")) unless param_idx;
          TemplateExpression.new(:val, param, param_idx)
        end 
    end
  end
  
  def TemplateExpression.decode(template, tokenizer, left = decode_var(template, tokenizer))
    op = case tokenizer.next;
      when "+" then :plus
      when "-" then :sub
      when "*" then :mult
      when "/" then :div
      when ")",nil then return left;
      else raise "Parse Error (Expected op, got '#{tokenizer.last}')";
    end
    right = decode_var(template, tokenizer);
    
    if tokenizer.more? then decode(template, tokenizer, TemplateExpression.new(op, left, right))
    else                    TemplateExpression.new(op, left, right);
    end
  end
  
  def TemplateExpression.parse(template, line)
    decode(
      template,
      Tokenizer.new(
        line, 
        /\(|\)|\+|-|\/|\*|[0-9]*\.[0-9]+|[0-9]+|[a-zA-Z][a-zA-Z0-9_]*|Map|\[|\]|,/
      )
    );
  end
end

###################################################

class TemplateCondition
  attr_reader :type, :left, :right;
  
  def initialize(template, line)
    parsed = line.scan(/<=|<>|<|=|[^<>=]+/);
    raise SpreadException.new("Condition: " + line + " contains " + parsed.size.to_s + " != 3 terms") unless parsed.size == 3;
    @type = 
      case parsed[1]
        when "<=" then :lessthanequal
        when "<>" then :notequal
        when "<"  then :lessthan
        when "="  then :equal
        else raise SpreadException.new("Condition: " + line + " of invalid type: " + parsed[1]);
      end
    @left  = TemplateExpression.parse(template, parsed[0]);
    @right = TemplateExpression.parse(template, parsed[2]);
  end
  
  def to_b(params = TemplateValuation.new)
    case @type
      when :lessthanequal then @left.to_f(params) <= @right.to_f(params);
      when :notequal      then @left.to_f(params) != @right.to_f(params);
      when :lessthan      then @left.to_f(params) <  @right.to_f(params);
      when :equal         then @left.to_f(params) == @right.to_f(params);
      else raise SpreadException.new("Corrupt condition type: " + @type.to_s);
    end
  end
  
  def entries
    @left.entries.concat(@right.entries);
  end
  
  def to_s
    @left.to_s + 
    case @type
      when :lessthanequal then "<="
      when :notequal      then "<>"
      when :lessthan      then "<"
      when :equal         then "="
    end +
    @right.to_s;
  end
end

###################################################

class TemplateConditionList
  attr_reader :conditions;
  
  def initialize(template, line)
    @conditions = line.split(/ *AND */).collect do |cond|
      TemplateCondition.new(template, cond);
    end
  end
  
  def to_b(params = TemplateValuation.new)
    @conditions.each do |cond|
      unless cond.to_b(params) then return false; end;
    end
    return true;
  end
  
  def entries
    @conditions.collect do |cond|
      cond.entries;
    end.flatten
  end
  
  def to_s
    @conditions.join(" AND ");
  end
end

###################################################

class TemplateVariable
  attr_reader :ref, :type, :name
  def initialize(name, template)
    @name = name
    if template.paramlist.include? name then
      @ref = template.paramlist.index(name);
      @type = :param;
    elsif template.varlist.include? name then
      @ref = template.varlist.index(name) + template.paramlist.length;
      @type = :var;
    else
      template.varlist.push(name);
      @ref = template.varlist.index(name) + template.paramlist.length;
      @type = :var;
    end
  end
  
  def to_f(params = nil)
    return -1 if params == nil;
    return -1 if params.length <= @ref;
    return params[@ref];
  end
  
  def to_s(params = nil)
    return "#{name}:#{to_f(params)}"
  end
end

###################################################

class UpdateTemplate
  attr_reader :relation, :paramlist, :loopvarlist, :target, :conditions, :expression, :index, :varlist, :entries;
  attr_writer :index;
  @@map_names = Hash.new;
  @@map_id = 0;
  
  def initialize(text_line, index = 0)
    line = text_line.split("\t");
    raise "Instantiating update template with insufficient components: #{text_line}" if line.size < 5;
    
    @entries = Array.new;
    @allocation_grid = nil;
    
    begin
      # This template applies to updates to [0]
      @relation = line[0].to_s;
      # This template is parametrized by the parameters listed in the comma delimited list [1]
      @paramlist = line[1].split(";").collect do |k| k.gsub(/ *([^ ]) */, "\\1") end;
      @varlist = Array.new;
      # The target's Map Entry (template) is [2]
      @target = TemplateEntry.parse(self, line[2]);
      # The conditions for the target to apply are [3]
      @conditions = TemplateConditionList.new(self, line[3]);
      # The expression for the update is [4]
      @expression = TemplateExpression.parse(self, line[4]);
    rescue Exception => e
      puts e.backtrace.join("\n");
      raise e;
    end
    
    # loopvarlist deserves a little discussion.  This is essentially a pre-computed
    # datastructure that streamlines computation of the domain of each loop variable
    # from the corresponding entries that arrive at a node.  This list is an Array
    # of rules,  Array elements the form:
    # [ TemplateEntry, [ [KEYINDEX, LOOPVAR], ... ] ]
    # Every rule corresponds to one or more loop variables; Specifically, each rule
    # corresponds to all the loop variables for a particular instance of a map on the RHS
    # of the expression. 
    # Under normal processing, the TemplateEntry is matched against incoming Entries using 
    # weak_match? to determine if this rule is applicable.  If it is, then the KEYINDEXth 
    # element of the entry's key is identified as a member of LOOPVAR's domain.  It is 
    # possible for there to be multiple KEYINDEX/LOOPVAR pairs.   
    @loopvarlist = @expression.loop_vars(self);
    
    @index = index;
  end
  
  def valuation(params)
    return TemplateValuation.new(self, params);
  end
  
  def add_expression(expression)
    @expression = TemplateExpression.new(:plus, @expression, expression);
  end
  
  def requires_loop?
    !@loopvarlist.empty?
  end
  
  def param_map(param_inputs)
    @paramlist.merge(param_inputs).collect_hash;
  end
  
  def to_f(params = TemplateValuation.new)
    if @conditions.to_b(params) then
      @expression.to_f(params)
    else
      0
    end
  end
  
  def to_s
    @relation + "\t" +
      @paramlist.join(";") + "\t" +
#      @loopvarlist.collect do |e| e[0].to_s end.join(";") + "\t" + 
      @target.to_s + "\t" +
      @conditions.to_s + "\t" +
      @expression.to_s;
  end
  
  def summary
    "ON " + @relation + " : Map " + target.source.to_s + " <- " + entries.collect { |e| "Map " + e.source.to_s }.join(", ");
  end
  
  def access_patterns(map)
    @expression.entries.collect do |entry|
      if entry.source == map then
        CLog.debug { "Map " + map.to_s + ": " + entry.keys.collect_index { |i,k| i if k.type == :param }.compact.join(",") }
        entry.keys.collect_index { |i,k| i if k.type == :param }.compact
      end
    end.compact;
  end
  
  def compute_allocation_grid
    return if @allocation_grid;
    
    # Allocation grid is an n-dimensional hypertable; Every possible valuation of the
    # underlying relation maps to one specific cell in the allocation grid; and the
    # contents of that cell are the set of nodes that need to push values to perform
    # this template.
    @allocation_grid_sizes = Array.new(@paramlist.size+@varlist.size, 1)
    [target].concat(@entries).each do |entry|
      entry.key.zip($config.partition_sizes[entry.source]).each do |dim|
        if dim[0].is_a? TemplateVariable then
          @allocation_grid_sizes[dim[0].ref] = Math.max(@allocation_grid_sizes[dim[0].ref], dim[1]);
        end
      end
    end
    count = 1;
    @allocation_grid_sizes.each { |s| count *= s; }
    CLog.debug { "Creating allocation grid for template #{@index}(#{@summary}); #{count} entries" }
    @allocation_grid = Hash.new
    @allocation_grid_sizes.collect { |size| (0...size) }.each_cross_product do |global_partition|
      CLog.trace { "allocation grid [#{global_partition.join(",")}] : " }
      grid_cell = Array.new
      @allocation_grid[global_partition] = grid_cell;
      @entries.each do |entry|
        entry_partition = entry.key.zip($config.partition_sizes[entry.source]).collect do |dim|
          if(dim[0].is_a? Numeric) then 
            dim[0] % dim[1];
          else
            global_partition[dim[0].ref] % dim[1];
          end
        end
        
        entry_owner = $config.partition_owners[entry.source][entry_partition];
        CLog.trace { "   requires #{entry} @ #{entry_owner}" }
        grid_cell.push(entry_owner) unless grid_cell.include? entry_owner;
      end
    end
  end
  
  def project_param(entry, term_list)
    (0...@paramlist.size).collect do |param|
      entry.key.zip(term_list).find do |dim|
        dim[1].to_i if (dim[0].is_a? TemplateVariable) && (dim[0].type == :param) && (dim[0].ref == param)
      end || nil;
    end
  end
  
  def constants_in_partition_are_local(entry, map_partitions)
    # this function verifies that any constants in the target key are potentially present locally.
    # if that's not true (eg, for q[] on anything but the first node), then this template will never be 
    # triggered locally and we can ignore it.
    entry.key.zip((0...entry.key.size).to_a).assert do |dim| 
      (dim[0].is_a? TemplateVariable) || 
      map_partitions.find { |part| part[dim[1]] == dim[0] % $config.partition_sizes[entry.source][dim[1]] }
    end
  end
  
  def project_grid_to_entry(entry, partition)
    projection = Hash.new;
    @allocation_grid.each_pair do |cell_partition, grid_cell| 
      if partition.zip(entry.instantiated_key(cell_partition), $config.partition_sizes[entry.source]).assert do |dim|
        dim[0] == (dim[1] % dim[2]);
      end then
        projection[cell_partition] = grid_cell;
      end
    end
    projection;
  end
  
  def nodes_grouped_by_relation(grid)
    targets = Hash.new { |h,k| h[k] = Array.new };
    grid.each_pair do |grid_partition, destination_nodes|
      grouped_by = targets[grid_partition[0...@paramlist.size]]
      destination_nodes.each { |node| grouped_by.push(node) unless grouped_by.include? node }
    end
    targets;
  end
  
  def compile_to_local(program, map_partitions)
    compute_allocation_grid;
    
    return unless map_partitions[@target.source];
    
    CLog.debug { "Compiling Local Instance of ##{@index}: #{summary}" }
    if constants_in_partition_are_local(@target, map_partitions) then
      put_message = program.installPutComponent(
        @relation, self, @index, @allocation_grid_sizes[0...@paramlist.size]
      );
      map_partitions[@target.source].each do |partition|
        # figure out when we need to turn this update into a put.
        targets = 
        nodes_grouped_by_relation(project_grid_to_entry(@target, partition)).each_pair do |cell_partition, source_nodes|
          source_nodes = source_nodes.clone;
          source_nodes.delete($config.my_config["address"]);
#          CLog.debug { "  me == #{$config.my_config["address"]}" }
          CLog.debug { "  triggers put on: [#{cell_partition.collect{|e| e || "*"}.join(",")}] with gets from: #{source_nodes.join(",")}" }
          put_message.condition.addPartition(cell_partition, source_nodes.size)
        end
      end
    end
      
    #figure out when this update results in us sending data.
    @entries.each do |entry|
      if constants_in_partition_are_local(entry, map_partitions) then
        target_nodes = Hash.new;
        $config.nodes.each_pair do |node, node_info|
          node_info["partitions"][entry.source].each do |partition|
            target_nodes[partition] = node_info["address"];
          end
        end

        fetch_message = program.installFetchComponent(
          @relation, entry, @index, @target, 
          @allocation_grid_sizes[0...@paramlist.size], 
          entry.keys.zip((0...entry.key.size).to_a).collect { |dim| [dim[1], dim[0].ref] if(dim[0].is_a? TemplateVariable) && dim[0].type == :var }.compact,
          target_nodes
        );
        CLog.debug { "  will send push for Map #{entry.source} <- {#{fetch_message.entry_mapping.collect{|mapping| mapping.join(" := ")}.join(", ")}}" }
        map_partitions[entry.source].each do |partition|
          partition_targets = Hash.new { |h,k| h[k] = Array.new };
          project_grid_to_entry(entry, partition).keys.each do |grid_partition|
            destination_partition = 
              @target.instantiated_key(grid_partition).zip($config.partition_sizes[@target.source]).collect do |dim| 
                dim[0] % dim[1];
              end
            CLog.trace { "    ... considering {#{grid_partition.join(",")}} -> Map #{@target.source}[#{destination_partition.join(",")}] @ #{$config.partition_owners[@target.source][destination_partition]}" }
            
            unless ($config.partition_owners[@target.source][destination_partition] == $config.my_config["address"]) ||
                   (partition_targets[grid_partition[0...@paramlist.size]].include? $config.partition_owners[@target.source][destination_partition])
            then   partition_targets[grid_partition[0...@paramlist.size]].push($config.partition_owners[@target.source][destination_partition])
            end
          end
          partition_targets.each_pair do |grid_partition, targets|;
            CLog.debug { "    ... will send on: [#{grid_partition.join(",")}] to #{targets.join(", ")}" }
            raise "Error: NIL in Fetch" if targets.find { |t| t.nil? };
            fetch_message.condition.addPartition(grid_partition, targets);
          end
        end
      end
    end
  end
  
  def UpdateTemplate.get_map(map_name, params)
    @@map_names.assert_key(map_name) { {"id" => @@map_id += 1, "params"=> params}; }
    @@map_names[map_name]["id"];
  end
  
  def UpdateTemplate.map_names
    @@map_names;
  end
end
