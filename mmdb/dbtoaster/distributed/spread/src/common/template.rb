
require 'thrift';
require 'spread_types';

class Tokenizer
  def initialize(string, token)
    @tokens = string.scan(token);
    @last = nil;
  end
  
  def scan
    while @tokens.size > 0
      if !(yield @tokens.shift) then break; end
    end
  end
  
  def next
    @last = 
      if @tokens.size > 0 then @tokens.shift
      else nil; end
  end
  
  def last
    @last;
  end
  
  def more?
    @tokens.size > 0;
  end
  
  def flatten
    @tokens = @tokens.flatten;
  end
  
  def assert_next(token, errstr = nil)
    if self.next != token then
      errstr = "Parse Error: Expected '" + token.to_s + "' but found '" + last.to_s + "'" unless errstr != nil;
      raise SpreadException.new(errstr);
    end
  end
  
  def tokens_up_to(token)
    ret = Array.new;
    while (more? && (self.next != token))
      ret.push(last);
    end
    ret;
  end
end

###################################################

class TemplateEntry
  attr_reader :source, :keys;
  # index is used by a hack involved in trigger compilation (see switch/maplayout.rb)
  attr_reader :index;
  attr_writer :index;
  alias :key :keys;
  
  def initialize(source, *keys)
    @source = source.to_i;
    @keys = keys.flatten.collect do |key|
      case key
        when String then if key.to_i.to_s == key then key.to_i else key end;
        else key
      end
    end
  end
  
  def instantiate(params = Hash.new)
    Entry.make(
      @source, 
      @keys.collect do |k|
        case k
          when String then if params.has_key? k then params[k].to_i else -1 end;
          else k;
        end
      end
      );
  end
  
  def clone(params = Hash.new)
    TemplateEntry.new(
      @source, 
      @keys.collect do |k|
        case k
          when String then if params.has_key? k then params[k] else k end;
          else k;
        end
      end
    );
    
  end
  
  def weak_match?(entry, params = Hash.new)
    return false unless entry.source == @source;
    @keys.each_pair(entry.key) do |a, b|
      case a
        # If the variable isn't bound, let it through
        when String then return false if (params.has_key? a) && (params[a].to_i != b.to_i);
        else return false if a.to_i != b.to_i;
      end
    end
    return true;
  end
  
  def to_s(params = Hash.new)
    "Map " + @source.to_s + "[" + 
      @keys.collect do |key| 
        case key
          when String then if params.has_key? key then params[key].to_i.to_s else key end;
          else key.to_s;
        end
      end.join(",") + "]";
  end
  
  def TemplateEntry.parse(entry)
    decode(Tokenizer.new(entry, /Map|[0-9]+|\[|\]|[a-zA-Z][a-zA-Z0-9_]*|,/));
  end
  
  def TemplateEntry.decode(tokenizer)
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
    
    TemplateEntry.new(source, keys)  
  end
  
  def ==(other)
    case other
      when TemplateEntry then
        return false unless ((other.source != @source) && (keys.length != other.keys.length));
        @keys.each_index do |i|
          return false if @keys[i] != other.keys[i];
        end
      when Entry then
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
  attr_reader :params, :entries, :template;
  
  def initialize(template = nil, params = Hash.new, entries = Hash.new)
    @template, @params, @entries = template, params, entries;
    if entries.is_a? Array then
      @entries = Hash.new;
      prepare_entries(entries);
    end
    prepare_entries(template.entries) unless template == nil;
    @target = nil;
  end
  
  def param(key)
    return @params[key];
  end
  
  def entry(e)
    return @entries[e]
  end
  
  def has_key?(key)
    @params.has_key? key
  end
  
  def has_entry?(e)
    @entries.has_key? e
  end
  
  def value(key)
    case key
      when String then
        case param(key)
          when nil           then raise SpreadException.new("Converting to float with undefined parameter: " + key.to_s);
          when TemplateEntry then entry(param(key).instantiate(@params)).to_f;
          when Entry         then entry(param(key)).to_f
          else                    param(key).to_f;
        end
      when Numeric then
        key.to_f
      else raise SpreadException.new("Attempting to evaluate unknown variable type: " + key.class.to_s + " = " + key.to_s);
    end
  end
  
  def prepare_entries(*entries)
    entries.flatten.each do |e|
      e = e.instantiate(@params) if e.is_a? TemplateEntry;
      @entries[e] = nil unless @entries.has_key? e;
    end
  end
  
  def prepare(*entries)
    prepare_entries(entries);
  end
  
  def discover(entry, value)
    if (@entries.has_key? entry) then
      @entries[entry] = value;
      @target = nil;
    end
  end
  
  def clone(temp_vars = Hash.new)
    TemplateValuation.new(@template, @params.merge(temp_vars), @entries.clone);
  end
  
  def to_f
    @template.to_f(self)
  end
  
  def to_s
    @template.expression.to_s(self);
  end
  
  def target
    @target = @template.target.instantiate(self.params) if @target.nil?
    @target;
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
  def loop_vars(known_vars, list = Array.new)
    entries.delete_if do |entry|
      # see if any keys have not been bound (either explicitly, or via known_vars)
      entry.keys.clone.delete_if do |key| (key.is_a? Numeric) || (known_vars.include? key) end.empty?
    end.collect do |entry|
      [
        entry, 
        entry.key.collect_index do |i, k| 
          if (k.is_a? String) && !(known_vars.include? k) then
            [i, k]
          else 
            nil;
          end
        end.delete_if do |e| e.nil? end
      ]
    end
  end
  
  def to_f(params = TemplateValuation.new)
    case @op
      when :plus then @left.to_f(params) + @right.to_f(params);
      when :mult then @left.to_f(params) * @right.to_f(params);
      when :sub  then @left.to_f(params) - @right.to_f(params);
      when :div  then @left.to_f(params) / @right.to_f(params);
      when :val  then 
        case @left
          when String then params.value(@left).to_f
          else             @left.to_f;
        end
      when :map  then params.entry(@left.instantiate(params.params)).to_f;
      else            raise SpreadException.new("Unknown Expression operator (to_f): " + @op.to_s);
    end
  end
  
  def to_s(params = TemplateValuation.new)
    case @op
      when :plus      then "(" + @left.to_s(params) + ")+(" + @right.to_s(params) + ")";
      when :mult      then "(" + @left.to_s(params) + ")*(" + @right.to_s(params) + ")";
      when :sub       then "(" + @left.to_s(params) + ")-(" + @right.to_s(params) + ")";
      when :div       then "(" + @left.to_s(params) + ")/(" + @right.to_s(params) + ")";
      when :val       then
        case @left
          when String then if params.has_key? @left then params.value(@left).to_s else @left end;
          else @left.to_s
        end
      when :map       then @left.to_s(params.params);
      else            raise SpreadException.new("Unknown Expression operator (to_s): " + @op.to_s);
    end
  end
  
  def TemplateExpression.decode_var(tokenizer)
    case tokenizer.next
      when "(" then decode(tokenizer);
      when "+","*","/",")" then 
        raise SpreadException.new("Parse Error: Found "+tokenizer.last+" instead of rval");
      when "-" then
        raise SpreadException.new("Parse Error: Found - instead of rval") unless tokenizer.next.is_number?;
        TemplateExpression.new(:val, tokenizer.last.to_i * -1);
      when "Map" then
        TemplateExpression.new(:map, TemplateEntry.decode(tokenizer));
      else
        TemplateExpression.new(
          :val,
          if tokenizer.last.is_number? then tokenizer.last.to_f else tokenizer.last.to_s end
        );
    end
  end
  
  def TemplateExpression.decode(tokenizer, left = decode_var(tokenizer))
    op = case tokenizer.next;
      when "+" then :plus
      when "-" then :sub
      when "*" then :mult
      when "/" then :div
      when ")",nil then return left;
      else raise SpreadException.new("Parse Error (Expected op, got '"+tokenizer.last.to_s+"')");
    end
    right = decode_var(tokenizer);
    
    if tokenizer.more? then decode(tokenizer, TemplateExpression.new(op, left, right))
    else                    TemplateExpression.new(op, left, right);
    end
  end
  
  def TemplateExpression.parse(line)
    decode(
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
  
  def initialize(line)
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
    @left  = TemplateExpression.parse(parsed[0]);
    @right = TemplateExpression.parse(parsed[2]);
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
  
  def initialize(line)
    @conditions = line.split(/ *AND */).collect do |cond|
      TemplateCondition.new(cond);
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

class UpdateTemplate
  attr_reader :relation, :paramlist, :loopvarlist, :target, :conditions, :expression, :index;
  attr_writer :index;
  @@map_names = Hash.new;
  @@map_id = 0;
  
  def initialize(text_line, index = 0)
    line = text_line.split("\t");
    raise SpreadException.new("Instantiating update template with insufficient components: " + text_line) if line.size < 5;
    
    # This template applies to updates to [0]
    @relation = line[0].to_s;
    # This template is parametrized by the parameters listed in the comma delimited list [1]
    @paramlist = line[1].split(";").collect do |k| k.gsub(/ *([^ ]) */, "\\1") end;
    # The target's Map Entry (template) is [2]
    @target = TemplateEntry.parse(line[2]);
    # The conditions for the target to apply are [3]
    @conditions = TemplateConditionList.new(line[3]);
    # The expression for the update is [4]
    @expression = TemplateExpression.parse(line[4]);
    
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
    @loopvarlist = @expression.loop_vars(@paramlist);
    
    @index = index;
  end
  
  def requires_loop?
    !@loopvarlist.empty?
  end
  
  def entries
    @conditions.entries.concat(@expression.entries);
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
  
  def UpdateTemplate.get_map(map_name, params)
    @@map_names.assert_key(map_name) { {"id" => @@map_id += 1, "params"=> params}; }
    @@map_names[map_name]["id"];
  end
  
  def UpdateTemplate.map_names
    @@map_names;
  end
end

###################################################

class TemplateForeachEvaluator
  attr_reader :valuation;

  # Note that valuation is treated as if it were 
  def initialize(valuation)
    @domains, @valuation = Array.new, valuation;
    
    # @domains contains a list of arrays in the same order as valuation.template.loopvarlist
    # Every array in @domains describes the domains of one or more loop variables in the
    # foreach loop.  As we gather more and more entries, we build up the domains of the
    # corresponding values.  
    
    valuation.template.loopvarlist.each do |loopentry|
      @domains.push(Array.new);
    end
  end
  
  def discover(entry, value)
    @valuation.template.loopvarlist.each_pair(@domains) do |rule, row|
      # rule[1] contains a list of elements of the form [KEYINDEX, LOOPVAR]
      # for each element extract the KEYINDEXth value of entry.key and save it
      # for the domain of LOOPVAR... as long as the rule is applicable (the
      # TemplateEntry at rule[0] weakly matches entry)
      
      # Also, note that it is possible for the same map to appear twice with 
      # two different sets of loop variables; One entry might potentially match 
      # several rules.
      
      # It is also possible for an entry to match no rules at all; This occurs if
      # the template's expression includes both nonlooping and looping maps.
      row.push(rule[1].collect do |k| entry.key[k[0]] end) if rule[0].weak_match?(entry, @valuation.params);
#      Logger.debug { entry.to_s + " = " + value.to_s + "; Matches: " + rule[0].to_s + " : " + rule[0].weak_match?(entry, @valuation.params).to_s };
    end
    @valuation.entries[entry] = value;
  end
  
  # Foreach is effectively a nested loop where the nesting depth is equal to the 
  # number of maps being scanned (since we're looping over entries, we get to
  # loop over each loop parameter for free).  The passed block is called once for
  # each tuple in the cross-product of the domains of all loop variables (and 
  # comes with the corresponding template valuation and target).  
  def foreach(&callback)
    foreach_impl(callback);
  end
  
  private
  
  def foreach_impl(callback, depth = 0, valuation = @valuation.clone)
    if depth >= @domains.size then
      val = valuation.to_f;
      callback.call(valuation.template.target.instantiate(valuation.params), val) unless val == 0;
    else
      @domains[depth].each do |row|
        valuation.template.loopvarlist[depth][1].each_pair(row) do |k, v|
          valuation.params[k[1].to_s] = v.to_i;
        end
        foreach_impl(callback, depth+1, valuation);
      end
    end
  end
end


