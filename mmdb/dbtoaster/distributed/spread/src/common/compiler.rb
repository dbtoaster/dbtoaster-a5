
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
    if @tokens.size > 0 then @last = @tokens.shift;
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
      @keys.collect do |key|
        case key
          when String then if params.has_key? key then params[key] else -1 end;
          else key;
        end
      end
      );
  end
  
  def to_s(params = Hash.new)
    "Hash " + @source.to_s + "[" + 
      @keys.collect do |key| 
        case key
          when String then if params.has_key? key then params[key].to_i.to_s else key end;
          else key.to_s;
        end
      end.join(",") + "]";
  end
  
  def TemplateEntry.parse(entry)
    decode(Tokenizer.new(entry, /Map|[0-9]+|\[|\]|[a-zA-Z][a-zA-Z0-9_]*/));
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
    if @entries.has_key? entry then
      @entries[entry] = value;
      @target = nil;
    end
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
  
  def TemplateExpression.decode_var(tokenizer)
    case tokenizer.next
      when "(" then decode(tokenizer);
      when "+","-","*","/",")" then 
        raise SpreadException.new("Parse Error: Found "+tokenizer.last+" instead of rval");
      when "Map" then
        TemplateExpression.new(:map, TemplateEntry.decode(tokenizer));
      else
        TemplateExpression.new(
          :val,
          if tokenizer.last.is_number? then tokenizer.last.to_f else tokenizer.last.to_s end
        );
    end
  end
  
  def entries(list = Array.new)
    case @op
      when :plus, :mult, :sub, :div then @left.entries(@right.entries(list));
      when :map                     then list.push(@left);
      when :val                     then list;
      else            raise SpreadException.new("Unknown Expression operator (entries): " + @op.to_s);
    end
  end
  
  def loop_vars(known_vars, list = Array.new)
    entries.delete_if do |entry|
      # see if any keys have not been bound (either explicitly, or via known_vars)
      entry.keys.clone.delete_if do |key| (key.is_a? Numeric) || (known_vars.include? key) end.empty?
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
  
  def TemplateExpression.decode(tokenizer, left = decode_var(tokenizer))
    op = case tokenizer.next
      when "+" then :plus
      when "-" then :sub
      when "*" then :mult
      when "/" then :div
      when ")" then return left;
      else raise SpreadException.new("Parse Error ("+tokenizer.last+")");
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
end

###################################################

class UpdateTemplate
  attr_reader :relation, :paramlist, :loopvarlist, :target, :conditions, :expression;
  
  def initialize(line)
    line = line.split("\t");
    raise SpreadException.new("Instantiating update template with insufficient components") if line.size < 5;
    
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
    
    @loopvarlist = @expression.loop_vars(@paramlist);
  end
  
  def requires_loop?
    @loopvarlist.empty?
  end
  
  def entries
    @conditions.entries.concat(@expression.entries);
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
      @loopvarlist.join(";") + "\t" + 
      @target.to_s + "\t" +
      @conditions.to_s + "\t" +
      @expression.to_s;
  end
end


