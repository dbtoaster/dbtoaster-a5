
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
end

###################################################

class MapReference
  attr_reader :entry, :value
  
  def initialize(entry)
    @entry = entry;
    @value = nil;
  end
  
  def ready
    value === nil;
  end
  
  def <=(value)
    value = value;
  end
end

###################################################

class MapEquation
  def initialize(type, left = nil, right = nil)
    @type, @left, @right = type, left, right;
  end
  
  def ready
    case @type
      when :plus, :mult, :sub, :div then 
        @left.ready && @right.ready;
      when :num   then true;
      when :map   then @right != nil;
      when :param then false;
      else             false;
    end
  end
  
  def to_f
    case @type
      when :plus  then @left.to_f + @right.to_f;
      when :mult  then @left.to_f * @right.to_f;
      when :sub   then @left.to_f - @right.to_f;
      when :div   then @left.to_f / @right.to_f;
      when :num   then @left;
      when :map   then if @right != nil then @right; 
                       else raise SpreadException.new("Trying to read incomplete Equation"); 
                       end
      when :param then raise SpreadException.new("Trying to read uninstantiated Equation");
    end
  end
  
  def to_s
    case @type
      when :plus  then "(" + @left.to_s + ")+(" + @right.to_s + ")";
      when :mult  then "(" + @left.to_s + ")*(" + @right.to_s + ")";
      when :sub   then "(" + @left.to_s + ")-(" + @right.to_s + ")";
      when :div   then "(" + @left.to_s + ")/(" + @right.to_s + ")";
      when :num   then @left.to_s;
      when :param then "#" + @left.to_s;
      when :map   then "(M"+@left.source.to_s+"["+@left.key.to_s+"("+@left.version.to_s+")]="+@right.to_s+")";
    end
  end
  
  def parametrize(params)
    case @type
      when :plus, :mult, :sub, :div then
        MapEquation.new(@type, @left.parametrize(params), @right.parametrize(params));
      when :num, :map then
        MapEquation.new(@type, @left);
      when :param then 
          if params[@left.to_i] != nil then MapEquation.new(params[@left.to_i].type, params[@left.to_i].value)
          else raise SpreadException.new("Instantiating equation missing parameter: '" + @left.to_s);
          end
    end
  end
  
  def entries(list = Array.new)
    case @type
      when :num, :param then nil;
      when :map         then list.push(self);
      else                   @left.entries(list); @right.entries(list);
    end  
    list
  end
  
  def discover(entry, value)
    if @type == :map then
      if @left.source == entry.source && @left.key == entry.key then
        @right = value; return true;
      end
    end
    false;
  end 
  
  def maptarget
    if @type == :map then @left else nil; end
  end
  
  def MapEquation.decodeVar(parser)
    case parser.next
      when "(" then decode(parser);
      when "+","-","*","/",")" then 
        raise SpreadException.new("Parse Error ("+parser.last+")");
      else
          if parser.last[0] == "#"[0] then 
            if parser.last == "#=" then
              MapEquation.new(:param, -1);
            else
              MapEquation.new(:param, parser.last.slice(1..-1).to_i);
            end
          else
            MapEquation.new(:num, parser.last.to_f);
          end
    end
  end
  
  def MapEquation.decode(parser, left = decodeVar(parser))
    op = case parser.next
      when "+" then :plus
      when "-" then :sub
      when "*" then :mult
      when "/" then :div
      when ")" then return left;
      else raise SpreadException.new("Parse Error ("+parser.last+")");
    end
    right = decodeVar(parser);
    if parser.more? then decode(parser, MapEquation.new(op, left, right))
    else                 MapEquation.new(op, left, right);
    end
  end
  
  def MapEquation.parse(cmd)
    decode(Tokenizer.new(cmd, /\(|\)|\+|-|\/|\*|#[0-9]+|#=|[0-9.]+/));
  end
end
