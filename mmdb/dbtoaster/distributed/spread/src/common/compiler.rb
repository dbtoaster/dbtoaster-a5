
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
    case type
      when :plus  then left.ready && right.ready;
      when :mult  then left.ready && right.ready;
      when :sub   then left.ready && right.ready;
      when :div   then left.ready && right.ready;
      when :num   then true;
      when :map   then left.ready;
      when :param then left === nil;
      else             false;
    end
  end
  
  def to_f
    case type
      when :plus  then left.to_f + right.to_f;
      when :mult  then left.to_f * right.to_f;
      when :sub   then left.to_f - right.to_f;
      when :div   then left.to_f / right.to_f;
      when :num   then left;
      when :map   then if ready left.value; 
                       else raise SpreadException.new("Trying to read incomplete Equation");
                       end
      when :param then raise SpreadException.new("Trying to read uninstantiated Equation");
    end
  end
  
  def instantiate(params)
    case type
      when :plus  then MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :mult  then MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :sub   then MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :div   then MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :num   then MapEquation.new(type, left);
      when :map   then MapEquation.new(type, left);
      when :param then if params[right] == nil then MapEquation.new(:num, params[right])
                       else raise SpreadException.new("Instantiating equation missing parameter: '" + right.to_s);
                       end
    end
  end
  
  
end