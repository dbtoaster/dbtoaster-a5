
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
      when :plus  left.ready && right.ready;
      when :mult  left.ready && right.ready;
      when :sub   left.ready && right.ready;
      when :div   left.ready && right.ready;
      when :num   true;
      when :map   left.ready;
      when :param left === nil;
      else        false;
    end
  end
  
  def to_f
    case type
      when :plus  left.to_f + right.to_f;
      when :mult  left.to_f * right.to_f;
      when :sub   left.to_f - right.to_f;
      when :div   left.to_f / right.to_f;
      when :num   left;
      when :map   if ready left.value; 
                  else raise SpreadException.new("Trying to read incomplete Equation");
                  end
      when :param raise SpreadException.new("Trying to read uninstantiated Equation");
    end
  end
  
  def instantiate(params)
    case type
      when :plus  MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :mult  MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :sub   MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :div   MapEquation.new(type, left.instantiate(params), right.instantiate(params));
      when :num   MapEquation.new(type, left);
      when :map   MapEquation.new(type, left);
      when :param if params[right] == nil MapEquation.new(:num, params[right])
                  else raise SpreadException.new("Instantiating equation missing parameter: '" + right.to_s);
                  end
    end
  end
  
  
end