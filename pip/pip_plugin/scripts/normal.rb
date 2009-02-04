#!/usr/bin/ruby

class Integrator
  def initialize(low, high)
    @low = low;
    @high = high;
    @iteration = 0;
    @points = [];
    @estimate;
  end
  
  def evaluate(point)
    return 0.0;
  end
  
  def nextStep
    @iteration += 1;
    if(@iteration == 1) then
      @estimate = (@high - @low) * evaluate(0.5 * (@high - @low));
    else
      stepCount = (@iteration-1) ** 3;
      stepSize = (@high - @low)/(3.0 * stepCount.to_f);
      x = @low + 0.5 * stepSize;
      sum = 0.0;
      (0 ... stepCount).each do |j| 
        sum += evaluate(x).to_f;
        x += stepSize + stepSize;
        sum += evaluate(x).to_f;
        x += stepSize;
      end
      @estimate = (@estimate + (((@high - @low) * sum) / stepCount.to_f)) / 3.0
    end
    return @estimate;
  end
  
  def integrate(epsilon)
    lastEstimate = 0;
    while(@iteration < 5) do
      nextStep();
    end
    while(@iteration < 20) do
      lastEstimate = @estimate;
      nextStep();
      if((lastEstimate - @estimate).abs < (lastEstimate * epsilon).abs) then
        return @estimate;
      end
    end
  end
end

class NormalIntegrator < Integrator

  def evaluate(point)
    return Math.exp( (0 - ((point.to_f ** 2.0) / 2.0 )) ) / 2.506628274631;
  end

end

precision = 0.001;
point = 0;
total = 0.0;
step = 0;
print "double pip_normal_cdf_precision = " + precision.to_s + ";\n";
print "double pip_normal_cdf_precomp[] = {\n"
print "  0.0\n";
while(total < 0.5 - precision) do
  step += 1;
  point = precision * step.to_f;
  total = (NormalIntegrator.new(-point, point)).integrate(precision) / 2;
  print "  ," + total.to_s + " // " + point.to_s + "\n";
end
print "};"
print "int pip_normal_cdf_precomp_size = " + (step + 1).to_s + ";\n";
