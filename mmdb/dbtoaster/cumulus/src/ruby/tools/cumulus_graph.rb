require 'util/graph_draw.rb'

class CumulusLogSource < LogSource
  attr_reader :processes;
  
  def initialize(source)
    super(source, /Switch: [0-9]+ updates processed/);
    @processes = Array.new;

    # node state
    add_event(/([^ ]+) +[^ ]+ +[0-9]+ ([0-9.]+) +([0-9.]+) +([0-9]+) +([0-9.]+) +[^ ]+ +[^ ]+ +[^ ]+ +[^ ]+ +([a-zA-Z.0-9]+)/) do |variables, event|
      process = "#{event[5].split(/\./)[0]}.#{event[0]}";
      @processes.push(process) unless @processes.include? process;
      variables[process+"-cpu"] = event[1];
      variables[process+"-%mem"] = event[2];
      variables[process+"-vmem"] = event[3];
      variables[process+"-rmem"] = event[4];
    end
    
    # chef state
    add_event(
      /Switch: ([0-9]+) updates processed, rate: ([0-9.]+), [0-9]+ puts, [0-9]+ fetches/, 
      ["chef-updates", "chef-rate"]
    );
    
    finish_setup;
  end
end


raise "usage: graph.sh datafile" unless ARGV.size >= 1;

$source = CumulusLogSource.new(ARGV[0]);

$graph = Graph.new;
$graph.add_source($source.select(["chef-updates", "chef-rate"]));
$graph.add_view("Update Rate", "chef-updates", "chef-rate");
$graph.draw;
