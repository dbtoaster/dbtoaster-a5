require 'ok_mixins'
require 'graph_draw'

def make_node_filter(cnt, template = "^%")
  (1..cnt).collect do |i|
    [ "Node " + if i < 10 then "0" else "" end + i.to_s,
      Regexp.compile(template.gsub("%", "Node"  + if i < 10 then "0" else "" end + i.to_s))
    ]
  end
end

def make_node_list(cnt)
  make_node_filter(cnt).collect { |f| f[0] }
end

def run_graphs(node_cnt)
  a = Graph.new("measurements/tpch_q12_"+node_cnt.to_s+"nodes.run.200persec");
  a.make_counter(/; ([0-9]+) updates total/);
  b = a.clone.filter(make_node_filter(node_cnt, "^[0-9]+ %"));
  b.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (Bytes)"];
  b.extract("Updates", "CPU %");
  b.subgraphs.push(a.filter(/^[^?].*Query Engine:/).gsub(/^([0-9]+).*; last result: ([0-9]+.)/).title("Completion").set_axis(:y2));
  
  b.set_range(:y2, (0..610000));
  b.set_range(:y, (0..100));
  b.directive("set key left top");
  b.directive("set y2label 'Completion'");
  b.average(2, make_node_list(node_cnt), "Node Avg CPU")
  b.style = "lines";
  b.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.completion.pdf");
  
  #############################
  
  a = Graph.new("measurements/tpch_q12_"+node_cnt.to_s+"nodes.run.200persec");
  a.make_counter(/; ([0-9]+) updates total/);
  a.filter(make_node_filter(node_cnt, "^[0-9]+ %") << [ "Switch" ,  /^[0-9]+ switch/]);
  a.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (Bytes)"];
  
  b = a.clone
    b.extract("Line #", "Memory (Bytes)")
    b.label(:x, "Timestep")
    b.directive("set key left top")
    b["Switch"].style = "lines";
    b.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.mem.pdf");
  
  a.extract("Line #", "CPU %")
    a.label(:x, "Timestep")
    a.directive("set key right bottom")
    a.average(2, make_node_list(node_cnt), "Node Avg")
    a.style = "lines";
    a.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.cpu.pdf");

end
  
#############################

a = Graph.new(
  { 
    [8, 100]  => "measurements/tpch_q12_8nodes.run.100persec",
    [8, 200]  => "measurements/tpch_q12_8nodes.run.200persec",
    [15, 100] => "measurements/tpch_q12_15nodes.run.100persec",
    [15, 200] => "measurements/tpch_q12_15nodes.run.200persec",
    [20, 200] => "measurements/tpch_q12_20nodes.run.200persec"
  }.collect do |num, file|
    graph = Graph.new(file);
    graph.make_counter(/; ([0-9]+) updates total/);
    graph.filter(/^[^?].*Query Engine:/);
    graph.gsub(/^([0-9]+) Query Engine: ([0-9]+) queries; ([0-9.]+) avg time per query; ([0-9.]+) windowed time per query; last result: ([0-9]+.)/, "\\1 \\2 \\3 \\4 \\5")
    graph.keys = [ "Updates", "Queries", "Avg Time / Query (s)", "Query Latency (s)", "Result" ];
    graph.extract("Updates", "Query Latency (s)").title(num[1].to_s + " Updates/sec, "+num[0].to_s+" Nodes")
    num << graph;
  end.sort do |a, b|
    if a[0] == b[0] then a[1] <=> b[1] else a[0] <=> b[0] end
  end.collect do |a|
    a[2]
  end
);
a.set_range(:y, (0..10));
a.keys = [ "Updates", "Time / Query (s)" ];
a.directive("set key left top");
a.graph("measurements/tpch_q12.query_rate.pdf");

#############################

run_graphs(8);
run_graphs(15);
