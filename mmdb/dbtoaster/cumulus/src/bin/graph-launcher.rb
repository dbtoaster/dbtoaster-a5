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

#############################

a = Graph.new(
  [ "Baseline Performance", "Foreign Key Optimized" ].collect_index do |i, name|
    b = Graph.new(
      [
        [ "q12",  "8agg", "KEYDEP", "COL"  ],
        [ "ppss", "7agg", "KEYDEP", "PPsS" ]
      ].collect do |info|
        c = Graph.new("measurements/tpch_"+info[0]+"_40nodes.run.INFTYpersec.5000m."+info[1+i]+".txt");
        c.filter(/; [0-9.]+ updates per sec/);
        c.gsub(/([0-9.]+) seconds; ([0-9.]+) updates per sec; ([0-9.]+) updates total/, "\\3 \\2 \\1");
        c.keys = [ "Updates", "Update Rate (updates/s)", "Time for 1000 updates" ];
        c.extract("Updates", "Update Rate (updates/s)");
        c.name = info[3];
        c;
      end
    )
    b.keys = [ "Updates", "Update Rate (updates/s)" ];
    b.pivot("Update Rate (updates/s)", :avg);
    b.name = name;
    b;
  end
)

a.name = "Update Rate"
a.set_range(:y, (0...800));
a.keys = [ "Query", "Maximum Update Rate (updates/s)" ];
a.graph_bar("foreignkey.pdf");

exit
#############################


a = Graph.new(
  [
    [20, "200persec"                 , "20 Nodes (200 Updates/s)"], 
    [40, "INFTYpersec"               , "40 Nodes (Uncapped)"], 
    [40, "INFTYpersec.INORDER"       , "40 Nodes (Uncapped, In-Order)"]
#    [40, "INFTYpersec.INORDER.KEYDEP", "40 Nodes (Uncapped, In-Order+FK Opt)"]
  ].collect do |info|
    b = Graph.new("measurements/tpch_q12_"+info[0].to_s+"nodes.run." + info[1].to_s + ".txt");
    b.make_counter(/; ([0-9]+) updates total/);
    b.filter(make_node_filter(info[0], "^[0-9]+ %"));
    b.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (KB)"];
    b.extract("Updates", "CPU %");
    b.average("CPU %");
    b.name = info[2];
    b;
  end
);
max = Math.max(*a.subgraphs.collect { |sg| sg.compute_max(0) });
a.style = "lines"
a.limit(0, (0...max))
a.set_range(:y, (0..80));
a.directive("set key top right");
a.keys = [ "Updates", "Node Avg CPU %" ];
a.graph("100mbUpdatesVsCPUCompletion.pdf");



exit;
a = Graph.new(
  [ [20, "200persec"                 , "20 Nodes (200 Updates/s)"], 
    [40, "INFTYpersec"               , "40 Nodes (Uncapped)"], 
    [40, "INFTYpersec.INORDER"       , "40 Nodes (Uncapped, In-Order)"] 
#    [40, "INFTYpersec.INORDER.KEYDEP", "40 Nodes (Uncapped, In-Order+FK Opt)"]
  ].collect do |info|
    
    b = Graph.new("measurements/tpch_q12_"+info[0].to_s+"nodes.run." + info[1].to_s + ".txt");
    b.make_counter(/^switch/);
    b.filter(make_node_filter(info[0], "^[0-9]+ %"));# << [ "Switch" ,  /^[0-9]+ switch/]);
    b.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (KB)"];
    
    b.extract("Line #", "Memory (KB)")
    b.adjust("Line #") { |val| val.to_i * 5 }
    b.label(:x, "Time (s)")
    b.adjust("Memory (KB)") { |val| val.to_f / 1024.0 }
    b.label(:y, "Memory (MB)")
    b.limit("Time (s)", (0..4000));
    b.average("Memory (MB)");
    b.name = info[2].to_s;
    b;
  end
);
b = Graph.new("measurements/tpch_q12_40nodes.run.INFTYpersec.INORDER.txt");
  b.make_counter(/^switch/);
  b.filter(/^[0-9]+ switch/);
  b.name = "Switch"
  b.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (KB)"];
  b.extract("Line #", "Memory (KB)")
  b.adjust("Line #") { |val| val.to_i * 5 }
  b.label(:x, "Time (s)")
  b.adjust("Memory (KB)") { |val| val.to_f / 1024.0 }
  b.label(:y, "Memory (MB)")
  b.limit("Time (s)", (0..4000));

a.subgraphs.push(b);
a.directive("set key left top")
a.style = "lines";
a.graph("100mbUpdatesVsMemory.pdf");

#############################

a = Graph.new(
  [
    [ "q12", (2..8), "COL" ],
    [ "PPSS", (2..7), "PPsS" ],
  ].collect do |info|
    b = Graph.new(
      info[1].collect do |i|
        c = Graph.new("measurements/tpch_"+info[0]+"_40nodes.run.INFTYpersec.5000m."+i.to_s+"agg.txt")
        c.filter(/; [0-9.]+ updates per sec/)
        c.gsub(/([0-9.]+) seconds; ([0-9.]+) updates per sec; ([0-9.]+) updates total/, "\\3 \\2 \\1")
        c.keys = [ "Updates", "Update Rate (updates/s)", "Time for 1000 updates" ];
        c.extract("Updates", "Update Rate (updates/s)");
        c.name = i.to_s;
        c.limit("Updates", (20000..160000))
        c;
      end
    );
    b.keys = [ "Updates", "Update Rate (updates/s)" ];
    b.pivot("Update Rate (updates/s)", :avg);
    b.name = info[2];
    b;
  end
)
a.directive("set key bottom right");
a.set_range(:y, (0...500));
a.graph("measurements/aggvsupdates.pdf")
#############################

a = Graph.new(
  [
    [ "q12", (2..8), "COL" ],
    [ "PPSS", (3..7), "PPsS" ],
  ].collect do |info|
    b = Graph.new(
      info[1].collect do |i|
        c = Graph.new("measurements/tpch_"+info[0]+"_40nodes.run.INFTYpersec.5000m."+i.to_s+"agg.txt")
        c.make_counter(/; ([0-9]+) updates total/);
        c.filter(/; [0-9.]+ windowed time per query/)
        c.gsub(/^([0-9]+) Query Engine: ([0-9]+) queries; ([0-9.]+) avg time per query; ([0-9.]+) windowed time per query; last result: ([0-9]+.)/, "\\1 \\2 \\3 \\4 \\5")
        c.keys = [ "Updates", "Queries", "Avg Time / Query (s)", "Query Latency (s)", "Result" ];
        c.extract("Updates", "Query Latency (s)");
        c.name = i.to_s;
        c.limit("Updates", (20000..160000))
        c;
      end
    );
    b.keys = [ "Updates", "Query Latency (s)" ];
    b.pivot("Query Latency (s)", :avg);
    b.name = info[2];
    b;
  end
)
a.directive("set key bottom right");
a.set_range(:y, (0...5));
a.graph("measurements/aggvslatency.pdf")
    
#############################

a = Graph.new(
  [ [8, 100],
    [8, 200],
    [15, 100],
    [15, 200],
    [20, 200],
    [40, "INFTY"]
  ].collect_hash do |key|
    [ key, "measurements/tpch_q12_" + key[0].to_s + "nodes.run." + key[1].to_s + "persec.txt" ];
  end.collect do |num, file|
    graph = Graph.new(file);
    graph.make_counter(/; ([0-9]+) updates total/);
    graph.filter(/^[^?].*Query Engine:/);
    graph.gsub(/^([0-9]+) Query Engine: ([0-9]+) queries; ([0-9.]+) avg time per query; ([0-9.]+) windowed time per query; last result: ([0-9]+.)/, "\\1 \\2 \\3 \\4 \\5")
    graph.keys = [ "Updates", "Queries", "Avg Time / Query (s)", "Query Latency (s)", "Result" ];
    name = 
      if num[1] == "INFTY" then 
        ((graph.compute_average(1)/100).to_i * 100).to_s 
      else num[1].to_s end;
    graph.extract("Updates", "Query Latency (s)").title(name + " Updates/sec, "+num[0].to_s+" Nodes")
    num << graph;
  end.sort do |a, b|
    if a[0] == b[0] then a[1] <=> b[1] else a[0] <=> b[0] end
  end.collect do |a|
    a[2]
  end
);
cap = (0...a["200 Updates/sec, 20 Nodes"].compute_max(0))
a.limit(0, cap);
a.set_range(:x, cap);
a.set_range(:y, (0..10));
a.keys = [ "Updates", "Time / Query (s)" ];
a.directive("set key left top");
a.graph("measurements/tpch_q12.query_rate.pdf");

#############################


exit


if false then
  
  #############################
  
  #############################
  
end
  [8, 15, 20, 40].each_pair([200, 200, 200, "INFTY"]) do |node_cnt, rate|
    a = Graph.new("measurements/tpch_q12_"+node_cnt.to_s+"nodes.run." + rate.to_s + "persec.txt");
  #  a.make_counter(/; ([0-9]+) updates total/);
    a.make_counter(/^switch/);
    a.adjust(0) { |val| val.to_i * 5 }
    
    b = a.clone.filter(make_node_filter(node_cnt, "^[0-9]+ %"));
    b.keys = ["Time (s)", "Name", "CPU %", "Mem%", "Memory (KB)"];
    b.extract("Time (s)", "CPU %");
    b.subgraphs.push(a.filter(/^[^?].*Query Engine:/).gsub(/^([0-9]+).*; last result: ([0-9]+.)/).title("Completion").set_axis(:y2));
    
    b.set_range(:y2, (0..100));
    max = b["Completion"].compute_max(1).to_f
    b["Completion"].adjust(1) { |val| (val.to_f / max)*100.0 }
    b.set_range(:y, (0..100));
    b.directive("set key left top");
    b.directive("set y2label 'Completion (%)'");
    b.average(2, make_node_list(node_cnt), "Node Avg CPU")
    b.style = "lines";
    b.set_range(:x, (0..4000));
    b.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.completion.pdf");
    
    #############################
    
    a = Graph.new("measurements/tpch_q12_"+node_cnt.to_s+"nodes.run." + rate.to_s + "persec.txt");
    a.make_counter(/; ([0-9]+) updates total/);
    a.filter(make_node_filter(node_cnt, "^[0-9]+ %") << [ "Switch" ,  /^[0-9]+ switch/]);
    a.keys = ["Updates", "Name", "CPU %", "Mem%", "Memory (KB)"];
    
    b = a.clone
      b.extract("Line #", "Memory (KB)")
      b.adjust("Line #") { |val| val.to_i * 5 }
      b.label(:x, "Time (s)")
      b.adjust("Memory (KB)") { |val| val.to_f / 1024.0 }
      b.label(:y, "Memory (MB)")
      b.limit("Time (s)", (0..4000));
      b.directive("set key left top")
      b["Switch"].style = "lines";
      b.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.mem.pdf");
    
    a.extract("Line #", "CPU %")
      a.adjust("Line #") { |val| val.to_i * 5 }
      a.label(:x, "Time (s)")
      a.directive("set key right bottom")
      a.average(2, make_node_list(node_cnt), "Node Avg")
      a.style = "lines";
      a.graph("measurements/tpch_q12_"+node_cnt.to_s+"nodes.cpu.pdf");
  
  end
  
  #############################
