require 'ok_mixins'
require 'graph_draw'

a = Graph.new("measurements/tpch_q12_8nodes.run");
a.filter(
  [ 
    [ "Node 01",  /^Node01/], 
    [ "Node 02",  /^Node02/], 
    [ "Node 03",  /^Node03/], 
    [ "Node 04",  /^Node04/], 
    [ "Node 05",  /^Node05/], 
    [ "Node 06",  /^Node06/], 
    [ "Node 07",  /^Node07/], 
    [ "Node 08",  /^Node08/], 
    [ "Switch" ,  /^switch/]
  ] 
)
#a.filter(/^Node01/)
a.keys = ["Name", "CPU %", "Mem%", "Memory (Bytes)"];
a.clone.extract("Line #", "Memory (Bytes)").label(:x, "Timestep").graph("measurements/tpch_q12_8nodes.mem.pdf");
a.clone.extract("Line #", "CPU %").label(:x, "Timestep").graph("measurements/tpch_q12_8nodes.cpu.pdf");