<script src="http://d3js.org/d3.v3.min.js"></script>

<div class="bakeoff"></div>
<script>

var margin = {top: 20, right: 20, bottom: 70, left: 40},
    width = 600 - margin.left - margin.right,
    height = 300 - margin.top - margin.bottom;

var x = d3.scale.ordinal().rangeRoundBands([0, width], .05);

var y = d3.scale.linear().range([height, 0]);

var xAxis = d3.svg.axis()
   .scale(x)
   .orient("bottom");

var yAxis = d3.svg.axis()
   .scale(y)
   .orient("left")
   .ticks(10);

var svg = d3.select(".bakeoff").append("svg")
      .attr("height", height + margin.top + margin.bottom)
   .append("g")
      .attr("transform",
         "translate(" + margin.left + "," + margin.top + ")");

d3.csv("data/bakeoff.csv", function(error, data) {
   x.domain(data.map(function(d) { return d.query; }));
   y.domain([0, d3.max(data, function(d) { return Math.max(d.cpp, d.scala, d.scalalms); })]);

   svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
   .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", "-.55em")
      .attr("transform", "rotate(-90)" );

   svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
   .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Execution time (s)");

   var groups = ["scala", "scalalms", "cpp"];
   var barwidth = x.rangeBand() / (groups.length + 1)

   svg.selectAll("bar")
      .data(data)
   .enter().append("rect")
      .attr("class", "barscala")
      .attr("x", function(d) { return x(d.query) + 0.5 * barwidth; })
      .attr("width", barwidth)
      .attr("y", function(d) { return y(d.scala); })
      .attr("height", function(d) { return height - y(d.scala); });

   svg.selectAll("bar")
      .data(data)
   .enter().append("rect")
      .attr("class", "barscalalms")
      .attr("x", function(d) { return x(d.query) + 1.5 * barwidth; })
      .attr("width", barwidth)
      .attr("y", function(d) { return y(d.scalalms); })
      .attr("height", function(d) { return height - y(d.scalalms); });

   svg.selectAll("bar")
      .data(data)
   .enter().append("rect")
      .attr("class", "barcpp")
      .attr("x", function(d) { return x(d.query) + 2.5 * barwidth; })
      .attr("width", barwidth)
      .attr("y", function(d) { return y(d.cpp); })
      .attr("height", function(d) { return height - y(d.cpp); });
});

</script>


<p>The above graphs show a performance comparison of DBToaster-generated query engines against a commercial database system (DBX), a commercial stream processor (SPY), and several naive query evaluation strategies implemented in DBToaster that do not involve the Higher-Order incremental view maintenance. (The commercial systems remain anonymous in accordance with the terms of their licensing agreements).</p>

<p>Performance is measured in terms of the rate at which each system can produce up-to-date (fresh) views of the query results.  As you can see, DBToaster regularly outperforms the commercial systems by <b>3-4 orders of magnitude.</b></p>

<p>The graphs show the performance of each system on a set of realtime data-warehousing queries based on the TPC-H query benchmark (Q1-Q22,Q11a,Q17a,Q18a,Q22a,SSB4), six algorithmic trading queries (BSV, BSP, AXF, PSP, MST, VWAP), and two scientific queries (MDDB1 and MDDB2).  These queries are all included as examples in the DBToaster distribution.</p>

<p>Depth-0 represents a naive query evaluation strategy where each refresh requires a full re-evaluation of the entire query.  Depth-1 is a well known technique called Incremental View Maintenance.  Naive recursive is a weaker form of the Higher-Order IVM (we discuss the distinctions in depth in our technical report).  Each of these was implemented by a DBToaster-generated engine.</p>
