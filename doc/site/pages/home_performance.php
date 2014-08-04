<script src="http://d3js.org/d3.v3.min.js"></script>

<label class="checkbox-inline" id="lbl_rep">
   <input class="filter_data" id="cb_rep" type="checkbox"> REP
</label>
<label class="checkbox-inline" id="lbl_dbx">
   <input class="filter_data" id="cb_dbx" type="checkbox" checked> DBX
</label>
<label class="checkbox-inline" id="lbl_spy">
   <input class="filter_data" id="cb_spy" type="checkbox" checked> SPY
</label>
<label class="checkbox-inline" id="lbl_ivm">
   <input class="filter_data" id="cb_ivm" type="checkbox"> IVM
</label>
<label class="checkbox-inline" id="lbl_prcpp">
   <input class="filter_data" id="cb_prcpp" type="checkbox"> C++ (R1)
</label>
<label class="checkbox-inline" id="lbl_prscala">
   <input class="filter_data" id="cb_prscala" type="checkbox"> Scala (R1)
</label>
<label class="checkbox-inline" id="lbl_scala">
   <input class="filter_data" id="cb_scala" type="checkbox"> Scala (R2)
</label>
<label class="checkbox-inline" id="lbl_cpp">
   <input class="filter_data" id="cb_cpp" type="checkbox" checked> C++ (R2)
</label>

<div class="bakeoff">
</div>

<script>
var tooltip = d3.select("body")
   .append("div")
   .attr("class", "datatooltip")
   .style("position", "absolute")
   .style("z-index", "10")
   .style("visibility", "hidden");

var margin = {top: 20, right: 20, bottom: 70, left: 60},
    width = 700 - margin.left - margin.right,
    height = 300 - margin.top - margin.bottom;

var bakeoff = d3.select(".bakeoff")

var x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
var y = d3.scale.log().range([height, 0]);

var svg = bakeoff.append("svg")
      .attr("height", height + margin.top + margin.bottom)
      .attr("width", width + margin.left + margin.right)
   .append("g")
      .attr("transform",
         "translate(" + margin.left + "," + margin.top + ")");

function drawBars(rawData, rawGroups) {
   var groups = rawGroups.filter(function (g) {
      return d3.select("#cb_" + g).node().checked;
   });
   var data = rawData.filter(function (d) {
      return groups.indexOf(d.group) >= 0;
   });

   var barwidth = x.rangeBand() / (groups.length + 1);
   var bars = svg.selectAll("rect").data(data);

   bars.enter().append("rect")
      .attr("class", function(d) { return "bar" + d.group; })
      .attr("x", function(d) { return x(d.query) + (0.5 + groups.indexOf(d.group)) * barwidth; })
      .attr("width", barwidth)
      .on("mouseover", function(d) { return tooltip.text(d.v + " Tuples/s").style("visibility", "visible"); })
      .on("mousemove", function(d) { return tooltip.style("top", (event.pageY - 10) + "px").style("left",(event.pageX + 10) + "px"); })
      .on("mouseout", function(d) { return tooltip.style("visibility", "hidden");})
      .attr("y", height)
      .attr("height", 0)
      .transition()
      .attr("y", function(d) { return y(d.v); })
      .attr("height", function(d) { return height - y(d.v); });

   bars.transition()
      .attr("class", function(d) { return "bar" + d.group; })
      .attr("x", function(d) { return x(d.query) + (0.5 + groups.indexOf(d.group)) * barwidth; })
      .attr("width", barwidth)
      .attr("y", height)
      .attr("height", 0)
      .attr("y", function(d) { return y(d.v); })
      .attr("height", function(d) { return height - y(d.v); });

   bars.exit().transition().attr("y", height).attr("height", 0).remove();
}

d3.csv("data/bakeoff.csv", function(error, rawInput) {
   var data = [];
   var groups = ["rep", "dbx", "spy", "ivm", "prcpp", "prscala", "scala", "cpp"];

   rawInput.forEach(function (d) {
      groups.forEach(function (g) {
         var newData = { query: d.query, group: g, v: d[g] };
         data.push(newData);
      });
   });

   x.domain(data.map(function(d) { return d.query; }));
   y.domain([1, d3.max(data, function(d) { return Math.max(d.v); })]).nice();

   var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");

   var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left")
      .ticks(10);

   drawBars(data, groups);

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
      .attr("dy", "-3.5em")
      .style("text-anchor", "end")
      .text("Average Refresh Rate (1/s)");

   d3.selectAll(".filter_data").on("change", function() {
      drawBars(data, groups);
   });
});

</script>


<p>The above graphs show a performance comparison of DBToaster-generated query engines against a commercial database system (DBX), a commercial stream processor (SPY), and several naive query evaluation strategies implemented in DBToaster that do not involve the Higher-Order incremental view maintenance. (The commercial systems remain anonymous in accordance with the terms of their licensing agreements).</p>

<p>In addition, the performance comparison covers generated C++ and Scala programs from both Release 1 (R1) and Release 2 (the current release - R2) releases, in order to compare the performance improvements gained in the latest release.</p>

<p>Performance is measured in terms of the rate at which each system can produce up-to-date (fresh) views of the query results.  As you can see, DBToaster regularly outperforms the commercial systems by <b>3-6 orders of magnitude.</b></p>

<p>The graphs show the performance of each system on a set of realtime data-warehousing queries based on the TPC-H query benchmark.  These queries are all included as examples in the DBToaster distribution.</p>

<p>REP (Depth-0) represents a naive query evaluation strategy where each refresh requires a full re-evaluation of the entire query.  IVM (Depth-1) is a well known technique called Incremental View Maintenance (we discuss the distinctions in depth in our technical report).  Each of these was implemented by a DBToaster-generated engine.</p>
