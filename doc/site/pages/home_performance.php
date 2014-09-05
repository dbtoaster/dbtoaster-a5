<script src="http://d3js.org/d3.v3.min.js"></script>

<div id="staticimg">
   <img src="perf.png" style="width:100%"/>
</div>

<div class="magic_chkbox bardbx">
   <input type="checkbox" class="filter_data" id="cb_dbx" name="" checked /> 
   <label class="checkbox-inline" id="lbl_dbx" for="cb_dbx">&nbsp;DBX</label>
</div>
<div class="magic_chkbox barspy">
   <input type="checkbox" class="filter_data" id="cb_spy" name="" checked /> 
   <label class="checkbox-inline" id="lbl_spy" for="cb_spy">&nbsp;SPY</label>
</div>
<div class="magic_chkbox barrep">
   <input type="checkbox" class="filter_data" id="cb_rep" name="" /> 
   <label class="checkbox-inline" id="lbl_rep" for="cb_rep">&nbsp;REP</label>
</div>
<div class="magic_chkbox barivm">
   <input type="checkbox" class="filter_data" id="cb_ivm" name="" /> 
   <label class="checkbox-inline" id="lbl_ivm" for="cb_ivm">&nbsp;IVM</label>
</div>
<div class="magic_chkbox barprscala">
   <input type="checkbox" class="filter_data" id="cb_prscala" name="" /> 
   <label class="checkbox-inline" id="lbl_prscala" for="cb_prscala">&nbsp;R1&nbsp;(Scala)</label>
</div>
<div class="magic_chkbox barprcpp">
   <input type="checkbox" class="filter_data" id="cb_prcpp" name="" /> 
   <label class="checkbox-inline" id="lbl_prcpp" for="cb_prcpp">&nbsp;R1&nbsp;(C++)</label>
</div>
<div class="magic_chkbox barscala">
   <input type="checkbox" class="filter_data" id="cb_scala" name="" /> 
   <label class="checkbox-inline" id="lbl_scala" for="cb_scala">&nbsp;R2&nbsp;(Scala)</label>
</div>
<div class="magic_chkbox barcpp">
   <input type="checkbox" class="filter_data" id="cb_cpp" name="" checked /> 
   <label class="checkbox-inline" id="lbl_cpp" for="cb_cpp">&nbsp;R2&nbsp;(C++)</label>
</div>

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
   var groups = ["dbx", "spy", "rep", "ivm", "prscala", "prcpp", "scala", "cpp"];

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
   $("#staticimg").hide();
});

</script>

<p><i>If you cannot see the above graphs, please <a href="perf.png">click here</a>. The underlying performance measurement data can be downloaded from <a href="data/bakeoff.csv">here</a>.</i></p>

<p>The above graphs show a performance comparison of DBToaster-generated query engines against a commercial database system (DBX), a commercial stream processor (SPY), and several naive query evaluation strategies implemented in DBToaster that do not involve the Higher-Order incremental view maintenance. (The commercial systems remain anonymous in accordance with the terms of their licensing agreements).</p>

<p>In addition, the performance comparison covers generated C++ and Scala programs from both Release 1 (R1) and Release 2 (the current release - R2) releases, in order to compare the performance improvements gained in the latest release.</p>

<p>Performance is measured in terms of the rate at which each system can produce up-to-date (fresh) views of the query results.  As you can see, DBToaster regularly outperforms the commercial systems by <b>3-6 orders of magnitude.</b></p>

<p>The graphs show the performance of each system on a set of realtime data-warehousing queries based on the TPC-H query benchmark.  These queries are all included as examples in the DBToaster distribution.</p>

<p>REP (Depth-0) represents a naive query evaluation strategy where each refresh requires a full re-evaluation of the entire query.  IVM (Depth-1) is a well known technique called Incremental View Maintenance (we discuss the distinctions in depth in our technical report).  Both of these (REP and IVM) were implemented by a DBToaster-generated engine.</p>

<p>The experiments are performed on a single-socket Intel Xeon E5- 2620 (6 physical cores), 128GB of RAM, RHEL 6, and Open-JDK 1.6.0 28. Hyper-threading, turbo-boost, and frequency scaling are disabled to achieve more stable results.</p>

<p>We used LMS 0.3, running with Scala 2.10 on the Hotspot 64-bit server VM having 14 GB of heap memory, and the generated code was compiled with the -optimise option of the Scala compiler.</p>

<p>A stream synthesized from a scaling factor 0.1 database (100MB) is used for performing the experiments (with an upper limit of 2 hours), while our scaling experiments extend these results up to a scaling factor of 10 (10 GB).</p>
