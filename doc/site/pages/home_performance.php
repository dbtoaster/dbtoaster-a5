<img src="bakeoff.png" class="inline_image" width="600" height="300"/>

<p>The above graph shows a performance comparison of DBToaster-generated query engines against a commercial database system (DBX), a commercial stream processor (SPY), and several naive query evaluation strategies implemented in DBToaster that do not involve the viewlet transform.  (The commercial systems remain anonymous in accordance with the terms of their licensing agreements).</p>

<p>Performance is measured in terms of the rate at which each system can produce up-to-date (fresh) views of the query results.  As you can see, DBToaster regularly outperforms the commercial systems by <b>3-4 orders of magnitude.</b></p>

<p>The graph shows the performance of each system on six realtime data-warehousing queries based on the TPC-H query benchmark (Q3,Q11,Q17,Q18,Q22,SSB4), and six algorithmic trading queries (BSV, BSP, AXF, PSP, MST, VWAP).  These queries are all included as examples in the DBToaster distribution.</p>

<p>Depth-0 represents a naive query evaluation strategy where each refresh requires a full re-evaluation of the entire query.  Depth-1 is a well known technique called Incremental View Maintenance.  Naive recursive is a weaker form of the Viewlet Transform (we discuss the distinctions in depth in our VLDB 2012 paper).  Each of these was implemented by a DBToaster-generated engine.</p>