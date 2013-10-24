<img src="engine_bakeoff0.png" class="inline_image" width="600" />
<img src="engine_bakeoff1.png" class="inline_image" width="600" />

<p>The above graphs show a performance comparison of DBToaster-generated query engines against a commercial database system (DBX), a commercial stream processor (SPY), and several naive query evaluation strategies implemented in DBToaster that do not involve the Higher-Order incremental view maintenance. (The commercial systems remain anonymous in accordance with the terms of their licensing agreements).</p>

<p>Performance is measured in terms of the rate at which each system can produce up-to-date (fresh) views of the query results.  As you can see, DBToaster regularly outperforms the commercial systems by <b>3-4 orders of magnitude.</b></p>

<p>The graphs show the performance of each system on a set of realtime data-warehousing queries based on the TPC-H query benchmark (Q1-Q22,Q11a,Q17a,Q18a,Q22a,SSB4), six algorithmic trading queries (BSV, BSP, AXF, PSP, MST, VWAP), and two scientific queries (MDDB1 and MDDB2).  These queries are all included as examples in the DBToaster distribution.</p>

<p>Depth-0 represents a naive query evaluation strategy where each refresh requires a full re-evaluation of the entire query.  Depth-1 is a well known technique called Incremental View Maintenance.  Naive recursive is a weaker form of the Higher-Order IVM (we discuss the distinctions in depth in our technical report).  Each of these was implemented by a DBToaster-generated engine.</p>
