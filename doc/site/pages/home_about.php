<p>DBToaster generates lightweight, specialized, embeddable query engines for applications that require real-time, low-latency data processing and monitoring capabilities.
The DBToaster compiler accepts queries written in SQL and generates code that can be easily incorporated into any C++ or JVM-based (Java, Scala, ...) project.
</p>


<h3>DBToaster is not a classical database system</h3>

<p>DBToaster-generated engines are optimized for long-lived queries, where query results must be kept up-to-date with rapidly changing input data. Using database terminology, DBToaster engines maintain in-memory materialized views.
</p>  

<p>Applications include:
<ul>
<li>Algorithmic Trading</li>
<li>Real-time Data Warehousing</li>
<li>Network/Cluster Monitoring</li>
<li>Clickstream Analysis</li>
<li>and more...</li>
</ul></p>


<p>Check out DBToaster if you need to
<ul>
<li>maintain materialized views of complex SQL queries,</li>
<li>read these views and care about very high refresh rates / low refresh latencies,</li>
<li>work with standing (aka continuous) rather than ad-hoc queries, i.e. you want to monitor the changing result of a given query over time, as the data changes, and</li>
</ul></p>

<p>DBToaster may be also right for you even if you do not care about low view refresh latencies: DBToaster turns a set of queries into efficient specialized code for processing just these queries. DBToaster generates code that you can link into your applications. No further software (such as a separate database server or CEP engine) is required. Thus DBToaster is a very lightweight way of including fixed (parameterized) SQL queries in your applications.</p>


<p>Here is a <a href="papers/whitepaper.pdf">white paper</a> that may help you
decide whether to try out DBToaster.</p>

<p><a href="papers/ecocloud2013-dbtoaster-mn.pdf">The DBToaster at EcoCloud 2013 presentation</a> gives a high-level overview of the DBToaster project.</p>



<h3>DBToaster code is fast</h3>
<p>Traditional relational databases are slow because they are designed to support arbitrary hand-written queries.  Nowadays though, few people execute queries directly.  Most queries are generated automatically based on templates.
DBToaster custom-tailors each engine it creates to the needs of a specific application.  This engine supports only query processing functionality that the application requires, avoiding the overhead of supporting unnecessary features.
</p>

<p>DBToaster employs incrementality to efficiently maintain query results in real-time as data changes.  As a consequence, DBToaster-generated engines provide extremely low-latency access to query results, and efficiently support monitoring of result values.
</p>

<p>
DBToaster-generated code is typically by <?=mk_link("3-6 orders of magnitude", "home", "performance")?> faster than existing state of the art data-management systems when we measure the time it takes to refresh a view given an update to the base data.
</p>


