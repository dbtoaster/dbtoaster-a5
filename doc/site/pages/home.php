<p>DBToaster creates query engines for embedding into applications that require real-time, low-latency data processing and monitoring capabilities. </p>

<p>DBToaster-generated engines are optimized for long-lived queries, where query results must be kept up-to-date with rapidly changing input data. Using database terminology, DBToaster engines maintain
in-memory materialized views. Our performance claims refer to the speed at which DBToaster engines
refresh views as the input data changes.</p>  

<p>Applications include:
<ul>
<li>Algorithmic Trading</li>
<li>Real-time Data Warehousing</li>
<li>Network/Cluster Monitoring</li>
<li>Clickstream Analysis</li>
<li>and more...</li>
</ul></p>

<p>The DBToaster compiler accepts queries written in SQL, and generates query engine code that can be incorporated directly into any C++ or Scala project (with support for more languages on the way).  DBToaster-generated engines use each platform's native collection types, making integration with existing projects a breeze. </p>

<h3>Is DBToaster right for you?</h3>
<p>We do not claim that DBToaster is the right solution for everyone, but you should check out DBToaster if you need to
<ul>
<li>maintain materialized views of complex SQL queries,</li>
<li>read these views and care about very high refresh rates / low refresh latencies,</li>
<li>work with standing (aka continuous) rather than ad-hoc queries, i.e. you want to monitor the changing result of a given query over time, as the data changes, and</li>
<li>do not work with extremely large datasets (this is a temporary restriction until we release our parallel/secondary storage runtimes).</li>
</ul></p>

<p>DBToaster may be also right for you even if you do not care about low view refresh latencies: DBToaster turns a set of queries into efficient specialized code for processing just these queries. DBToaster generates code that you can link into your applications. No further software (such as a separate database server or CEP engine) is required. Thus DBToaster is a very lightweight way of including fixed (parameterized) SQL queries in your applications.</p>


<h3>How can DBToaster be so fast?</h3>
<p>Traditional relational databases are slow because they are designed to support arbitrary hand-written queries.  Nowadays though, few people execute queries directly.  Most queries are generated automatically based on templates (e.g., by PHP, C#/LINQ, Scala/SLIQ, Java/JDBC, etc...). </p>

<p>DBToaster custom-tailors each engine it creates to the needs of a specific application.  This engine supports only query processing functionality that the application requires, avoiding the overhead of supporting unnecessary features.  The typical result is a speedup of <?=mk_link("3-4 orders of magnitude", "home", "performance")?> over existing state of the art data-management systems.</p>

<p>DBToaster also employs an innovative technique that exploits incrementality to efficiently maintain query results in real-time as data changes.  As a consequence, DBToaster-generated engines provide extremely low-latency access to query results, and efficiently support monitoring of result values.
</p>


