<p>DBToaster creates query engines for embedding into applications that require real-time, low-latency data processing and monitoring capabilities. </p>

<p>DBToaster-generated engines are especially optimized for long-lived queries, where query results must be kept up-to-date with rapidly changing input data.</p>  

<p>Applications include:
<ul>
<li>Algorithmic Trading</li>
<li>Real-time Data Warehousing</li>
<li>Network/Cluster Monitoring</li>
<li>Clickstream Analysis</li>
<li>and more...</li>
</ul></p>

<p>The DBToaster compiler accepts queries written in SQL, and generates query engine code that can be incorporated directly into any C++ or Scala project (with support for more languages on the way).  DBToaster-generated engines use each platform's native collection types, making integration with existing projects a breeze. </p>

<h3>How can DBToaster be so fast?</h3>
<p>Traditional relational databases are slow because they are designed to support arbitrary hand-written queries.  Nowadays though, few people execute queries directly.  Most queries are generated automatically based on templates (e.g., by PHP, C#/LINQ, Scala/SLIQ, Java/JDBC, etc...). </p>

<p>DBToaster custom-tailors each engine it creates to the needs of a specific application.  This engine supports only query processing functionality that the application requires, avoiding the overhead of supporting unnecessary features.  The typical result is a speedup of <?=mk_link("3-4 orders of magnitude", "home", "performance")?> over existing state of the art data-management systems.</p>

<p>DBToaster also employs an innovative technique that exploits incrementality to efficiently maintain query results in real-time as data changes.  As a consequence, DBToaster-generated engines provide extremely low-latency access to query results, and efficiently support monitoring of result values.
</p>
