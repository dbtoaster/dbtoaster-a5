

<p>We do not claim that DBToaster is the right solution for everyone, but you should check out DBToaster if you need to
<ul>
<li>maintain materialized views of complex SQL queries,</li>
<li>read these views and care about very high refresh rates / low refresh latencies,</li>
<li>work with standing (aka continuous) rather than ad-hoc queries, i.e. you want to monitor the changing result of a given query over time, as the data changes, and</li>
<li>do not work with extremely large datasets (this is a temporary restriction until we release our parallel/secondary storage runtimes).</li>
</ul></p>

<p>DBToaster may be also right for you even if you do not care so much about low view refresh latencies: DBToaster turns a set of queries into efficient specialized code for processing just these queries. DBToaster generates code that you can link into your applications. No further software (such as a separate database server or CEP engine) is required. Thus DBToaster is a very lightweight way of including fixed (parameterized) SQL queries in your applications.</p>


<?= chapter("Materialized Views of Nested Queries") ?>
<p>DBToaster supports efficient materialized views of nested SQL queries. Many commercial database systems support materialized views / incremental view maintenance, but no other system does so for nested SQL queries, even though they are essential for complex analytics.</p>

<p>Nesting refers to the presence of select-statements (SQL queries) in the SELECT, FROM, or WHERE clauses of SQL queries.</p>

<?= chapter("C++ and Scala code Generation") ?>
<p>DBToaster is able to generate both C++ an Scala code that can be integrated into applications written in these languages (as well as Java, since Scala lives in the Java ecosystem and compiles to Java Bytecode that can be linked with Java applications).</p>

<a name="roadmap"></a>
<?= chapter("Feature Roadmap") ?>

<table>
<tr><th>Milestone</th><th>Expected Date</th><th>Feature Summary</th></tr>

<tr><th>Milestone 1</th><td>Fall 2012</td>
  <td>
    <ul>
      <li>Support for all SQL functionality required by the TPC-H benchmark except ORDER-BY, MIN/MAX, and NULL-values/outer joins.</li>
      <li>Further performance improvements</li>
    </ul>
  </td></tr>
  
<tr><th>Milestone 2</th><td>Winter 2012/Spring 2013</td>
  <td>
    <ul>
      <li>A parallel runtime for DBToaster</li>
    </ul>
  </td></tr>  

<tr><th>Milestone 3</th><td>Summer 2013</td>
  <td>
    <ul>
      <li>Updated backend optimizer (code fusion, beta-reduction, etc...), updated C++ code generator, generation of custom-datastructures for maintenance.  Phase out Boost.  This should lead to considerable efficiency improvements.</li>
      <li>Synthesis of tree-based datastructures for efficiently processing and indexing theta-joins.</li>
      <li>Support order; Support for ORDER-BY, MIN and MAX</li>
    </ul>
  </td></tr>  

<tr><th>Milestone 4</th><td>Fall 2013</td>
  <td>
    <ul>
      <li>Frontends for APL-style array processing languages (e.g., The R analytics language, Matlab, Q).  DBToaster will be able to compile analytical queries expressed in such langauges in addition to SQL, with similar performance.</li>
    </ul>
  </td></tr>  

</table>

<?= section("SQL92 Support") ?>
<p>DBToaster presently only supports the COUNT, COUNT DISTINCT, SUM, and AVG aggregates.  Support for MIN and MAX is slated for Milestone 3.  </p>

<p>DBToaster does not presently suport the DISTINCT, UNION, LIMIT, ORDER BY and HAVING clauses of SELECT statements.  Support for DISTINCT, UNION, and HAVING is slated for Milestone 1.   Support for LIMIT and ORDER BY is slated for Milestone 4.</p>

<p>DBToaster does not presently support SQL's NULL value semantics (including OUTER JOINs).  We are investigating several potential solutions, and will commit to a milestone once more research has been performed.</p>

<p>All other unsupported features of SQL92 will be implemented as resources become available, or if there is sufficient demand.</p>

<?= section("Scalability") ?>

<p>DBToaster's internal aggregate calculus has several properties that make it exteremely ammenable to distribution.  We are in the process of implementing a scalable distributed runtime for DBToaster, slated for release as Milestone 2.</p>

<?= section("Dynamic Runtimes") ?>

<p>We are aware of demand for a platform for executing DBToaster-generated engines, where the query workload can be managed dynamically (i.e., queries can be added/removed at runtime).  This feature is slated for release, but at present we do not have the resources to commit to a specific milestone.</p>

<?= section("On-Demand Template Execution") ?>

<p>A powerful application of DBToaster is for evaluating template-style queries.  When an application is compiled, a DBToaster-generated engine could be produced to efficiently support evaluation of one or more queries with externally-bound variables.  Presently, a fragment of such queries can be implemented by rewriting the query to include externally bound variables as output columns.  We hope to have this feature implemented in an upcoming milestone release</p>
