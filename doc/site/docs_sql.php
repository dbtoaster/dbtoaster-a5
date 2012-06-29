<a name="create"/>
<?= chapter("CREATE") ?>
<center>Declare a relation for use in the query.</center>
<div class="codeblock">
create_statement := 
  CREATE { TABLE | STREAM } &lt;name&gt; ( &lt;schema&gt; ) 
         [&lt;source_declaration&gt;]

schema := [&lt;var_1&gt; &lt;type_1&gt; [, &lt;var_2&gt; &lt;type_2&gt; [, ...]]]

source_declaration := source_stream source_adaptor

source_stream := 
  FROM FILE '&lt;path&gt;' {
      FIXEDWIDTH &lt;bytes_per_row&gt;
    | LINE DELIMITED
    | '&lt;delim_string&gt;' DELIMITED
  } 

source_adaptor := 
  &lt;adaptor_name&gt; (
    [&lt;param_1&gt; := '&lt;value&gt;' [, &lt;param_2&gt; := '&lt;value&gt;' [, ...]]]
  )
</div>

<p>A create statement defines a relation named <tt>name</tt> with the indicated schema and declares a method for automatically populating/updating rows of that relation.</p>

<p>Each relation may be declared to be either a Stream or a Table:
<ul>
<li>Tables are static data sources.  A table is read in prior to query monitoring, and is assumed to remain constant throughout query evaluation and monitoring.</li>
<li>Streams are dynamic data sources.  Stream updates are read in one tuple at a time as data becomes available, and the query views are refreshed (monitored) after every update.</li>
</ul></p>

<p>The source declaration allows DBToaster (either in the interpreter, or the generated source code) to automatically update the relation.  A source declaration consists of stream and adaptor components.  The stream component defines where data should be read from, and how records in the data are delimited.  At present, DBToaster only supports reading tuples from files. 
</p>

<p>If the same file is referenced multiple times, the file will only be scanned once, and events will be generated in the order in which they appear in the file.
</p>

<p>The adaptor declares how to parse fields out of each record.  See below for documentation on DBToaster's standard adaptors package.
</p>

<p>The source declaration field is optional in source code mode.  User programs may inject updates manually, or specify sources programatically while initializing the DBToaster-generated code.  
</p>

<div class="codeblock">
CREATE STREAM R(a int, b date)
FROM FILE 'data/r.csv' LINE DELIMITED 
CSV (fields := '|')
</div>

<hr/>
<a name="select"/>
<?=chapter("SELECT") ?>
<center>Declare a query to monitor</center>

<div class="codeblock">
select_statement := 
  SELECT &lt;target_1&gt; [, &lt;target_2&gt; [, ...]] 
  FROM &lt;source_1&gt; [, &lt;source_2&gt; [, ...]]
  WHERE &lt;condition&gt;
  [GROUP BY &lt;group_vars&gt;]

target := &lt;expression&gt; [[AS] &lt;target_name&gt;] | * | *.* 
        | &lt;source_name&gt;.*

source := &lt;relation_name&gt; [[AS] &lt;source_name&gt;]
  | (&lt;select_statement&gt;) [AS] &lt;source_name&gt;
  | &lt;source&gt; [NATURAL] JOIN &lt;source&gt; [ON &lt;condition&gt;]

expression :=  (&lt;expression&gt;) | &lt;int&gt; | &lt;float&gt; | '&lt;string&gt;' 
  | &lt;var&gt; | &lt;source&gt;.&lt;var&gt;
  | &lt;expression&gt; { + | - | * | / } &lt;expression&gt;
  | -&lt;expression&gt;
  | (SELECT &lt;expression&gt; FROM ...)
  | SUM(&lt;expression&gt;) | COUNT(* | &lt;expression&gt;) 
  | AVG(&lt;expression&gt;) | COUNT(DISTINCT [var1, [var2, [...]]])
  | &lt;inline_function&gt;([&lt;expr_1&gt; [, &lt;expr_2&gt; [, ...]]])
  | DATE('yyyy-mm-dd')
  | EXTRACT({year|month|day} FROM &lt;date&gt;)

condition := (&lt;condition&gt;) | true | false | not (&lt;condition&gt;)
  | &lt;expression&gt; { < | <= | > | >= | = | <> } &lt;expression&gt;
  | &lt;expression&gt; { < | <= | > | >= | = | <> } { SOME | ALL } 
                   &lt;select_statement&gt;
  | &lt;condition&gt; AND &lt;condition&gt; | &lt;condition&gt; OR &lt;condition&gt;
  | EXISTS &lt;select_statement&gt;
  | &lt;expression&gt; BETWEEN &lt;expression&gt; AND &lt;expression&gt;
  | &lt;expression&gt; IN &lt;select_statement&gt;
  | &lt;expression&gt; LIKE &lt;matchstring&gt;
  

</div>

<p>DBToaster SQL's SELECT operation differs slightly from the SQL-92 standard.
<ul>
<li>CASE, UNION, LIMIT, ORDER BY, and HAVING are not supported.</li>
<li>Only the COUNT, SUM, and AVG aggregates are supported.</li>
<li>The only supported types are int, float, date, and string/varchar.</li>
<li>Variable scoping rules are slightly stricter than the SQL standard (you may need to use fully qualified names in some additional cases).</li>
<li>Support for division is limited.  DBToaster does not currently check for, or react to divide by zero errors.  If a result value ever becomes NAN or INFTY, it will no longer be possible to incrementally maintain it.</li>
<li>DBToaster's aggregate functions all produce results when evaluated over empty sets (as opposed to the NULL required by the SQL standard).  The default values for SUM, COUNT, and AVERAGE are all 0.</li>
<li>DBToaster does not allow non-aggregate queries to evaluate to singleton values.  That is, the query<br/>
<tt>SELECT 1 FROM R WHERE R.A = (SELECT A FROM S)</tt><br/>
is a compile-time error in DBToaster, as opposed to simply having the potential to cause a run-time error in SQL92.  An equivalent, valid query would be:<br/>
<tt>SELECT 1 FROM R WHERE R.A IN (SELECT A FROM S)</tt>
</li>
</ul>
</p>

<p>See the <a href="index.php?page=docs&subpage=stdlib">Standard Functions Documentation</a> for documentation on DBToaster's standard function library.</p>

<p>DBToaster maintains query results in the form of either multi-key dictionaries (a.k.a., maps, hashmaps, etc...), or single values.  Each query result is assigned a name based on the query.<p>
<ul>
<li>Non-aggregate queries produce a dictionary named "COUNT".  Keys are formed from the target fields of the SELECT.  Values are the number of times the tuple occurs in the output (i.e., the query includes an implicit group-by COUNT(*) aggregate).</li>
<li>Singleton (non-grouping) aggregate queries produce a single value result for each aggregate target in the SELECT.  The result names are assigned based on the name of each target (i.e., using the name following the optional <tt>AS</tt> clause, or a procedurally generated name otherwise).</li>
<li>Group-by aggregate queries produce a dictionary for each aggregate target.  The non-aggregate (group-by) targets are used as keys for the dictionary (as in non-aggregate queries), and the value is the aggregate value for each group.  The dictionaries are named based on the name of each aggregate target (as for singleton aggregate queries)</li>
</ul></p>

<hr/>

<div class="codeblock">
SELECT SUM(R.A * T.D) FROM R NATURAL JOIN S, T WHERE S.C = T.C;
</div>

<a name="include"/>
<?=chapter("INCLUDE") ?>
<div class="codeblock">
include_statement := INCLUDE 'file'
</div>

Import the contents of the selected file into DBToaster.  The file path is interpreted relative to the current working directory.

