
<div class="warning">Warning: This BETA API is not final, and subject to change before release.</div>

<a name="quickstart"></a>
<?=chapter("Quickstart Guide")?>

<?=section("Prerequisites")?>
<ul>
	<li>DBToaster Beta1</li>
	<li>Scala 2.9.2</li>
	<li>JVM (preferably a 64-bit version)</li>
</ul>
<i>Note:</i> The following steps have been tested on Fedora 14 (64-bit) and Ubuntu 12.04 (32-bit), the commands may be slightly different for other operating systems

<?=section("Compiling and running your first query")?>
We start with a simple query that looks like this:
<div class="codeblock">
CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT SUM(r.A*s.C) as RESULT FROM R r, S s WHERE r.B = s.B;
</div>
This query should be saved to a file named <span class="code">rs_example.sql</span>.
<p>
To compile the query to Scala code, we invoke the DBToaster compiler with the following command:
<div class="codeblock">$&gt; bin/dbtoaster -l scala -o rs_example.scala rs_example.sql</div>
This command will produce a file <span class="code">rs_example.scala</span> (or any other filename specified by the <span class="code">-o [filename]</span> switch) which contains the Scala code representing the query.</p>
<p>
To compile the query to a JAR file, we invoke the DBToaster compiler with the <span class="code">-c [JARname]</span> switch:
<div class="codeblock">$&gt; bin/dbtoaster -l scala -c rs_example rs_example.sql</div>
<i>Note:</i> The ending <span class="code">.jar</span> is automatically appended to the name of the JAR.</p>
<p>
The resulting JAR contains a main function that can be used to test the query. It can be run using the following command assuming that the Scala DBToaster library can be found in the subdirectory <span class="code">lib/dbt_scala</span>:
<div class="codeblock">$&gt; scala -classpath "rs_example.jar:lib/dbt_scala/dbtlib.jar" \
         org.dbtoaster.RunQuery
</div>
After all tuples in the data files were processed, the result of the query will be printed:
<div class="codeblock">Run time: 0.042 ms
&lt;RESULT&gt;156 &lt;/RESULT&gt;
</div></p>

<a name="apiguide"/></a>
<?= chapter("Scala API Guide") ?>
The following example shows how a query can be ran from your own Scala code. Suppose we have a the following source code in <span class="code">main_example.scala</span>:
<div class="codeblock">import org.dbtoaster.Query

package org.example {
  object MainExample {
    def main(args: Array[String]) {
      Query.run()
      Query.printResults()
    }
  }
}
</div>
This program will start the query and output its result after it finished.
<p>
To retrieve results, the <span class="code">get<i>RESULTNAME</i>()</span> of the <span class="code">Query</span> object can be used.</p>
<p>
<i>Note:</i>The <span class="code">get<i>RESULTNAME</i>()</span> functions are not thread-safe, meaning that results can be 
inconsistent if they are called from another thread than the query thread. A thread-safe alternative to retrieve
the results is planned for future versions of DBToaster.</p>
<p>
The program can be compiled to <span class="code">main_example.jar</span> using the following command (assuming that the query was compiled to a file named <span class="code">rs_example.jar</span>):
<div class="codeblock">
$&gt; scalac -classpath "rs_example.jar" -d main_example.jar main_example.scala
</div>
The resulting program can now be launched with:
<div class="codeblock">
$&gt; scala -classpath "main_example.jar:rs_example.jar:lib/dbt_scala/dbtlib.jar" org.example.MainExample
</div>
The <span class="code">Query.run()</span> method takes a function of type <span class="code">Unit => Unit</span> as an optional argument which is called every time when an event was processed. 
This function can be used to retrieve results while the query is still running.</p>
<p>
<i>Note:</i> The function will be executed on the same thread on which the query processing takes place, blocking further query processing while the function is being run.</p>

<a name="generatedcode"/></a>
<?=chapter("Generated Code Reference")?>
The DBToaster Scala codegenerator generates a single file containing an object <span class="code">Query</span> in the package <span class="code">org.dbtoaster</span>.
<p>
For the previous example the generated code looks like this:
<div class="codeblock">// Imports
import java.io.FileInputStream;
...

package org.dbtoaster {
  // The generated object
  object Query {
    // Declaration of sources
    val s1 = createInputStreamSource(
          new FileInputStream("../../experiments/data/simple/tiny/r.dat"), ...
      );
    ...

    // Data structures holding the intermediate result
    var RESULT = SimpleVal[Long](0);
    ...

    // Functions to retrieve the result
    def getRESULT():Long = {
      RESULT.get()
    };

    // Trigger functions
    def onInsertR(var_R_A: Long,var_R_B: Long) = ...
    ...
    def onDeleteS(var_S_B: Long,var_S_C: Long) = ...
    
    // Functions that handle static tables and system initialization
    def onSystemInitialized() = ...
    def fillTables(): Unit = ...
    
    // Function that dispatches events to the appropriate trigger functions 
    def dispatcher(event: DBTEvent, 
                   onEventProcessedHandler: Unit => Unit): Unit = ...
    
    // (Blocking) function to start the execution of the query
    def run(onEventProcessedHandler: Unit => Unit = (_ => ())): Unit = ...
    
    // Prints the query results in some XML-like form (for debugging) 
    def printResults(): Unit = ...
  }
}
</div></p>
<p>
When the <span class="code">run</span> method is called, the static tables are loaded and the processing
of events from the declared sources starts. The function returns when the sources provide no
more events.</p>

<?=section("Retrieving results")?>
<p>
To retrieve the result, the <span class="code">get<i>RESULTNAME</i>()</span> functions are used. In the example above,
the <span class="code">get<i>RESULT</i>()</span> method is simple but more complex methods may be generated
and the return value may be a collection instead of a single value.</p>

<?=subsection("Queries computing collections")?>
<p>
Consider the following query:
<div class="codeblock">CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny/s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT r.B, SUM(r.A*s.C) as RESULT_1, SUM(r.A+s.C) as RESULT_2 FROM R r, S s WHERE r.B = s.B GROUP BY r.B;
</div>
In this case two functions are being generated that can be called to retrieve the result, each of them representing
one of the result columns:
<div class="codeblock">def getRESULT_1():K3PersistentCollection[(Long), Long] = ...
def getRESULT_2():K3PersistentCollection[(Long), Long] = ...
</div>
In this case, the functions return a collection containing the result. For further processing, the results can be converted 
to lists of key-value pairs using the <span class="code">toList()</span> method of the collection class. The key in the pair corresponds 
to the columns in the <span class="code">GROUP BY</span> clause, in our case <span class="code">r.B</span>. The value corresponds to the aggregated 
value for the corresponding key.</p>

<?=subsection("Partial Materialization")?>
<p>
Some of the work involved in maintaining the results of a query can be saved by performing partial materialization
and only computing the final results when invoking <span class="code">tlq_t</span>'s <span class="code">get_<i>TLQ_NAME</i></span> functions. This
behaviour is especially desirable when the rate of querying the results is lower than the rate of updates, and
can be enabled through the <span class="code">-F EXPRESSIVE-TLQS</span> command line flag.</p>

<p>Below is an example of a query where partial materialization is indeed beneficial.
<div class="codeblock">CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny/r.dat' LINE DELIMITED
csv ();

SELECT r2.C FROM (
  SELECT r1.A, COUNT(*) AS C FROM R r1 GROUP BY r1.A
) r2;
</div>
When compiling this query with the <span class="code">-F EXPRESSIVE-TLQS</span> command line flag, the function to retrieve
the results is much more complex, unlike the functions that we have seen before. It uses the partial materialization 
<span class="code">COUNT_1_E1_1</span> to compute the result:
<div class="codeblock">$&gt; bin/dbtoaster -l scala -F EXPRESSIVE-TLQS test/queries/simple/r_lift_of_count.sql 
    def getCOUNT():K3IntermediateCollection[(Long), Long] = {
      (COUNT_1_E1_1.map((y:Tuple2[(Long),Long]) => 
      ...
      )
    };
</div></p>