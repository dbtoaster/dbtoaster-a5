<div class="warning">Warning: This BETA API is not final, and subject to change before release.</div>

<a name="quickstart"></a>

<?=chapter("Compiling and running a query")?>

<p>
   <i>Note:</i> To compile and run queries using the Scala backend requires the Scala compiler to be installed. Please refer to <?= mk_link("Getting Started", "docs"); ?> for details 
</p>

<p>
   DBToaster generates a JAR file for a query when using the <tt>-l scala</tt> and the <tt>-c &lt;file&gt;</tt> switch:
</p>

<div class="codeblock">
$> cat examples/queries/simple/rst.sql
CREATE STREAM R(A int, B int) 
  FROM FILE 'examples/data/simple/r.dat' LINE DELIMITED csv;

CREATE STREAM S(B int, C int) 
  FROM FILE 'examples/data/simple/s.dat' LINE DELIMITED csv;

CREATE STREAM T(C int, D int)
  FROM FILE 'examples/data/simple/t.dat' LINE DELIMITED csv;

SELECT sum(A*D) AS AtimesD FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
$> bin/dbtoaster -c test.jar -l scala examples/queries/simple/rst.sql
</div>

<p>
   The command above compiles the query to <tt>test.jar</tt>.
   It can now be run as follows:
</p>

<div class="codeblock">
$> java -classpath "rst.jar:lib/dbt_scala/*" ddbt.gen.Dbtoaster
Java 1.7.0_45, Scala 2.10.3
Time: 0.008s (30/0)
ATIMESD:
306
</div>

<p>
   After processing all insertions and deletions, the final result is printed 
</p>

<a name="apiguide"/></a>
<?= chapter("Scala API Guide") ?>
<p>
   In the previous example, we used the standard main function to test the query. 
   However, to use the query in real applications, it has to be run from within an application.
</p>

<p>
   The following listing shows a simple example application that communicates with the query class.
   The communication between the application and the query class is handled using <a href="http://akka.io/">akka</a>.
</p>

<div class="codeblock">
package org.dbtoaster

import ddbt.gen._
import ddbt.lib.Messages._
import akka.actor._

object ExampleApp {
  def main(args: Array[String]) {
    val system = ActorSystem("mySystem")
    val q = system.actorOf(Props[Dbtoaster], "Query")

    // Send events
    q ! TupleEvent(0, TupleInsert, "R", List(5L, 2L))
    q ! TupleEvent(1, TupleInsert, "S", List(2L, 3L))
    q ! TupleEvent(2, TupleInsert, "T", List(3L, 4L))

    // Retrieve result
    val to=akka.util.Timeout(1L << 42)
    val result = scala.concurrent.Await.result(akka.pattern.ask(q, EndOfStream)(to), to.duration).asInstanceOf[(StreamStat,List[Any])]

    println("Result: " + result._2(0))

    system.shutdown
  }
}
</div>

<p>
   This example first creates an ActorSystem and then launches the query actor.
   The events are sent to the query using <tt>TupleEvent</tt> messages with the following structure:
</p>

<table class="table">
   <tr>
      <th>Argument</th>
      <th>Comment</th>
   </tr>
   <tr>
      <td class="code">ord: Int</td>
      <td>Order number of the event.</td>
   </tr>
   <tr>
      <td class="code">op:TupleOp</td>
      <td><tt>TupleInsert</tt> for an insertion, <tt>TupleDelete</tt> for a deletion.</td>
   </tr>
   <tr>
      <td class="code">stream:String</td>
      <td>Name of the stream as it appears in the SQL file.</td>
   </tr>
   <tr>
      <td class="code">data:List[Any]</td>
      <td>The values of the tuple that was inserted into/deleted from the stream.</td>
   </tr>
</table>

<p>
   To retrieve the final result, an <tt>EndOfStream</tt> message is sent to the query actor.
   Alternatively the intermediate result of a query can be retrieved using a <tt>GetSnapshot</tt> message with the following structure:
</p>

<table class="table">
   <tr>
      <th>Argument</th>
      <th>Comment</th>
   </tr>
   <tr>
      <td class="code">view:List[Int]</td>
      <td>List of maps that a snapshot is taken of.</td>
   </tr>
</table>

<p>
   Assuming that the example code has been saved as <tt>example.scala</tt>, it can be compiled with:
</p>

<div class="codeblock">
$> scalac -classpath "rst.jar:lib/dbt_scala/*" -d example.jar example.scala
</div>

<p>
   It can then be launched with the following command:
</p>

<div class="codeblock">
$> java -classpath "rst.jar:lib/dbt_scala/*:example.jar" org.dbtoaster.ExampleApp
Result: 20
</div>

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
When the <span class="code">run()</span> method is called, the static tables are loaded and the processing
of events from the declared sources starts. The function returns when the sources provide no
more events.</p>

<?=section("Retrieving results")?>
<p>
To retrieve the result, the <span class="code">get<i>RESULTNAME</i>()</span> functions are used. In the example above,
the <span class="code">get<i>RESULTNAME</i>()</span> method is simple but more complex methods may be generated
and the return value may be a collection instead of a single value.</p>

<?=subsection("Queries computing collections")?>
<p>
Consider the following query:
<div class="codeblock">CREATE STREAM R(A int, B int) 
  FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE 'examples/data/tiny/s.dat' LINE DELIMITED
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
FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
csv ();

SELECT r2.C FROM (
  SELECT r1.A, COUNT(*) AS C FROM R r1 GROUP BY r1.A
) r2;
</div>
When compiling this query with the <span class="code">-F EXPRESSIVE-TLQS</span> command line flag, the function to retrieve
the results is much more complex, unlike the functions that we have seen before. It uses the partial materialization 
<span class="code">COUNT_1_E1_1</span> to compute the result:
<div class="codeblock">$&gt; bin/dbtoaster -l scala -F EXPRESSIVE-TLQS examples/queries/simple/r_lift_of_count.sql 
    def getCOUNT():K3IntermediateCollection[(Long), Long] = {
      (COUNT_1_E1_1.map((y:Tuple2[(Long),Long]) => 
      ...
      )
    };
</div></p>

<?=section("Using queries in Java programs")?>
<p>
Since Scala is compatible with Java, it is possible to use the queries in Java applications. 
In order to use a query in Java, the Java application has to reference three libraries: 
<ul>
	<li>The library containing the generated code for the query</li>
	<li>The Scala library</li>
	<li>Scala DBToaster library</li>
</ul>
</p>
<p>
The following code snippet illustrates how a query can be executed from within a Java application:
<div class="codeblock">
import org.dbtoaster.dbtoasterlib.*;
import org.dbtoaster.*;

public class MainClass {
	public static void main(String[] args) {
		final Query q = new Query();
		
		QueryInterface.DBTMessageReceiver rcvr = new QueryInterface.DBTMessageReceiver() {
			public void onTupleProcessed() {
				// do nothing
			}
			
			public void onQueryDone() {
				// print the results
				q.printResults();
			}
		};
		
		QueryInterface.QuerySupervisor supervisor = new QueryInterface.QuerySupervisor(q, rcvr);
		supervisor.start();
	}
}
</div>
This program executes the referenced query and prints the result once there are no more tuples
to be processed.
</p>
<?=subsection("Compiling in eclipse")?>
To compile the previously presented program in eclipse (with the official Scala plugin installed), 
we can create a new Java project with a single <span class="code">.java</span>-file containing 
the program. In the Java Build Path section of the project's properties the previously listed 
libraries have to be added. The Scala library can be added by clicking the "Add Library" button,
while the other libraries can be added as external JARs. After adding the libraries to the project,
the project should compile and execute the query once run.
