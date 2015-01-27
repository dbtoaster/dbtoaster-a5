<div class="warning">Warning: This API is subject to changes in future releases.</div>

<p>
   <i>Note:</i> To compile and run queries using the Scala backend requires the Scala compiler. Please refer to <?= mk_link("Installation", "docs"); ?> for more details. 
</p>

<a name="quickstart"></a>

<?=chapter("Compiling and running a query")?>
<p>
   DBToaster generates a JAR file for a query when using the <tt>-l scala</tt> and <tt>-c &lt;file&gt;</tt> switches:
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
   The command above compiles the query to <tt>test.jar</tt>, which can be run as follows:
</p>

<div class="codeblock">
$> java -classpath "test.jar:lib/dbt_scala/*" ddbt.gen.Dbtoaster
Java 1.7.0_45, Scala 2.10.3
Time: 0.008s (30/0)
ATIMESD:
306
</div>

<p>
   After processing all insertions and deletions, the final result is printed.
</p>

<p>
  <i>Note for Windows users:</i> When running compiled Scala programs under Cygwin or Windows directly, one should use Windows-style classpath separators (i.e., semicolons). For instance:
</p>
<div class="codeblock">
$> java -cp ".\test.jar;.\lib\dbt_scala\akka-actor_2.10-2.2.3.jar;.\lib\dbt_scala\dbtoaster_2.10-2.1-lms.jar;.\lib\dbt_scala\scala-library-2.10.2.jar;.\lib\dbt_scala\config-1.0.2.jar" ddbt.gen.Dbtoaster
Java 1.7.0_65, Scala 2.10.2
Time: 0.002s (30/0)
ATIMESD:
306
</div>
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
  val DEFAULT_TIMEOUT = akka.util.Timeout(1L << 42)

  def main(args: Array[String]) {
    val system = ActorSystem("mySystem")
    val q = system.actorOf(Props[Dbtoaster], "Query")
    var result:(StreamStat,List[Any]) = null

    // Send events
    println("Insert a tuple into R.")
    q ! TupleEvent(0, TupleInsert, "R", List(5L, 2L))

    println("Insert a tuple into S.")
    q ! TupleEvent(1, TupleInsert, "S", List(2L, 3L))

    println("Insert a tuple into T.")
    q ! TupleEvent(2, TupleInsert, "T", List(3L, 4L))

    result = scala.concurrent.Await.result(akka.pattern.ask(q, GetSnapshot(List(1)))(DEFAULT_TIMEOUT), DEFAULT_TIMEOUT.duration).asInstanceOf[(StreamStat,List[Any])]
    println("Result after this step: " + result._2(0))

    println("Insert another tuple into T.");
    q ! TupleEvent(3, TupleInsert, "T", List(3L, 5L))

    // Retrieve result
    result = scala.concurrent.Await.result(akka.pattern.ask(q, EndOfStream)(DEFAULT_TIMEOUT), DEFAULT_TIMEOUT.duration).asInstanceOf[(StreamStat,List[Any])]
    println("Final Result: " + result._2(0))

    system.shutdown
  }
}

</div>

<p>
   This example first creates an ActorSystem and then launches the query actor.
   The events are sent to the query actor using <tt>TupleEvent</tt> messages with the following structure:
</p>

<table class="table">
   <tr>
      <th>Argument</th>
      <th>Comment</th>
   </tr>
   <tr>
      <td class="code">ord : Int</td>
      <td>Order number of the event.</td>
   </tr>
   <tr>
      <td class="code">op : TupleOp</td>
      <td><tt>TupleInsert</tt> for insertion, <tt>TupleDelete</tt> for deletion.</td>
   </tr>
   <tr>
      <td class="code">stream : String</td>
      <td>Name of the stream as it appears in the SQL file.</td>
   </tr>
   <tr>
      <td class="code">data : List[Any]</td>
      <td>The values of the tuple being inserted into/deleted from the stream.</td>
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
      <td class="code">view : List[Int]</td>
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
Insert a tuple into R.
Insert a tuple into S.
Insert a tuple into T.
Result after this step: 20
Insert another tuple into T.
Final Result: 45
</div>

<a name="generatedcode"/></a>
<?=chapter("Generated Code Reference")?>
<p>
   The Scala code generator generates a single file containing an object and an actor for a query.
   Both of them are called <tt>Query</tt> by default if no other name has been specified using the <tt>-n <name></tt> switch.
</p>

<p>
   The code generated for the previous example looks as follows:
</p>

<div class="codeblock">
package ddbt.gen
import ddbt.lib._
...

// Query object used for standalone binaries
object Query {
  import Helper._
  def execute(args:Array[String],f:List[Any]=>Unit) = ...

  def main(args:Array[String]) {
    execute(args,(res:List[Any])=>{
      println("ATIMESD:\n"+M3Map.toStr(res(0))+"\n")
    })
  }
}

// Query actor
class Query extends Actor {
  import ddbt.lib.Messages._
  import ddbt.lib.Functions._

  // Maps/singletons that hold intermediate results
  var ATIMESD = 0L
  val ATIMESD_mT1 = M3Map.make[Long,Long]();
  ...

  // Triggers
  def onAddR(r_a:Long, r_b:Long) {
    ATIMESD += (ATIMESD_mR1.get(r_b) * r_a);
    ATIMESD_mT1_mR1.slice(0,r_b).foreach { (k1,v1) =>
      val atimesd_mtt_c = k1._2;
      ATIMESD_mT1.add(atimesd_mtt_c,(v1 * r_a));
    }
    ATIMESD_mS1.add(r_b,r_a);
  }

  def onDelR(r_a:Long, r_b:Long) { ... }
  ...
  def onDelT(t_c:Long, t_d:Long) { ... }
  def onSystemReady() { }

  ...
  def receive = {
    case TupleEvent(ord,TupleInsert,"R",List(v0:Long,v1:Long)) => if (t1>0 && (tN&127)==0) { val t=System.nanoTime; if (t>t1) { t1=t; tS=1; context.become(receive_skip) } else tN+=1 } else tN+=1; onAddR(v0,v1)
    ...
    case StreamInit(timeout) => onSystemReady(); t0=System.nanoTime; if (timeout>0) t1=t0+timeout*1000000L
    case EndOfStream | GetSnapshot(_) => t1=System.nanoTime;  sender ! (StreamStat(t1-t0,tN,tS),List(ATIMESD))
  }
}
</div>

<?=section("The query object")?>
<p>
   The query object contains the code used by the standalone binary to execute the query.
   Its <tt>execute</tt> method reads from the input streams specified in the query file and sends them to the query actor.
   The <tt>main</tt> method calls execute and prints the result when all tuples have been processed.
</p>

<?=section("The query actor")?>
<p>
   The actual query processor lives in the query actor.
   Events like tuple insertions and deletions are communicated to the actor using actor messages as described previously.
   The <tt>receive</tt> method routes events to the appropriate trigger method.
   For every stream <tt>R</tt>, there is an insertion <tt>onAddR</tt> and a deletion trigger <tt>onDelR</tt>.
   These trigger methods are responsible of updating the intermediate result.
   The map and singleton data structures at the top of the actor hold the intermediate result.
</p>

<p>
   The <tt>onSystemReady</tt> trigger is responsible of loading static information (<tt>CREATE TABLE</tt> statements in the query file) before the actual processing begins.
</p>

<p>
   The <tt>EndOfStream</tt> message is sent from the event source when it is exhausted. The query actor replies to this message with the current processing statistics (processing time, number of tuples processed, number of tuples skipped) and one or multiple query results.
</p>

<p>
   The <tt>GetSnapshot</tt> message can be used by an application to access the intermediate result. The query actor replies to this message with the current processing statistics and the results that the message asks for.
</p>

<p>
   The whole process is guarded by a timeout. If the timeout is reached, the actor will stop to process tuples.
</p>

<?=section("Partial materialization")?>

<p>
   Some of the work involved in maintaining the results of a query can be saved by performing partial materialization and only materialize the result when requested (i.e. when all tuples have been processed).
   This behaviour is especially desirable when the rate of querying the results is lower than the rate of updates, and can be enabled through the <tt>-F EXPRESSIVE-TLQS</tt> command line flag.
</p>

<p>
   Below is an example of a query where partial materialization is indeed beneficial (this query can be found as <tt>examples/queries/simple/r_lif_of_count.sql</tt> in the DBToaster download).
</p>

<div class="codeblock">
CREATE STREAM R(A int, B int)
FROM FILE 'examples/data/tiny/r.dat' LINE DELIMITED
csv ();

SELECT r2.C FROM (
  SELECT r1.A, COUNT(*) AS C FROM R r1 GROUP BY r1.A
) r2;
</div>

<p>
   When compiling this query with <tt>-F EXPRESSIVE-TLQS</tt>, the generated code now has functions representing top-level results:
</p>

<div class="codeblock">
def COUNT() = {
   val mCOUNT = M3Map.make[Long,Long]()
   val agg1 = M3Map.temp[Long,Long]()
   COUNT_1_E1_1.foreach { (r2_a,v1) =>
      val l1 = COUNT_1_E1_1.get(r2_a);
      agg1.add(l1,(if (v1 != 0) 1L else 0L));
   }
   agg1.foreach { (r2_c,v2) =>
      mCOUNT.add(r2_c,v2)
   }

   mCOUNT
}
</div>
