<p>
   <i>Note:</i> To compile and run queries using the Scala backend requires the Scala compiler. Please refer to <?= mk_link("Installation", "docs"); ?> for more details. 
</p>

<p>
   Since Scala is compatible with Java, one can write a Java application that interacts with generated Scala code and uses its routines for sending events to the query engine and retrieving the results back to the application. We demonstrate this with the following example.
</p>

<p>
   We use DBToaster to generate Scala code for a given SQL query, in this example <tt>rst.sql</tt> that ships with the release, and then compile the code into a JAR file. 
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
$> bin/dbtoaster -c rst.jar -l scala examples/queries/simple/rst.sql
</div>

<p>
   The following Java code communicates with the compiled query engine produced by DBToaster. Submiting events and retriving results is achieved using <a href="http://akka.io/">akka</a> actors. This example is equivalent to the Scala program presented in <?= mk_link("Scala Code Generation", "docs", "scala", "#apiguide"); ?>.
</p>

<div class="codeblock">
import ddbt.gen.*;
import ddbt.lib.Messages.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Inbox;

import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.$colon$colon;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import java.util.concurrent.TimeUnit;

class ExampleApp {
   public static final byte TUPLE_DELETE = 0x00;
   public static final byte TUPLE_INSERT = 0x01;
   private static final FiniteDuration DEFAULT_TIMEOUT = Duration.create(1L << 23, TimeUnit.SECONDS);
   private static final List EMPTY_LIST = List$.MODULE$.empty();
   public static List&lt;Object&gt; tuple(Object ... ts) {
      List&lt;Object&gt; result = EMPTY_LIST;
      for(int i = ts.length; i > 0; i--) {
         result = new $colon$colon&lt;Object&gt;(ts[i - 1], result);
      }
      return result;
   }

   public static void main(String[] args) {
      ActorSystem system = ActorSystem.create("mySystem");
      final Inbox inbox = Inbox.create(system);

      final ActorRef q = system.actorOf(Props.create(Dbtoaster.class), "Query");

      // Send events
      Tuple2 result;
      System.out.println("Insert a tuple into R.");
      q.tell(new TupleEvent(TUPLE_INSERT, "R", tuple(5L, 2L)), null);

      System.out.println("Insert a tuple into S.");
      q.tell(new TupleEvent(TUPLE_INSERT, "S", tuple(2L, 3L)), null);

      System.out.println("Insert a tuple into T.");
      q.tell(new TupleEvent(TUPLE_INSERT, "T", tuple(3L, 4L)), null);

      inbox.send(q, new GetSnapshot(new $colon$colon(1, EMPTY_LIST)));
      result = (Tuple2) (inbox.receive(DEFAULT_TIMEOUT));
      System.out.println("Result after this step: " + result._2().toString());

      System.out.println("Insert another tuple into T.");
      q.tell(new TupleEvent(TUPLE_INSERT, "T", tuple(3L, 5L)), null);

      // Retrieve result
      inbox.send(q, EndOfStream$.MODULE$);
      result = (Tuple2) (inbox.receive(DEFAULT_TIMEOUT));
      System.out.println("Final Result: " + result._2().toString());
      
      system.shutdown();
  }
}
</div>

<p>
   This example first creates an ActorSystem object and then launches the query actor.
   The events are sent to the query actor using <tt>TupleEvent</tt> objects whose constructor has the following parameters:
</p>

<table class="table">
   <tr>
      <th>Argument</th>
      <th>Comment</th>
   </tr>
   <!--tr>
      <td class="code">ord : int</td>
      <td>Order number of the event.</td>
   </tr-->
   <tr>
      <td class="code">op : byte</td>
      <td><tt>0x01</tt> for insertion, <tt>0x00</tt> for deletion.</td>
   </tr>
   <tr>
      <td class="code">stream : String</td>
      <td>Name of the stream as it appears in the SQL file.</td>
   </tr>
   <tr>
      <td class="code">data : List&lt;Object&gt;</td>
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
      <td class="code">view : List&lt;Integer&gt;</td>
      <td>List of maps that a snapshot is taken of.</td>
   </tr>
</table>

<p>
   <i>Note:</i> Currently, the DBToaster library does not offer a specialized interface for Java applications. We plan to resolve this issue in the future once the Scala API becomes stable.
</p>
<p>
   If this code has been saved to <tt>ExampleApp.java</tt>, the program can be compiled as follows:
</p>

<div class="codeblock">
$> javac -classpath "rst.jar:lib/dbt_scala/*" ExampleApp.java
$> jar cvf exampleapp.jar ExampleApp.class
</div>

<p>
   The resulting <tt>exampleapp.jar</tt> can be launched using the following command:
</p>

<div class="codeblock">
$> java -classpath "rst.jar:lib/dbt_scala/*:exampleapp.jar" ExampleApp
Insert a tuple into R.
Insert a tuple into S.
Insert a tuple into T.
Result after this step: List(20)
Insert another tuple into T.
Final Result: List(45)
</div>
