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
$> bin/dbtoaster -c test.jar -l scala examples/queries/simple/rst.sql
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
import java.util.concurrent.TimeUnit;

class ExampleApp {
   public static final int TUPLE_DELETE = 0x00;
   public static final int TUPLE_INSERT = 0x01;

   public static List&lt;Object&gt; tuple(Object ... ts) {
      List&lt;Object&gt; result = List$.MODULE$.empty();
      for(int i = ts.length; i > 0; i--) {
         result = new $colon$colon&lt;Object&gt;(ts[i - 1], result);
      }
      return result;
   }

   public static void main(String[] args) {
      ActorSystem system = ActorSystem.create("mySystem");

      final ActorRef q = system.actorOf(Props.create(Dbtoaster.class), "Query");

      // Send events
      q.tell(new TupleEvent(0, TUPLE_INSERT, "R", tuple(5L, 2L)), null);
      q.tell(new TupleEvent(1, TUPLE_INSERT, "S", tuple(2L, 3L)), null);
      q.tell(new TupleEvent(2, TUPLE_INSERT, "T", tuple(3L, 4L)), null);

      // Retrieve result
      final Inbox inbox = Inbox.create(system);
      inbox.send(q, EndOfStream$.MODULE$);
      Tuple2 result = (Tuple2) (inbox.receive(Duration.create(1L << 23, TimeUnit.SECONDS)));

      System.out.println("Result: " + result._2().toString());
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
   <tr>
      <td class="code">ord : int</td>
      <td>Order number of the event.</td>
   </tr>
   <tr>
      <td class="code">op : int</td>
      <td><tt>TUPLE_INSERT</tt> for insertion, <tt>TUPLE_DELETE</tt> for deletion.</td>
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
Result: List(20)
</div>
