<p>
   <i>Note:</i> To compile and run queries using the Scala backend requires the Scala compiler. Please refer to <?= mk_link("Installation", "docs"); ?> for more details. 
</p>

<p>
   Since Scala is compatible with Java, it is possible to use generated Scala code in Java applications. Currently, the DBToaster library does not offer a nice interface for Java applications. We plan to resolve this issue in the future once the Scala API becomes stable.
</p>

<p>
   The following Java code is equivalent to the example presented in <?= mk_link("Scala Code Generation", "docs", "scala", "#apiguide"); ?>:
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
   If this code has been saved to <tt>ExampleApp.java</tt> and the Scala library has been copied to <tt>lib/dbt_scala</tt>, the program can be compiled as follows:
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
