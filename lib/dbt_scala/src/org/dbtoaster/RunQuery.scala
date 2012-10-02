import org.dbtoaster.dbtoasterlib.K3Collection._
import scala.collection.mutable.Map;
import java.io._
import org.dbtoaster.dbtoasterlib.DBToasterExceptions._
import org.dbtoaster.dbtoasterlib.QueryInterface._

package org.dbtoaster {
  object RunQuery {
    def main(args: Array[String]) {
      var counter = 0
      val trace = args.contains("--trace")
    
      val fw = if(trace) {
        new BufferedWriter(new FileWriter("trace.txt"))
      } else null

      val q = new Query()

      val timeStart = System.currentTimeMillis()
      val msgRcvr = new DBTMessageReceiver {
        def onTupleProcessed(): Unit = {
          if(trace) {
            counter += 1
            if(counter % 1000 == 0) {
              fw.newLine()
              print(".")
            }
          }
        }

        def onQueryDone(): Unit = {
          q.printResults()  
          val runtime = (System.currentTimeMillis() - timeStart) / 1000.0
          if(args.contains("--time-only")) {
            println("" + runtime)
          }
          else {
            println("<runtime>" + runtime + "</runtime>")
          }
        }
      }

      val r = new QuerySupervisor(q, msgRcvr)
      r.start

      if(trace) 
        fw.close();
    }
  }
}
