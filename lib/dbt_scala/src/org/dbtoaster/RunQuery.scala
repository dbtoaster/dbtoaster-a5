import org.dbtoaster.dbtoasterlib.K3Collection._
import scala.collection.mutable.Map;
import java.io._
import org.dbtoaster.dbtoasterlib.DBToasterExceptions._
import org.dbtoaster.dbtoasterlib.QueryInterface._

package org.dbtoaster {
  object RunQuery {
    def printProgress(i: Int) {
	  val t = System.nanoTime() / 1000
	  val us = t % 1000000
	  val s = t / 1000000
	  println(i + " tuples processed at " + s + "s+" + us + "us")
	}
  
    def main(args: Array[String]) {
      var counter = 0
	  val logCountIdx = args.indexOf("--log-count")
	  val logCount: Int = 
	    if(logCountIdx >= 0) 
	      args(logCountIdx + 1).toInt
		else
		  -1
		  
      val q = new Query()

      val timeStart = System.nanoTime()
      val msgRcvr = new DBTMessageReceiver {
        def onTupleProcessed(): Unit = {		  
		  if(logCount > 0) {
		    counter += 1
		    if(counter % logCount == 0) {
			  printProgress(counter)
			}
		  }
        }

        def onQueryDone(): Unit = {
	      if(logCount > 0 && counter % logCount != 0)
	        printProgress(counter)
			
	      val runtime = (System.nanoTime() - timeStart) / 1000.0
          println("<runtime>" + runtime + "</runtime>")
          q.printResults()  
        }
      }

      val r = new QuerySupervisor(q, msgRcvr)
	  if(logCount > 0)
	    printProgress(0)
      r.start
    }
  }
}
