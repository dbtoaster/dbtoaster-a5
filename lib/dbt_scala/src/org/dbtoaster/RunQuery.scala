//import org.dbtoaster.Query._
import org.dbtoaster.dbtoasterlib.K3Collection._
import scala.collection.mutable.Map;

package org.dbtoaster {
  object RunQuery {
    def main(args: Array[String]) {
      val timeStart = System.currentTimeMillis()
      Query.run()
      println("Run time: " + (System.currentTimeMillis() - timeStart) + " ms")
      Query.printResults()
    }
  }
}