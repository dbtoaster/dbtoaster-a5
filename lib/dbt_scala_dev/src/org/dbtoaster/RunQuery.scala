import org.dbtoaster.dbtoasterlib.K3Collection._
import org.dbtoaster.dbtoasterlib.StreamAdaptor
import collection.mutable.Map;
import collection.concurrent.TrieMap

package org.dbtoaster {
  object RunQuery {
    def trimStr(s: String): String = {
      if (s.length < 200) s else s.substring(0, 200) + "..."
    }
    
    def main(args: Array[String]) {
      StreamAdaptor.verbose = true;
      val timeStart = System.currentTimeMillis()
      Query3.run()
      println("Query started.")
      var result: Option[K3PersistentCollection[Tuple3[Double,Double,Double], Double]] = None
      do {
        println("Intermediate result: " + trimStr(Query3.intermediateResult.getResult.toString))
        result = Query3.finalResult.get(10)
      } while (!result.isDefined)
      println("Final result: " + trimStr(result.get.toString))
      println("Run time: " + (System.currentTimeMillis() - timeStart) + " ms")
    }
  }
}