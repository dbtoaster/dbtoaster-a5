import org.dbtoaster.dbtoasterlib.StreamAdaptor._;
import org.dbtoaster.dbtoasterlib.K3Collection._;
import org.dbtoaster.dbtoasterlib.Source._;
import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._;

import scala.actors.Actor
import scala.actors.TIMEOUT
import scala.actors.Actor._

package org.dbtoaster {

abstract class DBTQuery[T] {
  implicit def boolToDouble(dbl: Boolean) = if(dbl) 1.0 else 0.0;
  protected def sources: SourceMultiplexer
  protected def getResult: T
  
  protected def dispatcher(event: Option[StreamEvent])
  
  val finalResult = new concurrent.SyncVar[T]
  
  abstract trait DBTMessage
  case class Tuple(event: Option[StreamEvent]) extends DBTMessage
  case object EndOfData extends DBTMessage
  case object RequestResult extends DBTMessage
    
  private val dispatchActor: Actor = actor {
    var run = true;
    // assures mutual exclusion of read and write accesses to the query result
    // makes sure that result requests have priority over tuple events
    while(run) {
      receiveWithin(0) {
        case RequestResult => reply(getResult)
        case TIMEOUT =>
          receive {
            case Tuple(event) => dispatcher(event)
            case EndOfData => finalResult.set(getResult); run = false
            case RequestResult => reply(getResult)
          }
      }
    }
  }
  
  def intermediateResult: T = (dispatchActor !? RequestResult).asInstanceOf[T]
    
  def run(): Unit = {
    while (sources.hasInput()) dispatchActor ! Tuple(sources.nextInput())
    dispatchActor ! EndOfData
  }

  def printResults(): Unit = {
    println("<QUERY_1_1>\n");
    println(finalResult.get);
    println("\n</QUERY_1_1>\n");
  }
}

}