import scala.actors.Actor
import scala.actors.Actor._

package org.dbtoaster.dbtoasterlib {
	object QueryInterface {
		trait DBTMessageReceiver {
			def onTupleProcessed(): Unit
			def onQueryDone(): Unit
		}

		case object DBTTupleProcessed
		case object DBTDone

		class QuerySupervisor(query: DBTQuery, msgRcv: DBTMessageReceiver) extends Actor {
			def act() {
      			query.setSupervisor(this)
				query.start
			
				while(true) {
					receive {
						case DBTDone => { 
							msgRcv.onQueryDone()
							exit()
						}
						case DBTTupleProcessed => 
						{
							msgRcv.onTupleProcessed()
						}
					}
				}
			}
		}

		trait DBTQuery extends Actor {
			def setSupervisor(supervisor: Actor): Unit
		}
	}
}