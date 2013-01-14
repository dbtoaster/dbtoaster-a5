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

		class DBTTimer(name: String) {
			var t = 0L

			def update(dt: Long): Unit = {
				t += dt
			}

			def print(): Unit = {
				println(name + ": " + t / 1000000000.0)
			}
		}

		def time[T](t: DBTTimer)(f: => T): T = {
			val start = System.nanoTime()
			val r = f
			t.update(System.nanoTime() - start)
			r
		}
	}
}