import org.dbtoaster.dbtoasterlib.StreamAdaptor._
import java.util.Scanner
import java.io.InputStream
import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._
import scala.collection.mutable.Queue
import scala.collection.mutable.PriorityQueue

package org.dbtoaster.dbtoasterlib {
  object Source {
    abstract class FramingType
    case class FixedSize(len: Int) extends FramingType
    case class Delimited(delim: String) extends FramingType
    case class VarSize(a: Int, b: Int) extends FramingType

    abstract class Source() {
      def init(): Unit
      def hasInput(): Boolean
      def nextInput(): DBTEvent
	  
	  def forEachEvent(f: DBTEvent => Unit): Unit = {
		val event = nextInput()
		event match {
			case StreamEvent(_, _, _, _) => f(event); forEachEvent(f)
			case EndOfStream => ()
		}
	  }
    }

    def createInputStreamSource(in: InputStream, adaptors: List[StreamAdaptor], framingType: FramingType) = {
      framingType match {
        case Delimited(delim) => DelimitedStreamSource(in, adaptors, delim)
        case _ => throw new NotImplementedException("Framing type not supported (yet): " + framingType)
      }
    }

    /**
     * Reads events from an InputStream, assumes that the events are DBTd
     *
     */
    case class DelimitedStreamSource(in: InputStream, adaptors: List[StreamAdaptor], delim: String) extends Source {
      val eventQueue = new Queue[StreamEvent]()
      val scanner: Scanner = new Scanner(in).useDelimiter(delim)

      def init(): Unit = {
      }

      def hasInput(): Boolean = scanner.hasNextLine() || !eventQueue.isEmpty

      def nextInput(): DBTEvent = {
        if (eventQueue.isEmpty && scanner.hasNextLine()) {
          val eventStr: String = scanner.nextLine()
          adaptors.foreach(adaptor => adaptor.processTuple(eventStr).foreach(x => eventQueue.enqueue(x)))
        }

        if (eventQueue.isEmpty)
          EndOfStream
        else
          eventQueue.dequeue()
      }
    }

    /**
     * Multiplexes a list of sources while preserving the orDBT of events (assuming
     * that the sources themselves are ordered)
     *
     */
    class SourceMultiplexer(sources: List[Source]) {
      var counter = 0
      val queue: PriorityQueue[StreamEvent] = PriorityQueue()

      def init(): Unit = ()

      def hasInput(): Boolean = !sources.forall(s => !s.hasInput()) || !queue.isEmpty

      def nextInput(): DBTEvent = {
        // Make sure to get an event from every source and return the one with the lowest
        // order number
        sources.foreach(x => {
          def getEvent(): Unit = {
            if (x.hasInput()) {
              x.nextInput() match {
                case e @ StreamEvent(_, _, _, _) => queue.enqueue(e)
                case EndOfStream => getEvent()
              }
            } else ()
          }
          getEvent()
        })

        if (!queue.isEmpty)
          queue.dequeue()
        else
          EndOfStream
      }
    }
  }
}