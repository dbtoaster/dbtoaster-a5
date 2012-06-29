import org.dbtoaster.dbtoasterlib.StreamAdaptor._
import java.util.Scanner
import java.io.InputStream
import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._
import scala.collection.mutable.Queue
import scala.collection.mutable.PriorityQueue

package org.dbtoaster.dbtoasterlib {
  /**
   * This object contains the implementation for sources which emit tuples
   *
   */
  object Source {
    /**
     * Description of which framing type is being used
     *
     */
    abstract class FramingType
    case class FixedSize(len: Int) extends FramingType
    case class Delimited(delim: String) extends FramingType

    /**
     * Abstract definition of a Source
     */
    abstract class Source() {
      /**
       * Initializes the source
       */
      def init(): Unit

      /**
       * Checks whether a source has more input
       * @return True if there is more input, false otherwise
       */
      def hasInput(): Boolean

      /**
       * Returns the next event of this source
       * @return The next event
       */
      def nextInput(): DBTEvent

      /**
       * Do something for all events of this source
       *
       * Note: this is used to fill static tables
       *
       * @param f The function that should be applied to all events
       */
      def forEachEvent(f: DBTEvent => Unit): Unit = {
        val event = nextInput()
        event match {
          case StreamEvent(_, _, _, _) => f(event); forEachEvent(f)
          case EndOfStream => ()
        }
      }
    }

    /**
     * Returns a source from an input stream
     *
     * @param in The input stream
     * @param adaptors The adaptors that use this source
     * @param framingType The framing type that should be used
     * @return The source
     */
    def createInputStreamSource(in: InputStream, adaptors: List[StreamAdaptor], framingType: FramingType) = {
      framingType match {
        case Delimited(delim) => DelimitedStreamSource(in, adaptors, delim)
        case _ => throw new DBTNotImplementedException("Framing type not supported (yet): " + framingType)
      }
    }

    /**
     * Reads events from an InputStream, assumes that the events are ordered
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