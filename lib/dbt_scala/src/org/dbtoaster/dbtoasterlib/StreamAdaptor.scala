import java.io.InputStream
import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._
import scala.util.Random
import scala.collection.mutable.Map

package org.dbtoaster.dbtoasterlib {
  object StreamAdaptor {
    abstract trait OrderbookType
    case object Asks extends OrderbookType
    case object Bids extends OrderbookType

    abstract trait EventType
    case object InsertTuple extends EventType
    case object DeleteTuple extends EventType
	case object SystemInitialized extends EventType

    abstract trait ColumnType
    case object IntColumn extends ColumnType
    case object FloatColumn extends ColumnType
    case object OrderColumn extends ColumnType
    case object DateColumn extends ColumnType
    case object HashColumn extends ColumnType

	abstract trait StreamEventType
    case class StreamEvent(eventType: EventType, order: Int, relation: String, vals: List[Any])
      extends Ordered[StreamEvent] with StreamEventType {
      def compare(other: StreamEvent) = other.order.compareTo(this.order)
    }
	case object EndOfStream

    def createAdaptor(adaptorType: String, relation: String, params: List[(String, String)]): StreamAdaptor = {
      println("Create Adaptor: " + adaptorType + ", " + relation + ", " + params)
      adaptorType match {
        case "orderbook" => createOrderbookAdaptor(relation, params)
        case _ => throw new IllegalArgumentException("No adaptor for: " + adaptorType)
      }
    }

    def createOrderbookAdaptor(relation: String, params: List[(String, String)]): OrderbookAdaptor = {
      var orderbookType: OrderbookType = Asks
      var brokers: Int = 10
      var deterministic: Boolean = false
      var insertOnly: Boolean = false

      def parseParams(params: List[(String, String)]): OrderbookAdaptor = params match {
        case x :: xs =>
          x match {
            case ("book", b) => orderbookType = if (b == "bids") Bids else Asks
            case ("brokers", n) => brokers = n.toInt
            case ("deterministic", b) => deterministic = (b == "yes")
            case ("insertOnly", b) => insertOnly = (b == "yes")
          }
          parseParams(xs)
        case Nil => new OrderbookAdaptor(relation, orderbookType, brokers, deterministic, insertOnly)
      }

      parseParams(params)
    }

    abstract class StreamAdaptor {
      def processTuple(row: String): List[StreamEvent]
    }
	
	class lineitemAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,int,int,int,float,float,float,float,hash,hash,date,date,date,hash,hash,hash") {}
	class customerAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,hash,hash,int,hash,float,hash,hash") {}	
	class ordersAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,int,hash,float,date,hash,hash,int,hash") {}	
	class partAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,hash,hash,hash,hash,int,hash,float,hash") {}	
	class partsuppAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,int,int,float,hash") {}	
	class nationAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,hash,int,hash") {}
	class regionAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,hash,hash") {}
	class supplierAdaptor(relation: String, deletions: String = "false") extends CSVAdaptor(relation, fields = "\\|", eventtype = "insert", 
	  schema = "int,hash,hash,int,hash,float,hash") {}
	
    class CSVAdaptor(relation: String, eventtype: String = "insert", deletions: String = "false", schema: String = "", fields: String = ";") extends StreamAdaptor {
      val eventType = if (eventtype == "insert") InsertTuple else DeleteTuple
	  val hasDeletions = (deletions == "true")
	
	  def parseColType(col: String): ColumnType = {
	    col match {
	      case "int" => IntColumn
	      case "float" => FloatColumn
	      case "order" => OrderColumn
	      case "date" => DateColumn
	      case "hash" => HashColumn
	    }
      }
	  
	  val colTypes: List[ColumnType] = schema.split(",").iterator.toList.map(x => parseColType(x))
	  
      def processTuple(row: String): List[StreamEvent] = {
        val cols = row.split(fields).toList
        
        val (valCols, insDel, order) = (if(hasDeletions) {
          (cols.drop(2), (if(cols(1) == "1") InsertTuple else DeleteTuple), cols(0).toInt)
        } else (cols, eventType, 0))

        val vals:List[Any] = (valCols zip colTypes).map {
          x =>
            x match {
              case (v, IntColumn) => v.toInt
              case (v, FloatColumn) => v.toDouble
              case (v, OrderColumn) => v.toInt
              case (v, DateColumn) => v.replace("-", "").toInt
              case (v, HashColumn) => v //v.hashCode().toInt
			  case _ => ""
            }
        }
        List(StreamEvent(insDel, order, relation, vals))
      }
    }

    class OrderbookAdaptor(relation: String, orderbookType: OrderbookType, brokers: Int, deterministic: Boolean, insertOnly: Boolean) extends StreamAdaptor {
      val asks: Map[Int, OrderbookRow] = Map()
      val bids: Map[Int, OrderbookRow] = Map()

      case class OrderbookRow(t: Int, id: Int, brokerId: Int, volume: Double, price: Double) {
        def toList: List[Any] = List[Any](t.toDouble, id, brokerId, volume, price)
      }

      def processTuple(row: String): List[StreamEvent] = {
        val rows = row.split(",")
        val t = rows(0).toInt
        val id = rows(1).toInt
        val volume = rows(3).toDouble
        val price = rows(4).toDouble

        rows(2) match {
          case "B" if orderbookType == Bids => {
            val brokerId = (if (deterministic) id else Random.nextInt) % brokers
            val row = OrderbookRow(t, id, brokerId, volume, price)
            bids += ((id, row))
            List(StreamEvent(InsertTuple, t, relation, row.toList))
          }

          case "S" if orderbookType == Asks => {
            val brokerId = (if (deterministic) id else Random.nextInt) % brokers
            val row = OrderbookRow(t, id, brokerId, volume, price)
            asks += ((id, row))
            List(StreamEvent(InsertTuple, t, relation, row.toList))
          }

          case "E" => {
            // TODO: Make code more readable/maintainable
            bids.get(id) match {
              case Some(x @ OrderbookRow(t, id, b, v, p)) => {
                val newVolume = v - volume

                List(StreamEvent(DeleteTuple, t, relation, x.toList)) :::
                  (if (newVolume <= 0.0) { bids -= id; Nil }
                  else {
                    val newRow = OrderbookRow(t, id, b, newVolume, p)
                    bids += ((id, newRow))
                    List(StreamEvent(InsertTuple, t, relation, newRow.toList))
                  })
              }
              case None => asks.get(id) match {
                case Some(x @ OrderbookRow(t, id, b, v, p)) => {
                  val newVolume = v - volume

                  List(StreamEvent(DeleteTuple, t, relation, x.toList)) :::
                    (if (newVolume <= 0.0) { asks -= id; Nil }
                    else {
                      val newRow = OrderbookRow(t, id, b, newVolume, p)
                      asks += ((id, newRow))
                      List(StreamEvent(InsertTuple, t, relation, newRow.toList))
                    })
                }
                case None => Nil
              }
            }
          }

          case "D" | "F" => {
            bids.get(id) match {
              case Some(x) => { bids -= id; List(StreamEvent(DeleteTuple, t, relation, x.toList)) }
              case None => asks.get(id) match {
                case Some(x) => { asks -= id; List(StreamEvent(DeleteTuple, t, relation, x.toList)) }
                case None => Nil
              }
            }
          }

          case _ => Nil
        }
      }
    }
  }
}