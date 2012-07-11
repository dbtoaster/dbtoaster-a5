import java.io.InputStream
import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._
import scala.util.Random
import scala.collection.mutable.Map

package org.dbtoaster.dbtoasterlib {
  object StreamAdaptor {
    var verbose = false;
    
    abstract trait OrderbookType
    case object Asks extends OrderbookType
    case object Bids extends OrderbookType

    abstract trait EventType
    case object InsertTuple extends EventType
    case object DeleteTuple extends EventType

    abstract trait ColumnType
      { def toDouble(col: String) = col.toDouble }
    case object IntColumn extends ColumnType
    case object FloatColumn extends ColumnType
    case object OrderColumn extends ColumnType
    case object DateColumn extends ColumnType
      { override def toDouble(col: String) = col.replace("-", "").toDouble }
    case object HashColumn extends ColumnType
      { override def toDouble(col: String) = col.hashCode().toDouble }
    

    case class StreamEvent(eventType: EventType, order: Int, relation: String, 
                           vals: List[Any])
      extends Ordered[StreamEvent] {
      def compare(other: StreamEvent) = other.order.compareTo(this.order)
    }

    /*
let li_schema = "int,int,int,int,int,float,float,float," + 
                "hash,hash,date,date,date,hash,hash,hash"
let lineitem_params = csv_params "|" li_schema "insert"
let order_params    = csv_params "|" 
                         "int,int,hash,float,date,hash,hash,int,hash" "insert"
let part_params     = csv_params "|" 
                         "int,hash,hash,hash,hash,int,hash,float,hash" "insert"
let partsupp_params = csv_params "|" "int,int,int,float,hash" "insert"
let customer_params = csv_params "|" 
                         "int,hash,hash,int,hash,float,hash,hash" "insert"
let supplier_params = csv_params "|" 
                         "int,hash,hash,int,hash,float,hash" "insert"
let nation_params   = csv_params "|" "int,hash,int,hash" "insert"
let region_params   = csv_params "|" "int,hash,hash" "insert"

     */

    val tpchparams = Map(
      "lineitem" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                         ("schema", 
                          "int,int,int,int,int,float,float,float," +
                          "hash,hash,date,date,date,hash,hash,hash")),
      "customer" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                         ("schema", "int,hash,hash,int,hash,float,hash,hash")),
      "orders" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                       ("schema", 
                        "int,int,hash,float,date,hash,hash,int,hash")),
      "part" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                     ("schema", 
                      "int,hash,hash,hash,hash,int,hash,float,hash")),
      "partsupp" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                         ("schema", "int,int,int,float,hash")),
      "nation" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                       ("schema", "int,hash,int,hash")),
      "region" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                       ("schema", "int,hash,hash")),
      "supplier" -> List(("fields", "\\|"), ("eventtype", "insert"), 
                         ("schema", "int,hash,hash,int,hash,float,hash")))

    def createAdaptor(adaptorType: String, relation: String, 
                      params: List[(String, String)]): StreamAdaptor = {
      if (verbose) println("Create Adaptor: " + adaptorType + ", " + 
                            relation + ", " + params)
      (adaptorType, tpchparams.get(adaptorType)) match {
        case ("csv", _) => createCSVAdaptor(relation, params)
        case ("orderbook", _) => createOrderbookAdaptor(relation, params)
        case (_, Some(x)) => createCSVAdaptor(relation, (x ::: params))
        case _ => throw new IllegalArgumentException(
                        "No adaptor for: " + adaptorType)
      }
    }

    def createOrderbookAdaptor(relation: String, 
                               params: List[(String, String)]): 
                               OrderbookAdaptor = {
      var orderbookType: OrderbookType = Asks
      var brokers: Int = 10
      var deterministic: Boolean = false
      var insertOnly: Boolean = false

      def parseParams(params: List[(String, String)]): 
            OrderbookAdaptor = params match {
        case x :: xs =>
          x match {
            case ("book", b) => orderbookType = if (b == "bids") Bids else Asks
            case ("brokers", n) => brokers = n.toInt
            case ("deterministic", b) => deterministic = (b == "yes")
            case ("insertOnly", b) => insertOnly = (b == "yes")
          }
          parseParams(xs)
        case Nil => new OrderbookAdaptor(relation, orderbookType, brokers,
                                         deterministic, insertOnly)
      }

      parseParams(params)
    }

    def createCSVAdaptor(relation: String, params: List[(String, String)]):
            CSVAdaptor = {
      var eventType: Option[EventType] = None
      var deletions: Boolean = false
      var colTypes: List[ColumnType] = List()
      var delimiter: Option[String] = None

      def parseParams(params: List[(String, String)]): 
            CSVAdaptor = params match {
        case x :: xs => {
          x match {
            case ("fields", d) => delimiter = Some(d)
            case ("schema", s) => {

              def parseColType(col: String): Unit = {
                colTypes = colTypes ::: List(col match {
                  case "int" => IntColumn
                  case "float" => FloatColumn
                  case "order" => OrderColumn
                  case "date" => DateColumn
                  case "hash" => HashColumn
                })
              }

              s.split(",").iterator.foreach(x => parseColType(x))
            }
            case ("eventtype", t) => 
                eventType = Some(if (t == "insert") InsertTuple 
                                 else DeleteTuple)
            case ("deletions", t) => deletions = (t == "true")
          }
          parseParams(xs)
        }
        case Nil => 
            new CSVAdaptor(relation, deletions, colTypes, delimiter.get)
      }

      parseParams(params)
    }

    abstract class StreamAdaptor {
      def processTuple(row: String): List[StreamEvent]
    }

    class CSVAdaptor(relation: String, deletions: Boolean, 
                     colTypes: List[ColumnType], delimiter: String) 
                     extends StreamAdaptor {
      def processTuple(row: String): List[StreamEvent] = {
        // TODO: use fold instead of map and get rid of vars
        val cols = row.split(delimiter).toList
        
        val (valCols, insDel, order) = (if(deletions) {
          (cols.drop(2), (if(cols(1) == "1") InsertTuple 
                          else DeleteTuple), cols(0).toInt)
        } else (cols, InsertTuple, 0))

        val vals = (valCols zip colTypes).map {
          case (col, colType) => colType.toDouble(col)
        }
        List(StreamEvent(insDel, order, relation, vals))
      }
    }

    class OrderbookAdaptor(relation: String, orderbookType: OrderbookType,
                           brokers: Int, deterministic: Boolean, 
                           insertOnly: Boolean) extends StreamAdaptor {
      val asks: Map[Int, OrderbookRow] = Map()
      val bids: Map[Int, OrderbookRow] = Map()

      case class OrderbookRow(t: Int, id: Int, brokerId: Int, 
                              volume: Double, price: Double) {
        def toList: List[Any] = List[Any](t.toDouble, id.toDouble,
                                          brokerId.toDouble, volume, price)
      }

      def processTuple(row: String): List[StreamEvent] = {
        val rows = row.split(",")
        val t = rows(0).toInt
        val id = rows(1).toInt
        val volume = rows(3).toDouble
        val price = rows(4).toDouble

        rows(2) match {
          case "B" if orderbookType == Bids => {
            val brokerId = (if (deterministic) id 
                            else Random.nextInt) % brokers
            val row = OrderbookRow(t, id, brokerId, volume, price)
            bids += ((id, row))
            List(StreamEvent(InsertTuple, t, relation, row.toList))
          }

          case "S" if orderbookType == Asks => {
            val brokerId = (if (deterministic) id 
                            else Random.nextInt) % brokers
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
                      List(StreamEvent(InsertTuple, t, relation, 
                                       newRow.toList))
                    })
                }
                case None => Nil
              }
            }
          }

          case "D" | "F" => {
            bids.get(id) match {
              case Some(x) => 
                { bids -= id; 
                  List(StreamEvent(DeleteTuple, t, relation, x.toList)) }
              case None => asks.get(id) match {
                case Some(x) => 
                { asks -= id; 
                  List(StreamEvent(DeleteTuple, t, relation, x.toList)) }
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