
import java.io.FileInputStream;

import org.dbtoaster.dbtoasterlib.StreamAdaptor._;

import org.dbtoaster.dbtoasterlib.K3Collection._;

import org.dbtoaster.dbtoasterlib.Source._;

import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._;

import scala.collection.mutable.Map;

import collection.concurrent.TrieMap

package org.dbtoaster {
  object Query3 extends DBTQuery[K3ResultCollection[
                                    Tuple3[Double,Double,Double], Double]] {
    //implicit def boolToDouble(dbl: Boolean) = if(dbl) 1.0 else 0.0;

    val sources = new SourceMultiplexer(List(createInputStreamSource(new FileInputStream("tpchdata/lineitem_del.csv"), List(createAdaptor("lineitem", "LINEITEM", List(("deletions", "true")))), Delimited("\n")),createInputStreamSource(new FileInputStream("tpchdata/customer_del.csv"), List(createAdaptor("customer", "CUSTOMER", List(("deletions", "true")))), Delimited("\n")),createInputStreamSource(new FileInputStream("tpchdata/orders_del.csv"), List(createAdaptor("orders", "ORDERS", List(("deletions", "true")))), Delimited("\n"))));

    var QUERY_1_1 = new K3ResultCollection[Tuple3[Double,Double,Double], Double](TrieMap(), Some(Map("2" -> SecondaryIndex[(Double),Tuple3[Double,Double,Double], Double](x => x match {
      case Tuple3(x1,x2,x3) => (x3) 
    }
    )))) /* out */;

    var QUERY_1_1_pLINEITEM1 = new K3PersistentCollection[Tuple3[Double,Double,Double], Double](Map(), Some(Map("0" -> SecondaryIndex[(Double),Tuple3[Double,Double,Double], Double](x => x match {
      case Tuple3(x1,x2,x3) => (x1) 
    }
    )))) /* out */;

    var QUERY_1_1_pORDERS2 = new K3PersistentCollection[(Double), Double](Map(), None) /* out */;

    var QUERY_1_1_pCUSTOMER2 = new K3PersistentCollection[Tuple4[Double,Double,Double,Double], Double](Map(), Some(Map("0" -> SecondaryIndex[(Double),Tuple4[Double,Double,Double,Double], Double](x => x match {
      case Tuple4(x1,x2,x3,x4) => (x1) 
    }
    )))) /* out */;

    var QUERY_1_1_pCUSTOMER2_pORDERS1 = new K3PersistentCollection[(Double), Double](Map(), None) /* out */;

    var QUERY_1_1_pCUSTOMER1 = new K3PersistentCollection[Tuple4[Double,Double,Double,Double], Double](Map(), Some(Map("0" -> SecondaryIndex[(Double),Tuple4[Double,Double,Double,Double], Double](x => x match {
      case Tuple4(x1,x2,x3,x4) => (x1) 
    }
    ),"1" -> SecondaryIndex[(Double),Tuple4[Double,Double,Double,Double], Double](x => x match {
      case Tuple4(x1,x2,x3,x4) => (x2) 
    }
    )))) /* out */;

    var QUERY_1_1_pCUSTOMER1_pLINEITEM1 = new K3PersistentCollection[Tuple4[Double,Double,Double,Double], Double](Map(), Some(Map("0" -> SecondaryIndex[(Double),Tuple4[Double,Double,Double,Double], Double](x => x match {
      case Tuple4(x1,x2,x3,x4) => (x1) 
    }
    ),"1" -> SecondaryIndex[(Double),Tuple4[Double,Double,Double,Double], Double](x => x match {
      case Tuple4(x1,x2,x3,x4) => (x2) 
    }
    )))) /* out */;

    var QUERY_1_1_pCUSTOMER1_pORDERS1 = new K3PersistentCollection[(Double), Double](Map(), None) /* out */;

    def dispatcher(event: Option[StreamEvent]): Unit = {   //, onEventProcessedHandler: Unit => Unit): Unit = {
      def onInsertLINEITEM(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT: Double): Unit = {
        (QUERY_1_1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple3(var___t1,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1).contains(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
                  ((x:Double) => {
                    x match {
                      case var___cse1 => {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1).lookup(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var___cse1) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        ))))
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) 
                }
                else {
                  ((x:Double) => {
                    x match {
                      case var___cse1 => {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var___cse1) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        )))
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) 
                }
              }
            }
          }
        };

        (QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var___t8,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var_v1) * (1.))) 
                  }
                  else {
                    0.0 
                  }
                  )) 
                }
                else {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var_v1) * (1.))) 
                  }
                  else {
                    0.0 
                  }
                  ) 
                }
              }
            }
          }
        };

        if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
          QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * (1.)) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * (1.)) 
          }
          else {
            0.0 
          }
          ) 
        };

        (QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var___t14,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_v1) * (1.)) 
                  }
                  else {
                    0.0 
                  }
                  )) 
                }
                else {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_v1) * (1.)) 
                  }
                  else {
                    0.0 
                  }
                  ) 
                }
              }
            }
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
          QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * (1.) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * (1.) 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      def onInsertORDERS(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT: Double): Unit = {
        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse10 => {
                                  ((x:Double) => {
                                    x match {
                                      case var___cse5 => {
                                        QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), (var___cse10) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                          (var___cse48) * ((var___cse5) * (1.)) 
                                        }
                                        else {
                                          0.0 
                                        }
                                        ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                          (var___cse24) * ((var___cse5) * ((1.) * (-1.))) 
                                        }
                                        else {
                                          0.0 
                                        }
                                        )))
                                      }
                                    }
                                  }
                                  ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                }
                              }
                            }
                            ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse10 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse4 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse3 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse2 => {
                                                      ((x:Double) => {
                                                        x match {
                                                          case var___cse1 => {
                                                            (var___cse4) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                              (var___cse3) * ((0.) * (1.)) 
                                                            }
                                                            else {
                                                              0.0 
                                                            }
                                                            ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                              (var___cse2) * ((var___cse1) * ((1.) * (-1.))) 
                                                            }
                                                            else {
                                                              0.0 
                                                            }
                                                            ))
                                                          }
                                                        }
                                                      }
                                                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse24)
                                              }
                                            }
                                          }
                                          ).apply(var___cse48)
                                        }
                                      }
                                    }
                                    ).apply(var___cse10)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse9 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                    (var___cse48) * ((var___cse9) * (1.)) 
                                  }
                                  else {
                                    0.0 
                                  }
                                  ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                    (var___cse24) * ((var___cse9) * ((1.) * (-1.))) 
                                  }
                                  else {
                                    0.0 
                                  }
                                  ))
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse8 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse7 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse6 => {
                                                (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse8) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse7) * ((var___cse6) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                )
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse24)
                                  }
                                }
                              }
                              ).apply(var___cse48)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
          else {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse23 => {
                            ((x:Double) => {
                              x match {
                                case var___cse17 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse13 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse12 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse11 => {
                                                      (var___cse13) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (var___cse12) * ((var___cse11) * (1.)) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (0.) * ((var___cse11) * ((1.) * (-1.))) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse17)
                                              }
                                            }
                                          }
                                          ).apply(var___cse48)
                                        }
                                      }
                                    }
                                    ).apply(var___cse23)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse23 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse16 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse15 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse14 => {
                                                (var___cse16) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse15) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (0.) * ((var___cse14) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ))
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse48)
                                  }
                                }
                              }
                              ).apply(var___cse23)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse22 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse19 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse18 => {
                                          (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (var___cse19) * ((var___cse18) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse18) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          )
                                        }
                                      }
                                    }
                                    ).apply(var___cse22)
                                  }
                                }
                              }
                              ).apply(var___cse48)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse21 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse20 => {
                                    (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (var___cse21) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((var___cse20) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    )
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse48)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse37 => {
                            ((x:Double) => {
                              x match {
                                case var___cse31 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse27 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse26 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse25 => {
                                                      (var___cse27) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (0.) * ((var___cse25) * (1.)) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (var___cse26) * ((var___cse25) * ((1.) * (-1.))) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse31)
                                              }
                                            }
                                          }
                                          ).apply(var___cse47)
                                        }
                                      }
                                    }
                                    ).apply(var___cse37)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse37 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse30 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse29 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse28 => {
                                                (var___cse30) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (0.) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse29) * ((var___cse28) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ))
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse47)
                                  }
                                }
                              }
                              ).apply(var___cse37)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse36 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse33 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse32 => {
                                          (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse32) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (var___cse33) * ((var___cse32) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          )
                                        }
                                      }
                                    }
                                    ).apply(var___cse36)
                                  }
                                }
                              }
                              ).apply(var___cse47)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse35 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse34 => {
                                    (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (var___cse35) * ((var___cse34) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    )
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse47)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
          else {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse46 => {
                      ((x:Double) => {
                        x match {
                          case var___cse42 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse39 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse38 => {
                                          (var___cse39) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse38) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse38) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ))
                                        }
                                      }
                                    }
                                    ).apply(var___cse42)
                                  }
                                }
                              }
                              ).apply(var___cse46)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse46 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse41 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse40 => {
                                    (var___cse41) + ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((var___cse40) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ))
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse46)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse45 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse43 => {
                              (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                (0.) * ((var___cse43) * (1.)) 
                              }
                              else {
                                0.0 
                              }
                              ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                (0.) * ((var___cse43) * ((1.) * (-1.))) 
                              }
                              else {
                                0.0 
                              }
                              )
                            }
                          }
                        }
                        ).apply(var___cse45)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) 
              }
              else {
                QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                  QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                  QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                  QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                  ((x:Double) => {
                    x match {
                      case var___cse44 => {
                        (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                          (0.) * ((0.) * (1.)) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                          (0.) * ((var___cse44) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        )
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                }
                ) 
              }
            }
          }
        };

        if((QUERY_1_1_pLINEITEM1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
          if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), (var___cse2) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) * (1.) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
                    QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                    ((x:Double) => {
                      x match {
                        case var___cse1 => {
                          (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                            (0.) * (1.) 
                          }
                          else {
                            0.0 
                          }
                          )
                        }
                      }
                    }
                    ).apply(var___cse2)
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
            QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
              ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) * (1.) 
            }
            else {
              0.0 
            }
            ) 
          }
          else {
            QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * (1.) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * (1.) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * (1.) 
                  }
                  else {
                    0.0 
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              ((x:Double) => {
                x match {
                  case var___cse1 => {
                    (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                      (0.) * (1.) 
                    }
                    else {
                      0.0 
                    }
                    )
                  }
                }
              }
              ).apply((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0))
            }
            ) 
          }
          else {
            QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * (1.) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * (1.) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * (1.) 
                  }
                  else {
                    0.0 
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              ((x:Double) => {
                x match {
                  case var___cse1 => {
                    (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                      (0.) * (1.) 
                    }
                    else {
                      0.0 
                    }
                    )
                  }
                }
              }
              ).apply((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0))
            }
            ) 
          }
          else {
            QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * (1.) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pLINEITEM1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
          QUERY_1_1_pCUSTOMER1_pLINEITEM1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1_pLINEITEM1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
            1. 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER1_pLINEITEM1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
            1. 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      def onInsertCUSTOMER(var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY: Double,var_QUERY_1_1_pORDERS2CUSTOMER_NAME: Double,var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS: Double,var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY: Double,var_QUERY_1_1_pORDERS2CUSTOMER_PHONE: Double,var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL: Double,var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT: Double,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT: Double): Unit = {
        ((x:K3IntermediateCollection[Tuple3[Double,Double,Double], Double]) => {
          x match {
            case var___cse1 => {
              (var___cse1).foreach {
                (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
                  x match {
                    case Tuple2(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY),var_dv) => {
                      if((QUERY_1_1).contains(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY))) {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), ((QUERY_1_1).lookup(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), 0.0)) + (var_dv)) 
                      }
                      else {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), var_dv) 
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ).apply(QUERY_1_1_pCUSTOMER1.slice((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), List(1)).groupByAggregate(0., {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var_ORDERS__ORDERKEY,var___t19,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                Tuple3((var_SHIPPRIORITY),(var_ORDERDATE),(var_ORDERS__ORDERKEY))
              }
            }
          }
        }
        , {
          (x:Tuple2[(Tuple4[Double,Double,Double,Double]), Double]) => {
            x match {
              case Tuple2(Tuple4(var_ORDERS__ORDERKEY,var___t19,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                (x:Double) => {
                  x match {
                    case var_accv_17 => {
                      if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                        ((if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                          ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) * ((1.) * (-1.)) 
                        }
                        else {
                          0.0 
                        }
                        )) + (var_accv_17) 
                      }
                      else {{
                          QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.);

                          ((if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                            (var_v1) * (1.) 
                          }
                          else {
                            0.0 
                          }
                          ) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                            (0.) * ((1.) * (-1.)) 
                          }
                          else {
                            0.0 
                          }
                          )) + (var_accv_17)
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ));

        ((x:K3IntermediateCollection[Tuple3[Double,Double,Double], Double]) => {
          x match {
            case var___cse1 => {
              (var___cse1).foreach {
                (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
                  x match {
                    case Tuple2(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY),var_dv) => {
                      if((QUERY_1_1_pLINEITEM1).contains(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                        QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (var_dv)) 
                      }
                      else {
                        QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), var_dv) 
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ).apply(QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), List(1)).groupByAggregate(0., {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var___t23,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                Tuple3((var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY),(var_ORDERDATE),(var_SHIPPRIORITY))
              }
            }
          }
        }
        , {
          (x:Tuple2[(Tuple4[Double,Double,Double,Double]), Double]) => {
            x match {
              case Tuple2(Tuple4(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var___t23,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                (x:Double) => {
                  x match {
                    case var_accv_19 => {
                      (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                        (var_v1) * (1.) 
                      }
                      else {
                        0.0 
                      }
                      ) + (var_accv_19)
                    }
                  }
                }
              }
            }
          }
        }
        ));

        if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY))) {
          QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), 0.0)) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
            1. 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
            1. 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      def onDeleteLINEITEM(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE: Double,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT: Double): Unit = {
        (QUERY_1_1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple3(var___t26,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1).contains(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
                  ((x:Double) => {
                    x match {
                      case var___cse1 => {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1).lookup(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((-1.) * ((if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var___cse1) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        )))))
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) 
                }
                else {
                  ((x:Double) => {
                    x match {
                      case var___cse1 => {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((-1.) * ((if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                          (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var___cse1) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        ))))
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) 
                }
              }
            }
          }
        };

        (QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var___t34,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var_v1) * ((1.) * (-1.)))) 
                  }
                  else {
                    0.0 
                  }
                  )) 
                }
                else {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((var_v1) * ((1.) * (-1.)))) 
                  }
                  else {
                    0.0 
                  }
                  ) 
                }
              }
            }
          }
        };

        if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
          QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((1.) * (-1.))) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT) * ((1.) * (-1.))) 
          }
          else {
            0.0 
          }
          ) 
        };

        (QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), List(0))).foreach {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var___t40,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_v1) * ((1.) * (-1.))) 
                  }
                  else {
                    0.0 
                  }
                  )) 
                }
                else {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
                    (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((var_v1) * ((1.) * (-1.))) 
                  }
                  else {
                    0.0 
                  }
                  ) 
                }
              }
            }
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY))) {
          QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), ((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), 0.0)) + (if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((1.) * (-1.)) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY), if((19950315.) < (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE)) {
            (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE) * ((1.) * (-1.)) 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      def onDeleteORDERS(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY: Double,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT: Double): Unit = {
        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse10 => {
                                  ((x:Double) => {
                                    x match {
                                      case var___cse5 => {
                                        QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), (var___cse10) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                          (var___cse48) * ((var___cse5) * (1.)) 
                                        }
                                        else {
                                          0.0 
                                        }
                                        ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                          (var___cse24) * ((var___cse5) * ((1.) * (-1.))) 
                                        }
                                        else {
                                          0.0 
                                        }
                                        ))))
                                      }
                                    }
                                  }
                                  ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                }
                              }
                            }
                            ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse10 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse4 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse3 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse2 => {
                                                      ((x:Double) => {
                                                        x match {
                                                          case var___cse1 => {
                                                            (var___cse4) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                              (var___cse3) * ((0.) * (1.)) 
                                                            }
                                                            else {
                                                              0.0 
                                                            }
                                                            ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                              (var___cse2) * ((var___cse1) * ((1.) * (-1.))) 
                                                            }
                                                            else {
                                                              0.0 
                                                            }
                                                            )))
                                                          }
                                                        }
                                                      }
                                                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse24)
                                              }
                                            }
                                          }
                                          ).apply(var___cse48)
                                        }
                                      }
                                    }
                                    ).apply(var___cse10)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            ((x:Double) => {
                              x match {
                                case var___cse9 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                    (var___cse48) * ((var___cse9) * (1.)) 
                                  }
                                  else {
                                    0.0 
                                  }
                                  ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                    (var___cse24) * ((var___cse9) * ((1.) * (-1.))) 
                                  }
                                  else {
                                    0.0 
                                  }
                                  )))
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse24 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse8 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse7 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse6 => {
                                                (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse8) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse7) * ((var___cse6) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ))
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse24)
                                  }
                                }
                              }
                              ).apply(var___cse48)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
          else {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse23 => {
                            ((x:Double) => {
                              x match {
                                case var___cse17 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse13 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse12 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse11 => {
                                                      (var___cse13) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (var___cse12) * ((var___cse11) * (1.)) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (0.) * ((var___cse11) * ((1.) * (-1.))) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      )))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse17)
                                              }
                                            }
                                          }
                                          ).apply(var___cse48)
                                        }
                                      }
                                    }
                                    ).apply(var___cse23)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse23 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse16 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse15 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse14 => {
                                                (var___cse16) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse15) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (0.) * ((var___cse14) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                )))
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse48)
                                  }
                                }
                              }
                              ).apply(var___cse23)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      ((x:Double) => {
                        x match {
                          case var___cse22 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse19 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse18 => {
                                          (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (var___cse19) * ((var___cse18) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse18) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ))
                                        }
                                      }
                                    }
                                    ).apply(var___cse22)
                                  }
                                }
                              }
                              ).apply(var___cse48)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse48 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse21 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse20 => {
                                    (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (var___cse21) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((var___cse20) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ))
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse48)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse37 => {
                            ((x:Double) => {
                              x match {
                                case var___cse31 => {
                                  QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                                    QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                                    ((x:Double) => {
                                      x match {
                                        case var___cse27 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse26 => {
                                                ((x:Double) => {
                                                  x match {
                                                    case var___cse25 => {
                                                      (var___cse27) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (0.) * ((var___cse25) * (1.)) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                        (var___cse26) * ((var___cse25) * ((1.) * (-1.))) 
                                                      }
                                                      else {
                                                        0.0 
                                                      }
                                                      )))
                                                    }
                                                  }
                                                }
                                                ).apply(var___cse31)
                                              }
                                            }
                                          }
                                          ).apply(var___cse47)
                                        }
                                      }
                                    }
                                    ).apply(var___cse37)
                                  }
                                  )
                                }
                              }
                            }
                            ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse37 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse30 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse29 => {
                                          ((x:Double) => {
                                            x match {
                                              case var___cse28 => {
                                                (var___cse30) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (0.) * ((0.) * (1.)) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                                  (var___cse29) * ((var___cse28) * ((1.) * (-1.))) 
                                                }
                                                else {
                                                  0.0 
                                                }
                                                )))
                                              }
                                            }
                                          }
                                          ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                                        }
                                      }
                                    }
                                    ).apply(var___cse47)
                                  }
                                }
                              }
                              ).apply(var___cse37)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      ((x:Double) => {
                        x match {
                          case var___cse36 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse33 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse32 => {
                                          (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse32) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (var___cse33) * ((var___cse32) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ))
                                        }
                                      }
                                    }
                                    ).apply(var___cse36)
                                  }
                                }
                              }
                              ).apply(var___cse47)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse47 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse35 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse34 => {
                                    (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (var___cse35) * ((var___cse34) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ))
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse47)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
          }
          else {
            if((QUERY_1_1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse46 => {
                      ((x:Double) => {
                        x match {
                          case var___cse42 => {
                            QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                              ((x:Double) => {
                                x match {
                                  case var___cse39 => {
                                    ((x:Double) => {
                                      x match {
                                        case var___cse38 => {
                                          (var___cse39) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse38) * (1.)) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                            (0.) * ((var___cse38) * ((1.) * (-1.))) 
                                          }
                                          else {
                                            0.0 
                                          }
                                          )))
                                        }
                                      }
                                    }
                                    ).apply(var___cse42)
                                  }
                                }
                              }
                              ).apply(var___cse46)
                            }
                            )
                          }
                        }
                      }
                      ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                    }
                  }
                }
                ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
              else {
                ((x:Double) => {
                  x match {
                    case var___cse46 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse41 => {
                              ((x:Double) => {
                                x match {
                                  case var___cse40 => {
                                    (var___cse41) + ((-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((0.) * (1.)) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                      (0.) * ((var___cse40) * ((1.) * (-1.))) 
                                    }
                                    else {
                                      0.0 
                                    }
                                    )))
                                  }
                                }
                              }
                              ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                            }
                          }
                        }
                        ).apply(var___cse46)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
              }
            }
            else {
              if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
                ((x:Double) => {
                  x match {
                    case var___cse45 => {
                      QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                        QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                        ((x:Double) => {
                          x match {
                            case var___cse43 => {
                              (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                (0.) * ((var___cse43) * (1.)) 
                              }
                              else {
                                0.0 
                              }
                              ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                                (0.) * ((var___cse43) * ((1.) * (-1.))) 
                              }
                              else {
                                0.0 
                              }
                              ))
                            }
                          }
                        }
                        ).apply(var___cse45)
                      }
                      )
                    }
                  }
                }
                ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) 
              }
              else {
                QUERY_1_1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), {
                  QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                  QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                  QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

                  ((x:Double) => {
                    x match {
                      case var___cse44 => {
                        (-1.) * ((if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                          (0.) * ((0.) * (1.)) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                          (0.) * ((var___cse44) * ((1.) * (-1.))) 
                        }
                        else {
                          0.0 
                        }
                        ))
                      }
                    }
                  }
                  ).apply((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0))
                }
                ) 
              }
            }
          }
        };

        if((QUERY_1_1_pLINEITEM1).contains(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
          if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), (var___cse2) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) * ((1.) * (-1.)) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
                    QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

                    ((x:Double) => {
                      x match {
                        case var___cse1 => {
                          (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                            (0.) * ((1.) * (-1.)) 
                          }
                          else {
                            0.0 
                          }
                          )
                        }
                      }
                    }
                    ).apply(var___cse2)
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY))) {
            QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
              ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.0)) * ((1.) * (-1.)) 
            }
            else {
              0.0 
            }
            ) 
          }
          else {
            QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * ((1.) * (-1.)) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER2_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * ((1.) * (-1.)) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * ((1.) * (-1.)) 
                  }
                  else {
                    0.0 
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER2_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              ((x:Double) => {
                x match {
                  case var___cse1 => {
                    (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                      (0.) * ((1.) * (-1.)) 
                    }
                    else {
                      0.0 
                    }
                    )
                  }
                }
              }
              ).apply((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0))
            }
            ) 
          }
          else {
            QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER2_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * ((1.) * (-1.)) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pORDERS1).contains((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY))) {
          if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * ((1.) * (-1.)) 
                  }
                  else {
                    0.0 
                  }
                  ))
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
          else {
            ((x:Double) => {
              x match {
                case var___cse2 => {
                  QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                    (var___cse2) * ((1.) * (-1.)) 
                  }
                  else {
                    0.0 
                  }
                  )
                }
              }
            }
            ).apply((QUERY_1_1_pCUSTOMER1_pORDERS1).lookup((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.0)) 
          }
        }
        else {
          if((QUERY_1_1_pCUSTOMER1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
            QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              ((x:Double) => {
                x match {
                  case var___cse1 => {
                    (var___cse1) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                      (0.) * ((1.) * (-1.)) 
                    }
                    else {
                      0.0 
                    }
                    )
                  }
                }
              }
              ).apply((QUERY_1_1_pCUSTOMER1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0))
            }
            ) 
          }
          else {
            QUERY_1_1_pCUSTOMER1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), {
              QUERY_1_1_pCUSTOMER1_pORDERS1.updateValue((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY), 0.);

              if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
                (0.) * ((1.) * (-1.)) 
              }
              else {
                0.0 
              }
            }
            ) 
          }
        };

        if((QUERY_1_1_pCUSTOMER1_pLINEITEM1).contains(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY))) {
          QUERY_1_1_pCUSTOMER1_pLINEITEM1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), ((QUERY_1_1_pCUSTOMER1_pLINEITEM1).lookup(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), 0.0)) + (if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
            (1.) * (-1.) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pCUSTOMER1_pLINEITEM1.updateValue(Tuple4(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY), if((var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE) < (19950315.)) {
            (1.) * (-1.) 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      def onDeleteCUSTOMER(var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY: Double,var_QUERY_1_1_pORDERS2CUSTOMER_NAME: Double,var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS: Double,var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY: Double,var_QUERY_1_1_pORDERS2CUSTOMER_PHONE: Double,var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL: Double,var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT: Double,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT: Double): Unit = {
        ((x:K3IntermediateCollection[Tuple3[Double,Double,Double], Double]) => {
          x match {
            case var___cse1 => {
              (var___cse1).foreach {
                (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
                  x match {
                    case Tuple2(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY),var_dv) => {
                      if((QUERY_1_1).contains(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY))) {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), ((QUERY_1_1).lookup(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), 0.0)) + (var_dv)) 
                      }
                      else {
                        QUERY_1_1.updateValue(Tuple3(var_SHIPPRIORITY,var_ORDERDATE,var_ORDERS__ORDERKEY), var_dv) 
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ).apply(QUERY_1_1_pCUSTOMER1.slice((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), List(1)).groupByAggregate(0., {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var_ORDERS__ORDERKEY,var___t45,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                Tuple3((var_SHIPPRIORITY),(var_ORDERDATE),(var_ORDERS__ORDERKEY))
              }
            }
          }
        }
        , {
          (x:Tuple2[(Tuple4[Double,Double,Double,Double]), Double]) => {
            x match {
              case Tuple2(Tuple4(var_ORDERS__ORDERKEY,var___t45,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                (x:Double) => {
                  x match {
                    case var_accv_36 => {
                      if((QUERY_1_1_pCUSTOMER2).contains(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                        ((-1.) * ((if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                          (var_v1) * (1.) 
                        }
                        else {
                          0.0 
                        }
                        ) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                          ((QUERY_1_1_pCUSTOMER2).lookup(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) * ((1.) * (-1.)) 
                        }
                        else {
                          0.0 
                        }
                        ))) + (var_accv_36) 
                      }
                      else {{
                          QUERY_1_1_pCUSTOMER2.updateValue(Tuple4(var_ORDERS__ORDERKEY,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.);

                          ((-1.) * ((if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                            (var_v1) * (1.) 
                          }
                          else {
                            0.0 
                          }
                          ) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                            (0.) * ((1.) * (-1.)) 
                          }
                          else {
                            0.0 
                          }
                          ))) + (var_accv_36)
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ));

        ((x:K3IntermediateCollection[Tuple3[Double,Double,Double], Double]) => {
          x match {
            case var___cse1 => {
              (var___cse1).foreach {
                (x:Tuple2[Tuple3[Double,Double,Double], Double]) => {
                  x match {
                    case Tuple2(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY),var_dv) => {
                      if((QUERY_1_1_pLINEITEM1).contains(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY))) {
                        QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), ((QUERY_1_1_pLINEITEM1).lookup(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), 0.0)) + (var_dv)) 
                      }
                      else {
                        QUERY_1_1_pLINEITEM1.updateValue(Tuple3(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var_ORDERDATE,var_SHIPPRIORITY), var_dv) 
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ).apply(QUERY_1_1_pCUSTOMER1_pLINEITEM1.slice((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), List(1)).groupByAggregate(0., {
          (x:Tuple2[Tuple4[Double,Double,Double,Double], Double]) => {
            x match {
              case Tuple2(Tuple4(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var___t50,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                Tuple3((var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY),(var_ORDERDATE),(var_SHIPPRIORITY))
              }
            }
          }
        }
        , {
          (x:Tuple2[(Tuple4[Double,Double,Double,Double]), Double]) => {
            x match {
              case Tuple2(Tuple4(var_QUERY_1_1LINEITEM_LINEITEM__ORDERKEY,var___t50,var_ORDERDATE,var_SHIPPRIORITY),var_v1) => {
                (x:Double) => {
                  x match {
                    case var_accv_38 => {
                      (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
                        (var_v1) * ((1.) * (-1.)) 
                      }
                      else {
                        0.0 
                      }
                      ) + (var_accv_38)
                    }
                  }
                }
              }
            }
          }
        }
        ));

        if((QUERY_1_1_pORDERS2).contains((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY))) {
          QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), ((QUERY_1_1_pORDERS2).lookup((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), 0.0)) + (if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
            (1.) * (-1.) 
          }
          else {
            0.0 
          }
          )) 
        }
        else {
          QUERY_1_1_pORDERS2.updateValue((var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY), if((var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT) == ("BUILDING".hashCode().toDouble)) {
            (1.) * (-1.) 
          }
          else {
            0.0 
          }
          ) 
        }
      };

      event match {
        case Some(StreamEvent(InsertTuple, o, "LINEITEM", (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT: Double) :: Nil)) => onInsertLINEITEM(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT);

        case Some(StreamEvent(InsertTuple, o, "ORDERS", (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT: Double) :: Nil)) => onInsertORDERS(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT);

        case Some(StreamEvent(InsertTuple, o, "CUSTOMER", (var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_NAME: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_PHONE: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT: Double) :: Nil)) => onInsertCUSTOMER(var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_QUERY_1_1_pORDERS2CUSTOMER_NAME,var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS,var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY,var_QUERY_1_1_pORDERS2CUSTOMER_PHONE,var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL,var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT);

        case Some(StreamEvent(DeleteTuple, o, "LINEITEM", (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT: Double) :: Nil)) => onDeleteLINEITEM(var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_PARTKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SUPPKEY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINENUMBER,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_QUANTITY,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_EXTENDEDPRICE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_DISCOUNT,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_TAX,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RETURNFLAG,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINESTATUS,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_COMMITDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_RECEIPTDATE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPINSTRUCT,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_SHIPMODE,var_QUERY_1_1_pCUSTOMER1_pORDERS1LINEITEM_LINEITEM__COMMENT);

        case Some(StreamEvent(DeleteTuple, o, "ORDERS", (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY: Double) :: (var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT: Double) :: Nil)) => onDeleteORDERS(var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__ORDERKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__CUSTKEY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERSTATUS,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_TOTALPRICE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERDATE,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_CLERK,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_SHIPPRIORITY,var_QUERY_1_1_pCUSTOMER1_pLINEITEM1ORDERS_ORDERS__COMMENT);

        case Some(StreamEvent(DeleteTuple, o, "CUSTOMER", (var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_NAME: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_PHONE: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT: Double) :: (var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT: Double) :: Nil)) => onDeleteCUSTOMER(var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__CUSTKEY,var_QUERY_1_1_pORDERS2CUSTOMER_NAME,var_QUERY_1_1_pORDERS2CUSTOMER_ADDRESS,var_QUERY_1_1_pORDERS2CUSTOMER_NATIONKEY,var_QUERY_1_1_pORDERS2CUSTOMER_PHONE,var_QUERY_1_1_pORDERS2CUSTOMER_ACCTBAL,var_QUERY_1_1_pORDERS2CUSTOMER_MKTSEGMENT,var_QUERY_1_1_pORDERS2CUSTOMER_CUSTOMER__COMMENT);

        case None => ();

        case _ => throw ShouldNotHappenError("Event could not be dispatched: " + event)
      }
      //onEventProcessedHandler();

      //if(sources.hasInput()) dispatcher(sources.nextInput(), onEventProcessedHandler)
    }
    
    def getResult = QUERY_1_1
    /*
    def run(onEventProcessedHandler: Unit => Unit = (_ => ())): Unit = {
      dispatcher(sources.nextInput(), onEventProcessedHandler) 
    }
    def printResults(): Unit = {
      println("<QUERY_1_1>\n");

      println(QUERY_1_1);

      println("\n</QUERY_1_1>\n");
;

    }*/
  }
}
