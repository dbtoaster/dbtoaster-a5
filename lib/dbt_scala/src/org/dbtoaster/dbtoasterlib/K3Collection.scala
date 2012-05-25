import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._

package org.dbtoaster.dbtoasterlib {

  object K3Collection {
    import scala.collection.mutable.Map

    trait K3Collection[K, V] {
      def contains(key: K): Boolean
      def lookup(key: K): V
      def toList(): List[Tuple2[K, V]]
      def map[K2, V2](f: Tuple2[K, V] => Tuple2[K2, V2]): K3IntermediateCollection[K2, V2]
      def foreach(f: Tuple2[K, V] => Unit): Unit
      def slice[KP](keyPart: KP, idx: List[Int]): K3IntermediateCollection[K, V]
      def groupByAggregate[K2, V2](init: V2, group: Tuple2[K, V] => K2, fn: Tuple2[K, V] => V2 => V2): K3IntermediateCollection[K2, V2]
      def fold[Y](init: Y, fn: Tuple2[K, V] => Y => Y): Y
      def flatten[K2, V2](): K3IntermediateCollection[K2, V2]
      def toPersistentCollection(): K3PersistentCollection[K, V]
    }

    trait Index[K, V] {
      def update(keyVal: Tuple2[K, V]): Unit
      def slice[PK](keyPart: PK): Map[K, V]
    }

    case class SimpleVal[T](defval: T) {
      var v: T = defval

      def get(): T = v

      def update(nv: T): Unit = {
        v = nv
      }

      override def toString(): String = v.toString
      def toXML(): String = v.toString
    }

    // KC = type of the complete key
    // K = type of the partial key
    // V = type of the values
    case class SecondaryIndex[PK, K, V](project: K => PK) extends Index[K, V] {
      val index = Map[PK, Map[K, V]]()

      def update(keyVal: Tuple2[K, V]): Unit = {
        val keyPart = project(keyVal._1)

        index.get(keyPart) match {
          case Some(m) => m += keyVal
          case None => index += ((keyPart, Map[K, V](keyVal)))
        }
      }

      def slice[PK2](keyPart: PK2): Map[K, V] = {
        index.get(keyPart.asInstanceOf[PK]) match {
          case Some(x) => x
          case None => Map[K, V]()
        }
      }
    }

    class K3PersistentCollection[K, V](elems: Map[K, V], sndIdx: Option[Map[String, Index[K, V]]]) extends K3Collection[K, V] {
      def map[K2, V2](f: Tuple2[K, V] => Tuple2[K2, V2]): K3IntermediateCollection[K2, V2] = {
        K3IntermediateCollection[K2, V2](elems.toList.map(f))
      }

      def contains(key: K): Boolean =
        elems.contains(key)

      def lookup(key: K): V = elems.get(key) match {
        case None => throw new ShouldNotHappenError("lookup of a non-existant key")
        case Some(v) => v
      }

      def updateValue(key: K, value: V): Unit = {
        value match {
          case _ => {
            val keyVal = (key, value)
            elems += keyVal
            sndIdx match {
              case Some(x) => x foreach { case (k, v) => v.update(keyVal) }
              case None => ()
            }
          }
        }
      }

      def foreach(f: Tuple2[K, V] => Unit): Unit = elems.foreach(f)

      def slice[KP](keyPart: KP, idx: List[Int]): K3IntermediateCollection[K, V] = {
        val strIdx = idx.foldLeft("")({ case (agg, nb) => agg + (if (agg != "") "_" else "") + nb })
        sndIdx match {
          case Some(x) => K3IntermediateCollection(x.get(strIdx).get.slice(keyPart).toList)
          case None => throw new IllegalArgumentException
        }
      }

      def groupByAggregate[K2, V2](init: V2, group: Tuple2[K, V] => K2, fn: Tuple2[K, V] => V2 => V2): K3IntermediateCollection[K2, V2] = {
        val groupedCollection = elems.foldLeft(Map[K2, V2]()) {
          case (grps, keyval) =>
            val key = group(keyval)
            val value = grps.get(key) match {
              case Some(v) => fn(keyval)(v)
              case None => fn(keyval)(init)
            }
            grps += ((key, value))
          case _ => throw new ShouldNotHappenError("Group By Aggregate failed")
        }
        K3IntermediateCollection(groupedCollection.toList)
      }

      def fold[Y](init: Y, fn: Tuple2[K, V] => Y => Y): Y = {
        elems.foldLeft(init) { case (y, kv) => fn(kv)(y) }
      }

      def flatten[K2, V2](): K3IntermediateCollection[K2, V2] =
        throw new K3ToScalaCompilerError("Flatten of a non-nested collection")

      def toList(): List[Tuple2[K, V]] = elems.toList

      override def toString = {
        elems.foldLeft("") {
          case (str, (k, v)) =>
            try {
              str + "<item>" + (k.asInstanceOf[Product].productIterator.foldLeft((0, "")) { case ((i, str), k) => (i + 1, str + "<__a" + i + ">" + k + "</__a" + i + ">") })._2 + "<__av>" + v + "</__av></item>\n"
            } catch {
              case e: java.lang.ClassCastException => str + "<item><__a0>" + k + "</__a0><__av>" + v + "</__av></item>\n"
            }
        }
      }

      def toPersistentCollection(): K3PersistentCollection[K, V] = this

    }

    class K3FullPersistentCollection[K1, K2, V](felems: Map[K1, K3PersistentCollection[K2, V]], fsndIdx: Option[Map[String, Index[K1, K3PersistentCollection[K2, V]]]]) extends K3PersistentCollection[K1, K3PersistentCollection[K2, V]](felems, fsndIdx) {
      def updateValue(inKey: K1, outKey: K2, value: V): Unit = {
        felems.get(inKey) match {
          case Some(outerMap) => outerMap.updateValue(outKey, value)
          case None => felems += ((inKey, new K3PersistentCollection[K2, V](Map((outKey -> value)), None)))
        }
      }
    }

    // Note that intermediate collections can have different values with the same key
    case class K3IntermediateCollection[K, V](elems: List[Tuple2[K, V]]) extends K3Collection[K, V] {
      def map[K2, V2](f: Tuple2[K, V] => Tuple2[K2, V2]): K3IntermediateCollection[K2, V2] =
        K3IntermediateCollection(elems.map(f))

      def contains(key: K): Boolean = {
        (elems.find { case (k, v) => k == key }) != None
      }

      def lookup(key: K): V = {
        (elems.find { case (k, v) => k == key }) match {
          case None => throw new ShouldNotHappenError("lookup of a non-existant key")
          case Some((k, v)) => v
        }
      }

      def foreach(f: Tuple2[K, V] => Unit): Unit =
        elems.foreach(f)

      def slice[K2](keyPart: K2, idx: List[Int]): K3IntermediateCollection[K, V] = {
        val kp = keyPart.asInstanceOf[Product].productIterator.toList
        K3IntermediateCollection(elems.filter { case (k, v) => println(k + "," + kp); (kp zip idx).forall { case (kp, i) => kp == k.asInstanceOf[Product].productElement(i) } })
      }

      def groupByAggregate[K2, V2](init: V2, group: Tuple2[K, V] => K2, fn: Tuple2[K, V] => V2 => V2): K3IntermediateCollection[K2, V2] = {
        val groupedCollection = elems.foldLeft(Map[K2, V2]()) {
          case (grps, keyval) =>
            val key = group(keyval)
            val value = grps.get(key) match {
              case Some(v) => fn(keyval)(v)
              case None => fn(keyval)(init)
            }
            grps += ((key, value))
          case _ => throw new ShouldNotHappenError("Group By Aggregate failed")
        }
        K3IntermediateCollection(groupedCollection.toList)
      }

      def fold[Y](init: Y, fn: Tuple2[K, V] => Y => Y): Y = {
        elems.foldLeft(init) { case (y, kv) => fn(kv)(y) }
      }

      def flatten[K2, V2](): K3IntermediateCollection[K2, V2] = {
        K3IntermediateCollection(elems.foldLeft(List[Tuple2[K2, V2]]()) {
          (agg, elem) =>
            (agg, elem) match {
              case (agg, ((), v)) => agg ::: v.asInstanceOf[K3Collection[K2, V2]].toList
              case _ => throw new IllegalArgumentException(elem.toString)
            }
        })
      }

      def toList(): List[Tuple2[K, V]] = elems

      def toPersistentCollection(): K3PersistentCollection[K, V] =
        new K3PersistentCollection[K, V](Map() ++ elems, None)
    }

  }

}