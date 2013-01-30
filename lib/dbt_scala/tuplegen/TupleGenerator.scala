class TupleSourceGenerator(n: Int) {
  val oneToN = (1 to n).toArray
  val className = "Tuple" + n
  val types = oneToN.map("T" + _)
  val typeVars = oneToN.map("+T" + _)
  val args = oneToN.map("_" + _)
  val argsWithTypes = oneToN.map(x => {
    "_" + x + ": T" + x
  })
  val valArgsWithTypes = argsWithTypes.map("val " + _)
  def makeTypeArg(ins: Array[String]) =
    ins.mkString("[", ", ", "]")
  def makeArg(ins: Array[String]) =
    ins.mkString("(", ", ", ")")
  def generate: String = {
    """
    class """ + className + makeTypeArg(typeVars) + makeArg(valArgsWithTypes) + """
      extends TupleN(Array""" + makeArg(args) + """)
  
    object """ + className + """ {
      def apply""" + makeTypeArg(types) + makeArg(argsWithTypes) + """ = 
        new """ + className + makeArg(args) + """
    }
    """
  }
}

object TupleGenerator {
	def main(args: Array[String]){
	  val maxT = args(0).toInt + 1
	  
	  println("package scala {")
	  (23 until maxT).map(i => { println(new TupleSourceGenerator(i).generate) })
	  println(
    """
    abstract class TupleN(val elems: Array[Any]) extends Product {
        override def productArity = elems.length
        override def productElement(n: Int) = elems(n)
        override def toString = elems.mkString("(", ",", ")")
        def canEqual(that: Any) = true
    }
}
	""")
	}
}