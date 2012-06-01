package org.dbtoaster.dbtoasterlib {
  /** This object defines dbtoaster-related exceptions
	*/
  object dbtoasterExceptions {
    final case class K3ToScalaCompilerError(msg: String) extends Error(msg)
    final case class ShouldNotHappenError(msg: String) extends Error(msg)
    final case class NotImplementedException(msg: String) extends RuntimeException(msg)
  }
  
  /** This object defines some additional implicit conversions that are being
    * used by queries.
	*/
  object ImplicitConversions {
	implicit def boolToInt(i: Boolean) = if(i) 1 else 0
  }
  
  /** In this object, the standard external functions that can be called by
	* queries are defined.
	*/
  object StdFunctions {
	def div(x: Double): Double = 1.0 / x
  }
}