/** 
 * This file contains some helper functions and exceptions 
 */

package org.dbtoaster.dbtoasterlib {
  /** This object defines dbtoaster-related exceptions
	*/
  object dbtoasterExceptions {
    final case class DBTScalaCodegenError(msg: String) extends Error(msg)
    final case class DBTFatalError(msg: String) extends Error(msg)
    final case class DBTNotImplementedException(msg: String) extends RuntimeException(msg)
  }
  
  /** This object defines some additional implicit conversions that are being
    * used by queries.
	*/
  object ImplicitConversions {
	implicit def boolToLong(i: Boolean): Long = if(i) 1.toLong else 0.toLong
  }
  
  /** In this object, the standard external functions that can be called by
	* queries are defined.
	*/
  object StdFunctions {
	def div(x: Double): Double = 1.0 / x
	
	def max(v1: Double, v2: Double): Double = if(v1 > v2) v1 else v2
	def min(v1: Double, v2: Double): Double = if(v1 < v2) v1 else v2
  }
}
