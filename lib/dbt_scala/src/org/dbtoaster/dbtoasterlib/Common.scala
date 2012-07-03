/** 
 * This file contains some helper functions and exceptions 
 */

package org.dbtoaster.dbtoasterlib {
  /** This object defines dbtoaster-related exceptions
    */
  object dbtoasterExceptions {
    final case class DBTScalaCodegenError(msg: String) extends Error(msg)
    final case class DBTFatalError(msg: String) extends Error(msg)
    final case class DBTNotImplementedException(msg: String) 
      extends RuntimeException(msg)
  }
  
  /** This object defines some additional implicit conversions that are being
    * used by queries.
    */
  object ImplicitConversions {
    implicit def boolToLong(b: Boolean): Long = if(b) 1.toLong else 0.toLong 
  }
  
  /** In this object, the standard external functions that can be called by
    * queries are defined.
    */
  object StdFunctions {

    def div(x: Double): Double = 1.0 / x
    
    def max(v1: Double, v2: Double): Double = if(v1 > v2) v1 else v2
    def min(v1: Double, v2: Double): Double = if(v1 < v2) v1 else v2
    
    /* This is some sort of a hack */
    def listmax(v1: Long, v2: Long): Double = max (v1, v2)
    def listmax(v1: Double, v2: Long): Double = max (v1, v2)
    def listmax(v1: Long, v2: Double): Double = max (v1, v2)
    def listmax(v1: Double, v2: Double): Double = max (v1, v2)
        
    def year_part(date: java.util.Date): Long = {
      val c = java.util.Calendar.getInstance
      c.setTime(date)   
      c.get(java.util.Calendar.YEAR) 
    }

    def month_part(date: java.util.Date): Long = {
      val c = java.util.Calendar.getInstance
      c.setTime(date)   
      c.get(java.util.Calendar.MONTH) 
    }
    
    def day_part(date: java.util.Date): Long = {
      val c = java.util.Calendar.getInstance
      c.setTime(date)   
      c.get(java.util.Calendar.DAY_OF_MONTH) 
    }    
    
    def date_part(field: String, date: java.util.Date): Long = {
      val c = java.util.Calendar.getInstance
      c.setTime(date)
      field.toUpperCase() match {
        case "YEAR"  => c.get(java.util.Calendar.YEAR)
        case "MONTH" => c.get(java.util.Calendar.MONTH)
        case "DAY"   => c.get(java.util.Calendar.DAY_OF_MONTH)
        case _ => throw new dbtoasterExceptions.DBTFatalError(
                                "Invalid date part.")
      }
    }
    
    def regexp_match(regexp: String, str: String): Int = {
      val pattern = java.util.regex.Pattern.compile(regexp)
      val matcher = pattern.matcher(str)
      if (matcher.find) 1 else 0
    }
    
    def substring(str: String, start: Long, length: Long): String =
      str.substring (start.toInt, (start + length).toInt)

    /* Type conversion functions */
    def cast_int(l: Long): Long = l
    def cast_int(d: Double): Long = d.toInt
    def cast_int(s: String): Long = s.toLong
    
    def cast_float(l: Long): Double = l.toDouble
    def cast_float(d: Double): Double = d
    def cast_float(s: String): Double = s.toDouble
    
    def cast_string(a: Any): String = a.toString
    def cast_string(d: java.util.Date): String = {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
        dateFormat.format(d)
    }

    def cast_date(d: java.util.Date): java.util.Date = d
    def cast_date (s: String): java.util.Date = {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
        dateFormat.parse(s)
    }
  }
}
