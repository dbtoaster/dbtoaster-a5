package org.dbtoaster.dbtoasterlib {
  object dbtoasterExceptions {
    final case class K3ToScalaCompilerError(msg: String) extends Error(msg)
    final case class ShouldNotHappenError(msg: String) extends Error(msg)
    final case class NotImplementedException(msg: String) extends RuntimeException(msg)
  }
}