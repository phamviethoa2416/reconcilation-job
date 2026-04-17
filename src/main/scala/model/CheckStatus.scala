package model

sealed trait CheckStatus

object CheckStatus {
  case object OK extends CheckStatus {
    override def toString = "OK"
  }

  case object MISMATCH extends CheckStatus {
    override def toString = "MISMATCH"
  }

  case object ERROR extends CheckStatus {
    override def toString = "ERROR"
  }
}