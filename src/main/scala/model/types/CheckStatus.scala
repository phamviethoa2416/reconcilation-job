package model.types

sealed trait CheckStatus {
  def value: String
  override def toString: String = value
}

object CheckStatus {
  case object OK extends CheckStatus {
    val value = "OK"
  }

  case object MISMATCH extends CheckStatus {
    val value = "MISMATCH"
  }

  case object ERROR extends CheckStatus {
    val value = "ERROR"
  }
}