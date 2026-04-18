package model.types

sealed trait SchemaIssue {
  def value: String
  override def toString: String = value
}

object SchemaIssue {
  case object MISSING_IN_SOURCE extends SchemaIssue {
    val value = "MISSING_IN_SOURCE"
  }

  case object MISSING_IN_SINK extends SchemaIssue {
    val value = "MISSING_IN_SINK"
  }

  case object MISMATCH extends SchemaIssue {
    val value = "MISMATCH"
  }
}
