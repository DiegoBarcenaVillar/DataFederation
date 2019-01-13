package tfm.cluster

object AllowedActions extends Enumeration {

  type AllowedActions = Value
  val Create, CreateSql, Drop, DropSql = Value
}
