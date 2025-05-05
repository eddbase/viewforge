package eddbase.dsl

import eddbase.parser.SqlCreateView
import eddbase.dsl.Helpers.SqlNodeListImplicit

case class View(name: String, params: Map[String, String], query: String)

object View {
  def fromSqlStatement(stmt: SqlCreateView): View = {
    assert(stmt.name.isSimple, "view name must be simple")
    View(stmt.name.getSimple, stmt.params.toKeyValueMap(_.toString), stmt.query.toString)
  }
}
