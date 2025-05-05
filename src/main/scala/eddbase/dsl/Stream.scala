package eddbase.dsl

import eddbase.parser.SqlCreateStream
import eddbase.dsl.Helpers.SqlNodeListImplicit

case class Stream(name: String, sourceTable: String, params: Map[String, String])

object Stream {
  def fromSqlStatement(stmt: SqlCreateStream): Stream = {
    assert(stmt.name.isSimple, "stream name must be simple")
    assert(!stmt.sourceTable.isSimple, "source table name must be compound")
    Stream(stmt.name.getSimple, stmt.sourceTable.toString, stmt.params.toKeyValueMap(_.toString))
  }
}
