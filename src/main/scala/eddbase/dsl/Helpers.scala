package eddbase.dsl

import org.apache.calcite.sql.{SqlCall, SqlLiteral, SqlNode, SqlNodeList}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object Helpers {

  def toUnquotedString(node: SqlNode): String = node match {
    case s: SqlLiteral => s.getValueAs(classOf[String])
    case _ => node.toString
  }

  implicit class SqlNodeImplicit(node: SqlNode) {
    def collect[A](f: PartialFunction[SqlNode, List[A]]): List[A] =
      f.applyOrElse(node, (n: SqlNode) => n match {
        case s: SqlNodeList =>
          s.getList.asScala.flatMap(_.collect(f)).toList
        case s: SqlCall =>
          s.getOperandList.asScala.filter(_ != null).flatMap(_.collect(f)).toList
        case _ => List.empty
      })
  }

  implicit class SqlNodeListImplicit(nodeList: SqlNodeList) {
    def toKeyValueMap(f: SqlNode => String): Map[String, String] = toKeyValueMap(f, f)

    def toKeyValueMap(keyFn: SqlNode => String, valueFn: SqlNode => String): Map[String, String] = {
      def toMap(l: List[SqlNode]): Map[String, String] = l match {
        case key :: value :: rest => toMap(rest) + (keyFn(key) -> valueFn(value))
        case _ => Map.empty[String, String]
      }
      toMap(nodeList.getList.asScala.toList)
    }
  }
}