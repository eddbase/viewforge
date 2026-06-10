package eddbase.pipeline

import org.apache.calcite.sql._
import org.apache.calcite.sql.util.SqlShuttle

import scala.collection.mutable.LinkedHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

object Helpers {
  def toUnquotedString(node: SqlNode): String = node match {
    case s: SqlLiteral => s.getValueAs(classOf[String])
    case _ => node.toString
  }

  implicit class SqlIdentifierImplicit(node: SqlIdentifier) {

    def getQualifiedName: QualifiedTableName =
      if (node.isSimple) getSimpleTableName else getCompoundTableName
    def getSimpleTableName: QualifiedTableName = {
      require(!node.isStar, "Identifier cannot be a star")
      SimpleTableName(node.names.asScala.toList)
    }

    def getCompoundTableName: QualifiedTableName = {
      require(!node.isStar, "Identifier cannot be a star")
      CompoundTableName(node.names.asScala.toList)
    }
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

    def extractTableNames(fromOrJoin: Boolean = false): Set[SqlIdentifier] = node match {
      case n: SqlNodeList =>
        n.getList.asScala.flatMap(_.extractTableNames(fromOrJoin)).toSet
      case n: SqlSelect =>
        val tablesInFrom = n.getFrom.extractTableNames(true)
        val tablesInSelectList = n.getSelectList.extractTableNames(false)
        val tablesInWhere = n.getWhere.extractTableNames(false)
        val tablesInHaving = n.getHaving.extractTableNames(false)
        tablesInFrom ++ tablesInSelectList ++ tablesInWhere ++ tablesInHaving
      case n: SqlJoin =>
        val left = n.getLeft.extractTableNames(true)
        val right = n.getRight.extractTableNames(true)
        left ++ right
      case n: SqlCall if n.getKind == SqlKind.AS && n.operandCount() > 1 =>
        n.operand(0).asInstanceOf[SqlNode].extractTableNames(true)
      case n: SqlCall =>
        n.getOperandList.asScala.flatMap(_.extractTableNames(false)).toSet
      case n: SqlIdentifier if fromOrJoin => Set(n)
      case _ => Set.empty
    }

    def rewriteQuery(f: Map[SqlIdentifier, String]): SqlNode = {
      val renamer = new SqlShuttle {
        override def visit(id: SqlIdentifier): SqlNode =
          if (f.contains(id))
            new SqlIdentifier(f(id), id.getParserPosition)
          else
            super.visit(id)
      }
      node.accept(renamer)
    }
  }

  implicit class SqlNodeListImplicit(nodeList: SqlNodeList) {
    def toKeyValueMap(f: SqlNode => String): Map[String, String] = toKeyValueMap(f, f)

    def toKeyValueMap(keyFn: SqlNode => String, valueFn: SqlNode => String): Map[String, String] = {
      val builder = LinkedHashMap.newBuilder[String, String]
      val params = nodeList.getList.asScala.toList
      // Iterate through the list of nodes in pairs (key, value)
      for (i <- 0 until params.length by 2) {
        builder += (keyFn(params(i)) -> valueFn(params(i + 1)))
      }
      builder.result().toMap  // Returns an immutable Map that preserves insertion order
    }
//    def toKeyValueMap(keyFn: SqlNode => String, valueFn: SqlNode => String): Map[String, String] = {
//      def toMap(l: List[SqlNode]): Map[String, String] = l match {
//        case key :: value :: rest => toMap(rest) + (keyFn(key) -> valueFn(value))
//        case _ => Map.empty[String, String]
//      }
//
//      toMap(nodeList.getList.asScala.toList)
//    }
//
//    def toParameterMap: Map[String, String] =
//      nodeList.toKeyValueMap(_.toString.toUpperCase, toUnquotedString)


    def toParameterMap: Map[String, String] = {
      // This function correctly extracts the key's name, even if it's quoted
      def keyNodeToString(node: SqlNode): String = node match {
        case id: SqlIdentifier => id.getSimple.toUpperCase
        case _ => node.toString.toUpperCase
      }
      nodeList.toKeyValueMap(keyNodeToString, toUnquotedString)
    }
  }
}