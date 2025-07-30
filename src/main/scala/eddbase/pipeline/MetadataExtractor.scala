package eddbase.pipeline

import Helpers.{SqlIdentifierImplicit,SqlNodeImplicit,SqlNodeListImplicit}
import eddbase.parser.{SqlCreateCatalog,SqlCreateStream,SqlCreateView}
import org.apache.calcite.sql.SqlNode

object MetadataExtractor {
  final class Context(mapping: Map[QualifiedTableName, Node] = Map.empty) {

    def contains(name: QualifiedTableName): Boolean = mapping.contains(name)

    def get(name: QualifiedTableName): Option[Node] = mapping.get(name)

    def put(name: QualifiedTableName, node: Node): Context =
      new Context(mapping + (name -> node))
  }
}
