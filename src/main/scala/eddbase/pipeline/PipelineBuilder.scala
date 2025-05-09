package eddbase.pipeline

import Helpers.{SqlIdentifierImplicit, SqlNodeImplicit, SqlNodeListImplicit}
import eddbase.parser.{SqlCreateCatalog, SqlCreateStream, SqlCreateView}
import org.apache.calcite.sql.SqlNode

object PipelineBuilder {

  final class Context(mapping: Map[QualifiedTableName, Node] = Map.empty) {

    def contains(name: QualifiedTableName): Boolean = mapping.contains(name)

    def get(name: QualifiedTableName): Option[Node] = mapping.get(name)

    def put(name: QualifiedTableName, node: Node): Context =
      new Context(mapping + (name -> node))
  }

  def build(nodes: List[SqlNode]): (List[Node], Context) = {
    val (reversedAcc, finalCtx) =
      nodes.foldLeft((List.empty[Node], new Context)) {
        case ((acc, ctx), node) =>
          val (newNode, newCtx) = build(node, ctx)
          (newNode :: acc, newCtx)
      }
    (reversedAcc.reverse, finalCtx)
  }

  private def build(node: SqlNode, ctx: Context): (Node, Context) = node match {
    case s: SqlCreateCatalog => build(s, ctx)
    case s: SqlCreateStream => build(s, ctx)
    case s: SqlCreateView => build(s, ctx)
    case _ => sys.error("Unsupported SQL statement: " + node)
  }

  private def build(node: SqlCreateCatalog, ctx: Context): (Node, Context) = {
    val catalog = buildCatalog(node)
    ctx.get(catalog.name) match {
      case Some(_) =>
        sys.error(s"Node with name ${catalog.name} already exists")
      case None =>
        (catalog, ctx.put(catalog.name, catalog))
    }
  }

  private def buildCatalog(node: SqlCreateCatalog): Catalog = {
    // Catalog name
    val name = node.name.getSimpleTableName
    // Catalog type and parameters
    val (tp, paramMap) = {
      val params = node.params.toParameterMap
      require(params.contains("TYPE"), "Catalog type missing: TYPE = ...")
      val tp = CatalogType.fromString(params("TYPE"))
      (tp, params - "TYPE")
    }
    // Construct catalog node based on type
    tp match {
      case DatabaseCatalogType =>
        require(paramMap.contains("URL"), "JDBC URL missing")
        val connUrl = paramMap("URL")
        val connParams = paramMap - "URL"
        DatabaseCatalog(name, connUrl, connParams)

      case FileCatalogType =>
        FileCatalog(name, paramMap)

      case IcebergCatalogType =>
        IcebergCatalog(name, paramMap)
    }
  }

  private def build(node: SqlCreateStream, ctx: Context): (Node, Context) = {
    val (stream, newCtx) = buildStream(node, ctx)
    ctx.get(stream.name) match {
      case Some(_) =>
        sys.error(s"Node with name ${stream.name} already exists")
      case _ =>
        (stream, newCtx.put(stream.name, stream))
    }
  }

  private def buildStream(node: SqlCreateStream, ctx: Context): (Stream, Context) = {
    // Stream name
    val name = node.name.getSimpleTableName
    // Source table
    val sourceTableName = node.sourceTable.getCompoundTableName
    val (sourceTable, newCtx) = buildTable(sourceTableName, ctx)
    // Construct stream node
    val params = node.params.toParameterMap
    val stream = Stream(name, sourceTable, params)
    (stream, newCtx)
  }

  private def buildTable(name: QualifiedTableName, ctx: Context): (Table, Context) = {
    // Resolve catalog
    require(name.catalog.nonEmpty, s"Table catalog not specified for ${name}")
    val catalogName = SimpleTableName(name.catalog.toList)
    val catalog = ctx.get(catalogName) match {
      case Some(c: Catalog) => c
      case _ => sys.error(s"Catalog name ${catalogName} does not exist")
    }
    // Resolve table
    ctx.get(name) match {
      case Some(t: Table) => (t, ctx)
      case Some(_) => sys.error(s"Table name ${name} already exists")
      case _ =>
        val t = Table(name, catalog)
        (t, ctx.put(name, t))
    }
  }

  private def build(node: SqlCreateView, ctx: Context): (Node, Context) = {
    // View name
    val name = node.name.getSimpleTableName
    assert(name.isSimple, s"View name must be simple: ${name}")
    // Resolve source tables
    val sourceNames = node.extractTableNames()
    val (reversedSources, finalCtx) =
      sourceNames.foldLeft((List.empty[DataSource], ctx)) {
        case ((srcAcc, ctxAcc), id) =>
          val srcTableName = id.getQualifiedName
          ctxAcc.get(srcTableName) match {
            case Some(s: DataSource) =>
              (s :: srcAcc, ctxAcc)
            case _ =>
              val (t, newCtxAcc) = buildTable(srcTableName, ctxAcc)
              (t :: srcAcc, newCtxAcc)
          }
      }
    // View parameters
    val params = node.params.toParameterMap
    val view = View(name, params, reversedSources.reverse, node.query)
    (view, finalCtx.put(name, view))
  }

}
