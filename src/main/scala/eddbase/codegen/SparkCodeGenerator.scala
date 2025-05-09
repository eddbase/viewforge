package eddbase.codegen

import eddbase.Utils
import eddbase.Utils.ListStringImplicit
import eddbase.pipeline.Helpers.{SqlIdentifierImplicit, SqlNodeImplicit}
import eddbase.pipeline.{Catalog, DatabaseCatalog, Node, QualifiedTableName, SimpleTableName, Stream, Table, View}

trait SparkCodeGenerator extends CodeGenerator

class PySparkCodeGenerator extends SparkCodeGenerator {

  final class Context(mapping: Map[QualifiedTableName, Map[String, String]] = Map.empty) {

    def contains(key: QualifiedTableName): Boolean = mapping.contains(key)

    def get(key: QualifiedTableName): Option[Map[String, String]] = mapping.get(key)

    def put(key: QualifiedTableName, value: Map[String, String]): Context =
      new Context(mapping + (key -> value))
  }

  def codegen(nodes: List[Node], appName: String): String = {
    val body = codegen(nodes, new Context)._1.mkString("\n")
    generateImports + initialiseSession(appName) + body
  }

  private def codegen(nodes: List[Node], ctx: Context): (List[String], Context) =
    nodes.foldLeft((List.empty[String], ctx)) {
      case ((accCode, accCtx), node) =>
        val (newCode, newCtx) = codegen(node, accCtx)
        (accCode ++ newCode, newCtx)
    }

  private def codegen(node: Node, ctx: Context): (List[String], Context) = node match {
    case c: Catalog => codegen(c, ctx)
    case t: Table => codegen(t, ctx)
    case s: Stream => codegen(s, ctx)
    case v: View => codegen(v, ctx)
    case _ => sys.error(s"Unexpected node: ${node}")
  }

  private def codegen(catalog: Catalog, ctx: Context): (List[String], Context) =
    if (ctx.contains(catalog.name)) (Nil, ctx) else
      catalog match {
        case c: DatabaseCatalog =>
          val urlKey = Utils.fresh(s"${c.name}_url")
          val propsKey = Utils.fresh(s"${c.name}_properties")
          val propsValue = c.params.map(x => s""""${x._1}" : "${x._2}"""").mkString(",\n")
          val code =
            s"""|# JDBC connection parameters for ${c.name}
                |${urlKey} = "${c.url}"
                |${propsKey} = {
                |${Utils.ind(propsValue)}
                |}
                |""".stripMargin
          val mapping = Map("JDBC_URL" -> urlKey, "JDBC_PROPERTIES" -> propsKey)
          (List(code), ctx.put(c.name, mapping))
        case _ => (Nil, ctx)
      }

  private def codegen(table: Table, ctx: Context): (List[String], Context) =
    if (ctx.contains(table.name)) (Nil, ctx) else {
      val (catalogCode, newCtx) = codegen(table.catalog, ctx)
      val name = Utils.fresh(table.name.table.toSnakeCase)
      val params = newCtx.get(table.catalog.name).get
      val tableCode =
        s"""|df_${name} = spark.read.jdbc( \\
            |  url=${params("JDBC_URL")}, \\
            |  properties=${params("JDBC_PROPERTIES")}, \\
            |  table="${table.name.table.mkString(".")}" \\
            |)
            |df_${name}.createOrReplaceTempView("${name}")
            |""".stripMargin
      (catalogCode :+ tableCode, ctx.put(table.name, Map("VIEW_NAME" -> name)))
    }

  private def codegen(stream: Stream, ctx: Context): (List[String], Context) =
    if (ctx.contains(stream.source.name)) (Nil, ctx) else {
      val (newCode, newCtx) = codegen(stream.source, ctx)
      val tableRef = newCtx.get(stream.source.name).get.apply("VIEW_NAME")
      (newCode, newCtx.put(stream.name, Map("VIEW_NAME" -> tableRef)))
    }

  private def codegen(view: View, ctx: Context): (List[String], Context) = {
    // Register referenced tables as views, if needed
    val (sources, finalCtx) =
      view.sources.foldLeft((List.empty[String], ctx)) {
        case ((accCode, accCtx), t: Table) =>
          val (newCode, newAccCtx) = codegen(t, accCtx)
          (accCode ++ newCode, newAccCtx)
        case ((accCode, accCtx), n) if ctx.contains(n.name) =>
          (accCode, accCtx)
        case (_, n) =>
          sys.error(s"Source name ${n.name} missing from context")
      }
    // Rename tables in SQL query
    val viewSources = view.query.extractTableNames()
    val renamingFn = viewSources.map(id =>
      finalCtx.get(id.getQualifiedName).map(m => id -> m("VIEW_NAME"))
    ).flatten.toMap
    val query = view.query.rewriteQuery(renamingFn)
    val queryStr = query.toString.replaceAll("\n", " \\\\\n")

    // Create view computation
    val viewQueryRef = Utils.fresh(s"query_${view.name.table.toSnakeCase}")
    val viewRef = Utils.fresh(s"${view.name.table.toSnakeCase}")
    val viewCode =
      s"""|${viewQueryRef} = "\\
          |${Utils.ind(queryStr)} \\
          |"
          |
          |df_${viewRef} = spark.sql(${viewQueryRef})
          |df_${viewRef}.createOrReplaceTempView("${viewRef}")
          |""".stripMargin

    // Create sink
    val sinkCode =
      view.params.get("SINK") match {
        case Some(d) =>
          val catalogName = SimpleTableName(List(d))
          val catalogParams = finalCtx.get(catalogName).get
          val code =
            s"""|df_${viewRef}.write.jdbc( \\
                |  url=${catalogParams("JDBC_URL")}, \\
                |  properties=${catalogParams("JDBC_PROPERTIES")}, \\
                |  table="${view.name.table.mkString(".")}" \\
                |)
                |""".stripMargin
          List(code)
        case None => Nil
      }
    (sources ++ (viewCode :: sinkCode), finalCtx.put(view.name, Map("VIEW_NAME" -> viewRef)))
  }

  private def generateImports: String =
    s"""|from pyspark.sql import SparkSession
        |
        |""".stripMargin

  private def initialiseSession(appName: String): String = {
    s"""|# Initialize Spark session
        |spark = SparkSession.builder \\
        |  .appName("${appName}") \\
        |  .config("spark.jars", "/Users/mnikolic/repo/projects/ViewForge/lib/postgresql-42.7.5.jar") \\
        |  .getOrCreate()
        |
        |""".stripMargin
  }

}
