package eddbase.codegen

import eddbase.pipeline._
import eddbase.pipeline.Helpers.SqlNodeImplicit
import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

class MetadataGenerator extends CodeGenerator {

  override def codegen(nodes: List[Node], appName: String): String = {
    val catalogs = mutable.LinkedHashMap[String, String]()
    val sources = mutable.LinkedHashMap[String, String]()
    val views = mutable.LinkedHashMap[String, String]()

    nodes.collect { case c: Catalog => c }.foreach { c =>
      catalogs += (c.name.toString -> generateCatalogMetadata(c))
    }
    val streams = nodes.collect { case s: Stream => s }
    streams.foreach { s =>
      sources += (s.name.toString -> generateSourceMetadataFromStream(s))
    }
    val streamSourceTableNames = streams.map(_.source.name).toSet
    nodes.collect { case v: View => v }.foreach { view =>
      view.sources.collect { case t: Table => t }.foreach { table =>
        if (!streamSourceTableNames.contains(table.name)) {
          val sourceName = table.name.table.last
          if (!sources.contains(sourceName)) {
            sources += (sourceName -> generateSourceMetadataFromTable(table))
          }
        }
      }
    }
    nodes.collect { case v: View => v }.foreach { v =>
      views += (v.name.toString -> generateViewMetadata(v))
    }

    val catalogEntries = catalogs.values.toList.map(indent(_)).mkString(",\n")
    val sourceEntries = sources.values.toList.map(indent(_)).mkString(",\n")
    val viewEntries = views.values.toList.map(indent(_)).mkString(",\n")

    val dslMetadata =
      s"""
         |DSL_METADATA = {
         |    "catalogs": {
         |$catalogEntries
         |    },
         |    "sources": {
         |$sourceEntries
         |    },
         |    "views": {
         |$viewEntries
         |    }
         |}
         |""".stripMargin
    dslMetadata.stripMargin.trim
  }
//
//  private def generateCatalogMetadata(catalog: Catalog): String = {
//    val name = catalog.name.toString
//    val (catalogType, subType, config) = catalog match {
//      case dc: DatabaseCatalog =>
//        val orderedConfig = LinkedHashMap[String, String]()
//        val params = dc.params
//
//        orderedConfig.put("url", dc.url)
//        params.get("DRIVER").foreach(d => orderedConfig.put("driver", d))
//
//        params.get("USERNAME").foreach(u => orderedConfig.put("user", u))
//
//        params.get("PASSWORD").foreach(p => orderedConfig.put("password", p))
//
//        val knownKeys = Set("DRIVER", "USERNAME", "PASSWORD")
//        val otherParams = params.filter { case (k, _) => !knownKeys.contains(k) }
//        otherParams.foreach { case (k, v) => orderedConfig.put(k.toLowerCase(), v) }
//
//        ("database", None, orderedConfig)
//
//      case ic: IcebergCatalog =>
//        val sub = ic.params.get("SUB_TYPE").map(_.toLowerCase)
//        ("iceberg", sub, ic.params - "SUB_TYPE")
//
//      case fc: FileCatalog =>
//        ("file", None, fc.params)
//    }
//
//    val subTypeEntry = subType.map(s => s""""sub_type": "$s",""").getOrElse("")
//    val configEntries = config.map { case (k, v) => s""""$k": "$v"""" }.mkString(",\n" + " " * 12)
//
//    s""""$name": {
//       |        "type": "$catalogType",
//       |        $subTypeEntry
//       |        "config": {
//       |            $configEntries
//       |        }
//       |    }""".stripMargin
//  }
  private def generateCatalogMetadata(catalog: Catalog): String = {
    val name = catalog.name.toString
    val (catalogType, sub_type, config) = catalog match {
      case dc: DatabaseCatalog =>
        // This logic correctly creates an ordered map with lowercase keys already
        val orderedConfig = LinkedHashMap[String, String]()
        val params = dc.params

        orderedConfig.put("url", dc.url)
        params.get("DRIVER").foreach(d => orderedConfig.put("driver", d))
        params.get("USERNAME").foreach(u => orderedConfig.put("user", u))
        params.get("PASSWORD").foreach(p => orderedConfig.put("password", p))

        val knownKeys = Set("DRIVER", "USERNAME", "PASSWORD")
        val otherParams = params.filter { case (k, _) => !knownKeys.contains(k) }
        otherParams.foreach { case (k, v) => orderedConfig.put(k.toLowerCase(), v) }

        ("database", None, orderedConfig)

      case ic: IcebergCatalog =>
        val sub = ic.params.get("SUB_TYPE").map(_.toLowerCase)
        // For Iceberg, the keys in `params` are uppercase. We will lowercase them below.
        ("iceberg", sub, ic.params - "SUB_TYPE")

      case fc: FileCatalog =>
        ("file", None, fc.params)
    }

    val subTypeEntry = sub_type.map(s => s""""sub_type": "$s",""").getOrElse("")

    // *** THIS IS THE CORRECTED LINE ***
    // Ensure all keys in the final JSON config are lowercase.
    val configEntries = config.map { case (k, v) => s""""${k.toLowerCase()}": "$v"""" }.mkString(",\n" + " " * 12)

    s""""$name": {
       |        "type": "$catalogType",
       |        $subTypeEntry
       |        "config": {
       |            $configEntries
       |        }
       |    }""".stripMargin
  }


  private def generateSourceMetadataFromStream(stream: Stream): String = {
////    val tableName = stream.source.name.table.last
////    s""""${stream.name}": {"catalog": "${stream.source.catalog.name}", "table": "$tableName"}"""
//    val tableName = stream.source.name.table.mkString(".")
//    s""""${stream.name}": {"catalog": "${stream.source.catalog.name}", "table": "$tableName"}"""
    val tableName = stream.source.catalog match {
       case _: IcebergCatalog => stream.source.name.table.mkString(".") // Full name for Iceberg
       case _ => stream.source.name.table.last // Simple name for others (like JDBC)
    }
    val paramsString = stream.params
      .map { case (k, v) => s""""$k": "$v"""" }
      .mkString(", ")

    s""""${stream.name}": {"catalog": "${stream.source.catalog.name}", "table": "$tableName", "params": {$paramsString}}"""

  }

  private def generateSourceMetadataFromTable(table: Table): String = {
    val sourceName = table.name.table.last
////    val tableNameInCatalog = table.name.table.last
////    s""""$sourceName": {"catalog": "${table.catalog.name}", "table": "$tableNameInCatalog"}"""
//    val tableNameInCatalog = table.name.table.mkString(".")
//    s""""$sourceName": {"catalog": "${table.catalog.name}", "table": "$tableNameInCatalog"}"""
    val tableNameInCatalog = table.catalog match {
      case _: IcebergCatalog => table.name.table.mkString(".") // Full name for Iceberg
      case _ => table.name.table.last // Simple name for others (like JDBC)
    }
    s""""$sourceName": {"catalog": "${table.catalog.name}", "table": "$tableNameInCatalog"}"""

  }


  private def generateViewMetadata(view: View): String = {
    val name = view.name.toString
    val targetLag = view.params.get("TARGET_LAG").map(lag => s"""    "target_lag": "$lag",""").getOrElse("")
    val dependsOn = view.sources
      .collect { case v: View => v.name.toString }
      .distinct
      .map(depName => s""""$depName"""")
      .mkString(", ")

    // 1. Extract all table identifiers from the original query SqlNode.
    val tableIdentifiers = view.query.extractTableNames(fromOrJoin = true)

    // 2. Create a mapping from each full identifier to its simple name.
    //    e.g., `pg_catalog.tpch.Customer` -> `Customer`
    val renamingMap = tableIdentifiers.map { id =>
      val simpleName = id.names.asScala.last
      id -> simpleName
    }.toMap

    // 3. Rewrite the query using the helper function.
    val rewrittenQueryNode = view.query.rewriteQuery(renamingMap)

    // 4. Convert the NEW, rewritten query node to a string for the output.
    val query = rewrittenQueryNode.toString.replaceAll("\n", "\n                ")

    val materialize = generateMaterializeMetadata(view)

    s""""$name": {
       |$targetLag
       |        "depends_on": [$dependsOn],
       |        "dataset_uri": "view://${name.toLowerCase}",
       |        "query": ""\"
       |                $query
       |            ""\",
       |        "materialize": $materialize
       |    }""".stripMargin
  }

  private def generateMaterializeMetadata(view: View): String = {
    view.params.get("SINK") match {
      case Some(sinkCatalog) =>
        s"""{"type": "sink", "catalog": "$sinkCatalog", "table": "${view.name}"}"""
      case None =>
        """{"type": "xcom"}"""
    }
  }

  private def indent(s: String, spaces: Int = 8): String = {
    s.split('\n').map(" " * spaces + _).mkString("\n")
  }
}