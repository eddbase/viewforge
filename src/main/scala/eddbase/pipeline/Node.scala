package eddbase.pipeline

import org.apache.calcite.sql.SqlNode

sealed trait Node {
  def name: QualifiedTableName
}

trait Catalog extends Node {
  def tp: CatalogType

  def params: Map[String, String]
}

trait DataSource extends Node {
  def updatable: Boolean
}

case class DatabaseCatalog(name: QualifiedTableName, url: String, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = DatabaseCatalogType
}

case class FileCatalog(name: QualifiedTableName, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = FileCatalogType
}

case class IcebergCatalog(name: QualifiedTableName, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = IcebergCatalogType
}

case class Table(name: QualifiedTableName, catalog: Catalog) extends DataSource {
  def updatable: Boolean = false
}

case class Stream(name: QualifiedTableName, source: Table, params: Map[String, String]) extends DataSource {
  def updatable: Boolean = true
}

case class View(name: QualifiedTableName, params: Map[String, String], sources: List[DataSource], query: SqlNode) extends DataSource {
  def updatable: Boolean = sources.exists(_.updatable)
}
