package eddbase.dsl

import eddbase.parser.SqlCreateCatalog
import eddbase.dsl.Helpers.{SqlNodeListImplicit, toUnquotedString}

trait Catalog {
  def name: String

  def tp: CatalogType

  def params: Map[String, String]
}

case class DatabaseCatalog(name: String, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = DatabaseCatalogType
}

case class FileCatalog(name: String, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = FileCatalogType
}

case class IcebergCatalog(name: String, params: Map[String, String]) extends Catalog {
  def tp: CatalogType = IcebergCatalogType
}

object Catalog {
  def apply(name: String, tp: CatalogType, params: Map[String, String]): Catalog = tp match {
    case DatabaseCatalogType => DatabaseCatalog(name, params)
    case FileCatalogType => FileCatalog(name, params)
    case IcebergCatalogType => IcebergCatalog(name, params)
  }

  def fromSqlStatement(stmt: SqlCreateCatalog): Catalog = {
    assert(stmt.name.isSimple, "catalog name must be simple")
    val paramMap = stmt.params.toKeyValueMap(_.toString.toUpperCase, toUnquotedString)
    val tp = paramMap.get("TYPE").map(CatalogType.fromString).getOrElse(IcebergCatalogType)
    Catalog(stmt.name.getSimple, tp, paramMap)
  }
}
