package eddbase.dsl

sealed trait CatalogType

case object DatabaseCatalogType extends CatalogType {
  override def toString = "DATABASE"
}

case object FileCatalogType extends CatalogType {
  override def toString = "FILE"
}

case object IcebergCatalogType extends CatalogType {
  override def toString = "ICEBERG"
}

object CatalogType {
  def fromString(s: String): CatalogType = s.toUpperCase match {
    case "DATABASE" => DatabaseCatalogType
    case "FILE" => FileCatalogType
    case "ICEBERG" => IcebergCatalogType
    case err => sys.error("Unknown catalog type: " + err)
  }
}