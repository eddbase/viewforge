package eddbase.pipeline

sealed trait CatalogType

case object DatabaseCatalogType extends CatalogType {
  override def toString: String = CatalogType.Database
}

case object FileCatalogType extends CatalogType {
  override def toString: String = CatalogType.File
}

case object IcebergCatalogType extends CatalogType {
  override def toString: String = CatalogType.Iceberg
}

object CatalogType {
  // Constants for catalog type names
  final val Database = "DATABASE"
  final val File = "FILE"
  final val Iceberg = "ICEBERG"

  def fromString(s: String): CatalogType = s.toUpperCase match {
    case Database => DatabaseCatalogType
    case File => FileCatalogType
    case Iceberg => IcebergCatalogType
    case other => sys.error(s"Unknown catalog type: ${other}")
  }
}