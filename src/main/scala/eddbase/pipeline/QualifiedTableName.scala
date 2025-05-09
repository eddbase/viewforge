package eddbase.pipeline

import eddbase.Utils

trait QualifiedTableName {
  def parts: List[String]

  def table: List[String]

  def catalog: Option[String]

  def isSimple: Boolean

  override def toString: String = parts.mkString(".")
}

final case class SimpleTableName(parts: List[String]) extends QualifiedTableName {
  require(parts.size > 0, "SimpleTableName must have at least one part")

  def table: List[String] = parts

  def catalog: Option[String] = None

  def isSimple: Boolean = true
}

final case class CompoundTableName(parts: List[String]) extends QualifiedTableName {
  require(parts.size > 1, "CompoundTableName must have at least two parts")

  def table: List[String] = parts.tail

  def catalog: Option[String] = parts.headOption

  def isSimple: Boolean = false
}
