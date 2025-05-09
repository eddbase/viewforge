package eddbase

object Utils {

  // Fresh variables name provider
  private val counter = scala.collection.mutable.HashMap[String, Int]()

  def fresh(name: String = "x"): String = {
    val c = counter.getOrElse(name, 0) + 1
    counter(name) = c
    name + c
  }

  def freshClear(): Unit = counter.clear

  // Indent text by n*2 spaces (and trim trailing space)
  def ind(s: String, n: Int = 1): String = {
    val i = "  " * n
    i + s.replaceAll("\n? *$", "").replaceAll("\n", "\n" + i)
  }

  implicit class ListStringImplicit(names: List[String]) {
    def toCamelCase: String = names match {
      case Nil => ""
      case hd :: tl =>
        hd.toLowerCase + tl.map(_.toLowerCase.capitalize).mkString
    }

    def toUpperCamelCase: String =
      names.map(_.toLowerCase.capitalize).mkString

    def toSnakeCase: String =
      names.map(_.toLowerCase).mkString("_")

    def toUpperSnakeCase: String =
      names.map(_.toUpperCase).mkString("_")

    def toSql: String = names.mkString(".")

  }
}
