package eddbase

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser
import eddbase.parser.impl.SqlParserImpl

object Main {
  def main(args: Array[String]): Unit = {
    testCalcite()
    testCalciteExtension()
  }

  def testCalcite(): Unit = {
    val sql = "SELECT id, name FROM employees WHERE department = 'Engineering'"

    val parserConfig = SqlParser.config().withLex(Lex.MYSQL);

    // Create the parser
    val parser = SqlParser.create(sql, parserConfig)

    // Parse the SQL
    val sqlNode = parser.parseQuery

    // Print the parsed output
    println("Parsed SQL:")
    println(sqlNode.toString)
  }

  def testCalciteExtension(): Unit = {
    val sql = "SELECT id, name FROM employees WHERE department = 'Engineering'"

    val parserConfig = SqlParser.config()
      .withLex(Lex.MYSQL)
      .withParserFactory(eddbase.parser.impl.SqlParserImpl.FACTORY);

    // Create the parser
    val parser = SqlParser.create(sql, parserConfig)

    // Parse the SQL
    val sqlNode = parser.parseQuery

    // Print the parsed output
    println("Parsed SQL:")
    println(sqlNode.toString)
  }
}