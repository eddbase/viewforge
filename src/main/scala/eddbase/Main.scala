package eddbase

import eddbase.dsl.{Catalog, Program, Stream, View}
import eddbase.dsl.Helpers.SqlNodeImplicit
import eddbase.parser.{SqlCreateCatalog, SqlCreateStream, SqlCreateView}
import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser

object Main {
  def main(args: Array[String]): Unit = {
//    testCalcite()
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
    val sql =
      s"""|CREATE CATALOG my_nessie (
          |  TYPE = 'iceberg',
          |  SUB_TYPE = 'nessie',
          |  URI = 'nessie_url',
          |  WAREHOUSE = 's3a://warehouse'
          |);
          |
          |CREATE CATALOG test (
          |  TYPE = 'file'
          |);
          |
          |CREATE STREAM OrderTbl FROM my_nessie.OrderTable;
          |
          |CREATE STREAM PgOrderTbl FROM pg_catalog.OrderTable (
          |  CDC_TIMESTAMP = 'last_update_date'
          |);
          |
          |CREATE VIEW TargetTbl (
          |  TARGET_LAG = '1 minute',
          |  SINK = 'my_nessie'
          |)
          |AS
          |  SELECT header.last_update_date
          |  FROM OrderTbl;
          |""".stripMargin

    val parserConfig = SqlParser.config()
      .withLex(Lex.MYSQL)
      .withParserFactory(eddbase.parser.impl.SqlParserImpl.FACTORY);

    // Create the parser
    val parser = SqlParser.create(sql, parserConfig)

    // Parse the SQL
    val nodeList = parser.parseStmtList()

    // Print the parsed output
    println("Parsed SQL:")
    println(nodeList.toString)

    val catalogs = nodeList.collect {
      case s: SqlCreateCatalog => List(Catalog.fromSqlStatement(s))
    }

    val streams = nodeList.collect {
      case s: SqlCreateStream => List(Stream.fromSqlStatement(s))
    }

    val views = nodeList.collect {
      case s: SqlCreateView => List(View.fromSqlStatement(s))
    }

    val program = new Program(catalogs, streams, views)
    println(program)

    val codeGenerator = new CodeGenerator(program)
    codeGenerator.run()
  }
}