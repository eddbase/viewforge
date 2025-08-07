package eddbase

import eddbase.codegen.AirflowCodeGenerator
//import eddbase.codegen.PySparkCodeGenerator
import eddbase.pipeline.PipelineBuilder
import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser

import java.io.PrintWriter
import scala.io.Source
import scala.jdk.CollectionConverters.IterableHasAsScala

object Main {
  def main(args: Array[String]): Unit = {
    val baseFileName = "icebergpipeline" // "pipeline", or "icebergpipeline", or  "filepipeline"
    val sql = Source.fromFile(s"$baseFileName.sql").getLines().mkString("\n")

    val parserConfig = SqlParser.config()
      .withLex(Lex.MYSQL)
      .withParserFactory(eddbase.parser.impl.SqlParserImpl.FACTORY)

    // Create the parser
    val parser = SqlParser.create(sql, parserConfig)

    // Parse the SQL
    val nodeList = parser.parseStmtList()

    // Print the parsed output
    println("Parsed SQL:")
    println(nodeList.toString)

    val (nodes, _) = PipelineBuilder.build(nodeList.getList.asScala.toList)
    nodes.foreach(n => println(n))

//    // this part is for testing MetadataGenerator
//    val metadataGenerator = new MetadataGenerator()
//    val metadataString = metadataGenerator.codegen(nodes, "my_app")
//    println(metadataString)

//    // this part is for testing PySparkCodeGenerator
//    val cg = new PySparkCodeGenerator
//    val code = cg.codegen(nodes, "SparkExample")
//    println(code)
//

    val airflowGenerator = new AirflowCodeGenerator()
    val code = airflowGenerator.codegen(nodes, "my_app")

    println(code)

    val writer = new PrintWriter(s"$baseFileName.py")
    writer.println(code)
    writer.close()

    //    testCalcite()
    //    testCalciteExtension()
  }

  def testCalcite(): Unit = {
    val sql = "SELECT id, name FROM employees WHERE department = 'Engineering'"

    val parserConfig = SqlParser.config().withLex(Lex.MYSQL)

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
          |CREATE CATALOG pg_catalog (
          |  TYPE = 'database',
          |  URL = 'postgres url',
          |  USERNAME = 'test',
          |  PASSWORD = '1234'
          |);
          |
          |CREATE STREAM OrderTbl FROM pg_catalog.retailer.OrderTable;
          |
          |CREATE STREAM PgOrderTbl FROM pg_catalog.shopping.Customer (
          |  CDC_TIMESTAMP = 'last_update_date'
          |);
          |
          |CREATE VIEW TargetTbl (
          |  TARGET_LAG = '1 minute',
          |  SINK = 'my_nessie'
          |)
          |AS
          |  SELECT header.last_update_date
          |  FROM OrderTbl AS O JOIN pg_catalog.retailer.Employee ON O.A = pg_catalog.retailer.Employee.id;
          |""".stripMargin

    val parserConfig = SqlParser.config()
      .withLex(Lex.MYSQL)
      .withParserFactory(eddbase.parser.impl.SqlParserImpl.FACTORY)

    // Create the parser
    val parser = SqlParser.create(sql, parserConfig)

    // Parse the SQL
    val nodeList = parser.parseStmtList()

    // Print the parsed output
    println("Parsed SQL:")
    println(nodeList.toString)

    val (nodes, _) = PipelineBuilder.build(nodeList.getList.asScala.toList)
    nodes.foreach(n => println(n))

//    val cg = new PySparkCodeGenerator
//    val code = cg.codegen(nodes, "SparkExample")
//    println(code)
    val cg = new AirflowCodeGenerator
    val code = cg.codegen(nodes, "AirflowExample")
    println(code)

  }
}