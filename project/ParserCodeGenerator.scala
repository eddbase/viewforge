import sbt._
import sbt.util.Logger
import fmpp.setting.Settings
import org.javacc.parser.Main
import java.nio.charset.Charset
import java.util.Locale
import javax.tools.{StandardLocation, ToolProvider}
import scala.jdk.CollectionConverters.asJavaIterableConverter

class ParserCodeGenerator(logger: Logger) {

  def generateParserTemplate(defaultConfig: File, config: File, templateDirectory: File, outputDirectory: File): File = {
    logger.info(
      s"""FMPP settings:
         |  defaultConfig : ${defaultConfig.toString()},
         |  config : ${config.toString()},
         |  template : ${templateDirectory.toString()},
         |  target : ${outputDirectory.toString()}
         |""".stripMargin)

    val fmppSettings = new Settings(config.asFile)
    fmppSettings.set(
      Settings.NAME_DATA,
      s"tdd(${config.getAbsolutePath.tddString}), default: tdd(${defaultConfig.getAbsolutePath.tddString})"
    )
    fmppSettings.set(Settings.NAME_SOURCE_ROOT, templateDirectory.getAbsolutePath)
    fmppSettings.set(Settings.NAME_OUTPUT_ROOT, outputDirectory.getAbsolutePath)
    fmppSettings.loadDefaults(config.asFile)
    fmppSettings.execute()
    logger.info("Processing Done")

    val outputFile = outputDirectory / "javacc" / "Parser.jj"
    assert(outputFile.exists(), "Processing done but no Parser.jj found")
    outputFile
  }

  def generateParserSource(template: File, outputDirectory: File, static: Boolean = false, lookAhead: Int = 1): Unit = {
    val args = Array(
      s"-STATIC=$static",
      s"-LOOKAHEAD:$lookAhead",
      s"-OUTPUT_DIRECTORY:${outputDirectory}",
      s"${template}"
    )
    logger.info(
      s"""JavaCC settings:
         |  ${args.mkString(",\n  ")}
         |""".stripMargin)

    Main.mainProgram(args)
    logger.info("Processing Done")
  }

  def compileParserTemplate(sourceFiles: Seq[File], outputDirectory: File, javacOptions: Seq[String]): Unit = {
//    val compilerOptions = {
//      if (javacOptions.isEmpty) null
//      else javacOptions.asJava
//    }

//    val compilerOptions = {
//      if (javacOptions.isEmpty) null
//      else javacOptions.asJava
//    }
    val compiler = ToolProvider.getSystemJavaCompiler
    val fileManager = compiler.getStandardFileManager(null, Locale.getDefault, Charset.defaultCharset())
    fileManager.setLocation(StandardLocation.CLASS_OUTPUT, java.util.Arrays.asList(outputDirectory.asFile))
    val success = compiler.getTask(
      null,
      fileManager,
      null,
      javacOptions.asJava,
      null,
      fileManager.getJavaFileObjectsFromFiles(sourceFiles.asJava)
//        sourcesFiles.flatMap(dir => dir.deepFiles.map(_.jfile))
//          .toList.asJava
//      )
    ).call()
    fileManager.close()
    assert(success, "Compilation failed")
  }

  implicit class TddString(stringData: String) {
    def tddString: String =
      "\"" + stringData.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  }
}
