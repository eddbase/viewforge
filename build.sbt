ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "ViewForge",
    libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.39.0",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.17",
  )

lazy val generateParserCode = taskKey[Seq[File]]("Generate parser code using FMPP")

generateParserCode := {
  val codeGenDir = (Compile / sourceDirectory).value / "codegen"
  val defaultConfig = codeGenDir / "default_config.fmpp"
  val config = codeGenDir / "config.fmpp"
  val templateDirectory = codeGenDir / "templates"
  val javaCCOutputDir = (Compile / sourceManaged).value / "output"

  val parserCodegen = new ParserCodeGenerator(streams.value.log)

  // Generate JavaCC parser file
  val javaCCFile =
    parserCodegen.generateParserTemplate(defaultConfig, config, templateDirectory, javaCCOutputDir)

  // Generate Java parser files
  val parserCodeOutputDir = (Compile / sourceManaged).value / "eddbase" / "parser" / "impl"
  parserCodegen.generateParserSource(javaCCFile, parserCodeOutputDir)

  parserCodeOutputDir.listFiles
}

Compile / sourceGenerators += generateParserCode.taskValue

addCommandAlias("generate-parser", "Compile / generateParserCode")
