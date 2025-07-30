package eddbase.codegen

import eddbase.pipeline.Node
import scala.io.Source
import scala.util.Using

class AirflowCodeGenerator extends CodeGenerator {

  private def loadTemplate(name: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(name)
    Using(Source.fromInputStream(stream)) { source =>
      source.mkString
    }.get
  }

  override def codegen(nodes: List[Node], appName: String): String = {
    // 1. Reuse MetadataGenerator to create the METADATA string.
    val metadataGenerator = new MetadataGenerator()
    val metadataString = metadataGenerator.codegen(nodes, appName)

    // 2. Load the Python template file from resources.
    val template = loadTemplate("airflowTemplate.py")

    // 3. Replace the placeholder with the generated metadata string.
    val finalCode = template.replace("##METADATA_PLACEHOLDER##", metadataString)

    finalCode
  }
}