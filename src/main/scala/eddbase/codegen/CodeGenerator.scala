package eddbase.codegen

import eddbase.pipeline.Node

trait CodeGenerator {
  def codegen(nodes: List[Node], appName: String): String
}
