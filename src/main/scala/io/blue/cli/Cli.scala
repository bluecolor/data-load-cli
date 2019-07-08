package io.blue.cli

import picocli.CommandLine
import picocli.CommandLine._


object Cli {
  def parse(args: Array[String]) : Cli = {
    var cli = new Cli
    new CommandLine(cli).parseArgs(args: _*)
    cli
  }
}

class Cli {

  @Option(names = Array("-h", "--help"), usageHelp = true, description = Array("display help message"))
  var helpRequested: Boolean = false

  @Option(names = Array("-c", "--config"), description = Array("config file (yaml)"))
  var config: String = _

  @Option(names = Array("-s", "--source"), description = Array("source connector"))
  var source: String = _

  @Option(names = Array("-t", "--target"), description = Array("target connector"))
  var target: String = _

  @Option(names = Array("-p", "--parallel"), description = Array("number of parallel transfer jobs"))
  var parallel: Integer = _

  @Option(names = Array("--source_parallel"), description = Array("number of parallel source jobs"))
  var sourceParallel: Integer = _

  @Option(names = Array("--target_parallel"), description = Array("number of parallel target jobs"))
  var targetParallel: Integer = _

  @Option(names = Array("-a", "--source_table"), description = Array("Source table to name"))
  var sourceTable: String = _

  @Option(names = Array("-b", "--target_table"), description = Array("Target table to name"))
  var targetTable: String = _

  @Option(names = Array("--truncate"), description = Array("Truncate table to name"))
  var truncate: Boolean = _

  def printHelp() {
    CommandLine.usage(this, System.out)
  }
}