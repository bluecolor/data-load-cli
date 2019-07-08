package io.blue

import io.blue.config.Config
import io.blue.cli.Cli

class Options(var cli: Cli, var config: Config) {

  var parallel: Int = if(cli.parallel != null && cli.parallel > 0) {
    cli.parallel
  } else {
    config.app.parallel
  }

  var sourceParallel: Int = {
    val name = this.cli.source
    if (cli.sourceParallel != null && cli.sourceParallel > 0) {
      cli.sourceParallel
    } else {
      val parallel = config.getSourceConnector(name).parallel
      if (parallel == null) { parallel } else { this.parallel }
    }
  }

  var targetParallel: Int = {
    val name = this.cli.target
    if (cli.targetParallel != null && cli.targetParallel > 0) {
      cli.targetParallel
    } else {
      val parallel = config.getTargetConnector(name).parallel
      if (parallel == null) { parallel } else { this.parallel }
    }
  }

}