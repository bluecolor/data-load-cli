package io.blue

import io.blue.config.Config
import io.blue.cli.Cli

class Options(var cli: Cli, var config: Config) {
  var parallel: Int = if(cli.parallel > 0) {
    cli.parallel
  } else {
    config.app.parallel
  }
}