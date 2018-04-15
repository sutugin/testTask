package sutugin.testtask

object CommandLineArgsParser {

  private val parser = new scopt.OptionParser[CommandLineArgs](Config.appName) {
    head(Config.appName, "0.0.1")

    opt[String]("appName").valueName("<appName>").action( (x, c) =>
      c.copy(appName = x) ).text("application name string")

    opt[String]("inputCsv").required().valueName("<inputCsv>").action( (x, c) =>
      c.copy(inputCsv = x) ).required().text("path to csv file")

    opt[String]("csvOptions").required().valueName("<csvOptions>").action( (x, c) =>
      c.copy(x)).text("csvOptions")

    opt[String]("convertArgs").required().valueName("<convertArgs>").
      action( (x, c) => c.copy(convertArgs = x) ).text("convertArgs")

    opt[String]("resultOutput").required().valueName("<resultOutput>").action( (x, c) =>
      c.copy(resultOutput = x) ).text("path to result json")
  }

  def initConfig(args: Seq[String]): CommandLineArgs = {
    parser.parse(args, CommandLineArgs()) match {
      case Some(config) => config
      case _ =>  throw new IllegalArgumentException("Bade input args")
    }
  }

}
