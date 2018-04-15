package sutugin.testtask

case class CommandLineArgs(appName: String = Config.appName,
                           inputCsv: String =Config.inputCsv ,
                           csvOptions: String = Config.csvOptionsStr,
                           convertArgs: String = Config.convertArgsStr,
                           resultOutput: String = Config.resultOutput)
