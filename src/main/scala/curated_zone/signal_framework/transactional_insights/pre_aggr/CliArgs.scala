package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr

private[pre_aggr] case class CliArgs(contractPath: String, fullReprocess: Boolean = false)

private[pre_aggr] object CliArgs {

    private val initOptions = CliArgs(contractPath = null)

    private def optionsParser = new scopt.OptionParser[CliArgs]("Pre-Aggregate Transactional Insights Job") {

        opt[String]("contract-path")
            .action((contractPath, c) => c.copy(contractPath = contractPath))
            .text("path to a contract for this job")
            .required

        opt[Boolean]("full-reprocess")
            .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
            .text("if true, reprocess everything from the beginning")

    }

    def parse(args: Seq[String]): CliArgs = {
        optionsParser
            .parse(args, initOptions)
            .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
    }

}
