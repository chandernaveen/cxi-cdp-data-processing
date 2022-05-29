package com.cxi.cdp.data_processing
package support

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.spark.sql.SparkSession

object WorkspaceConfigReader {
    def readWorkspaceConfig(spark: SparkSession, configPath: String): WorkspaceConfig = {
        val globalConfigDetails = spark.sparkContext.wholeTextFiles(configPath).collect.take(1)(0)._2
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        WorkspaceConfig(mapper.readValue(globalConfigDetails, classOf[Map[String, String]]))
    }
}

case class WorkspaceConfig(config: Map[String, String]) {
    def env: String = config("envType")
    def region: String = config("region")
    def productName: String = config("productName")
    def azureKeyVaultScopeName: String = s"$env-$region-keyvault-scope"
    def privacyContainerName: String = "data-privacy" // TODO: move to workspace config
    def privacyStorageAccount: String = s"dls2$env$region$productName"
}
