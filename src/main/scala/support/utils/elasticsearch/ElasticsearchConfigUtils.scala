package com.cxi.cdp.data_processing
package support.utils.elasticsearch

import support.WorkspaceConfigReader
import support.utils.ContractUtils

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

import java.net.URL

object ElasticsearchConfigUtils {

    def getElasticsearchConfig
        (spark: SparkSession, contract: ContractUtils, esIndex: String, elasticsearchProps: ElasticsearchProps = ElasticsearchProps()): ElasticsearchConfig = {

        val workspaceConfigPath: String = contract.prop[String](elasticsearchProps.workspaceConfigPath)
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val baseUrl = new URL(DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, contract.prop[String](elasticsearchProps.baseUrlKey)))
        val apiKey = DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, contract.prop[String](elasticsearchProps.elasticsearchApiKeyKey))

        val esConfig = Map(
            ConfigurationOptions.ES_NODES -> new URL(baseUrl.getProtocol, baseUrl.getHost, baseUrl.getPath).toString,
            ConfigurationOptions.ES_PORT -> baseUrl.getPort.toString,
            ConfigurationOptions.ES_NODES_WAN_ONLY -> "true",
            "es.net.http.header.Authorization" -> apiKey
        )
        ElasticsearchConfig(esIndex, esConfig)
    }

    case class ElasticsearchProps(
       workspaceConfigPath: String = "databricks_workspace_config",
       baseUrlKey: String = "elasticsearch.base_url_key",
       elasticsearchApiKeyKey: String = "elasticsearch.apiKey_key"
    )

    case class ElasticsearchConfig(esIndex: String, esConfigMap: Map[String, String])

}
