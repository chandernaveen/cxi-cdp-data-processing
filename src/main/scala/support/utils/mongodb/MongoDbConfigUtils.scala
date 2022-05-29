package com.cxi.cdp.data_processing
package support.utils.mongodb

import support.utils.ContractUtils
import support.WorkspaceConfigReader

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession

object MongoDbConfigUtils {

    val MongoSparkConnectorClass = "com.mongodb.spark.sql.DefaultSource"

    def getMongoDbConfig(
        spark: SparkSession,
        contract: ContractUtils,
        mongoDbProps: MongoDbProps = MongoDbProps()
    ): MongoDbConfig = {
        val workspaceConfigPath: String = contract.prop[String](mongoDbProps.workspaceConfigPath)
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val username = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String](mongoDbProps.usernameSecretKey)
        )
        val password = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String](mongoDbProps.passwordSecretKey)
        )
        val scheme = contract.prop[String](mongoDbProps.schema)
        val host = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String](mongoDbProps.hostSecretKey)
        )

        MongoDbConfig(username = username, password = password, scheme = scheme, host = host)
    }

    case class MongoDbProps(
        workspaceConfigPath: String = "databricks_workspace_config",
        usernameSecretKey: String = "mongo.username_secret_key",
        passwordSecretKey: String = "mongo.password_secret_key",
        schema: String = "mongo.scheme",
        hostSecretKey: String = "mongo.host_secret_key"
    )

    case class MongoDbConfig(username: String, password: String, scheme: String, host: String) {
        def uri: String = s"$scheme$username:$password@$host"
    }

}
