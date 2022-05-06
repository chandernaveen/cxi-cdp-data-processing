package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.WorkspaceConfig

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession

class PrivacyFunctions(spark: SparkSession, workspaceConfig: WorkspaceConfig) {
    private val storageAccount = workspaceConfig.privacyStorageAccount

    def authorize(): Unit = {
        spark.conf.set(getAdlsStorageAccountKey(storageAccount), getSparkConfStorageAccountValue)
    }

    def unauthorize(): Unit = {
        spark.conf.unset(getAdlsStorageAccountKey(storageAccount))
    }

    private def getAdlsStorageAccountKey(storageAccount: String): String =
        s"fs.azure.account.key.$storageAccount.dfs.core.windows.net"

    private def getSparkConfStorageAccountValue: String =
        DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, "cxi-int-cryptoShredding-storageAccountAccessKey")
}
