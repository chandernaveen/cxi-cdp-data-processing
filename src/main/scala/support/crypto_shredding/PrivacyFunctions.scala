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

object PrivacyFunctions {

    /** Executes `body` in the authorized context,
      * allowing it to access restricted data such as the privacy lookup table.
      *
      * Ensures that authorization settings are unset in case of a failure.
      */
    def inAuthorizedContext[T](spark: SparkSession, workspaceConfig: WorkspaceConfig)(body: => T): T = {
        val privacyFunctions = new PrivacyFunctions(spark, workspaceConfig)
        try {
            privacyFunctions.authorize()
            body
        } finally {
            privacyFunctions.unauthorize()
        }
    }

}
