package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.WorkspaceConfig

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession

class PrivacyFunctions(spark: SparkSession, workspaceConfig: WorkspaceConfig) {
    private val storageAccount = workspaceConfig.privacyStorageAccount

    // TODO: to be deleted in the scope of https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
    private val containerName = workspaceConfig.privacyContainerName

    def authorize() {
        spark.conf.set(getAdlsStorageAccountKey(storageAccount), getSparkConfStorageAccountValue)

        // TODO: 2 lines below to be deleted in the scope of https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
        spark.conf.set(getSparkConfStorageAccountKey(storageAccount), getSparkConfStorageAccountValue)
        spark.conf.set(getSparkConfContainerSasKey(storageAccount, containerName), getSparkConfContainerSasValue)
    }

    def unauthorize(): Unit = {
        spark.conf.unset(getAdlsStorageAccountKey(storageAccount))

        // TODO: 2 lines below to be deleted in the scope of https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
        spark.conf.unset(getSparkConfStorageAccountKey(storageAccount))
        spark.conf.unset(getSparkConfContainerSasKey(storageAccount, containerName))
    }

    private def getAdlsStorageAccountKey(storageAccount: String): String =
        s"fs.azure.account.key.$storageAccount.dfs.core.windows.net"

    private def getSparkConfStorageAccountValue: String =
        DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, "cxi-int-cryptoShredding-storageAccountAccessKey")

    // TODO: 3 methods below to be deleted in the scope of https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
    private def getSparkConfStorageAccountKey(storageAccount: String): String =
        s"fs.azure.account.key.$storageAccount.blob.core.windows.net"

    private def getSparkConfContainerSasKey(storageAccount: String, containerName: String): String =
        s"fs.azure.sas.$containerName.$storageAccount.blob.core.windows.net"

    private def getSparkConfContainerSasValue: String =
        DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, "cxi-int-cryptoShredding-sasContainerQueryString")

}
