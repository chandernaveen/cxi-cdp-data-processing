package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.WorkspaceConfig

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession

/***
 * https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage#access-azure-blob-storage-using-the-dataframe-api
 */
// TODO: Consider switching table location from Blob Storage
//  to ADLS Gen2: https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sas-access
class PrivacyFunctions(spark: SparkSession, workspaceConfig: WorkspaceConfig) {
    private val storageAccount = workspaceConfig.privacyStorageAccount
    private val containerName = workspaceConfig.privacyContainerName

    def authorize() {
        spark.conf.set(getSparkConfStorageAccountKey(storageAccount), getSparkConfStorageAccountValue)
        spark.conf.set(getSparkConfContainerSasKey(storageAccount, containerName), getSparkConfContainerSasValue)
    }

    def unauthorize(): Unit = {
        spark.conf.unset(getSparkConfStorageAccountKey(storageAccount))
        spark.conf.unset(getSparkConfContainerSasKey(storageAccount, containerName))
    }

    private def getSparkConfStorageAccountKey(storageAccount: String): String =
        s"fs.azure.account.key.$storageAccount.blob.core.windows.net"

    private def getSparkConfContainerSasKey(storageAccount: String, containerName: String): String =
        s"fs.azure.sas.$containerName.$storageAccount.blob.core.windows.net"

    private def getSparkConfStorageAccountValue: String =
        DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, "sp-crypto-rw-secret")

    private def getSparkConfContainerSasValue: String =
        DBUtils.secrets.get(workspaceConfig.azureKeyVaultScopeName, "sp-crypto-rw-sas-secret")

}
