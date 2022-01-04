package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing

import support.WorkspaceConfig

import com.databricks.service.DBUtils

object Salt {
    def getSalt(saltScope: String, saltKey: String): String = {
        try {
            DBUtils.secrets.get(saltScope, saltKey)
        } catch {
            case e: Exception =>
                throw new RuntimeException(
                    s"Error getting salt with scope: $saltScope and key: $saltKey", e)
        }
    }

    def getSalt(workspaceConfig: WorkspaceConfig): String = {
        getSalt(workspaceConfig.azureKeyVaultScopeName, "cxi-int-cryptoShredding-salt")
    }
}
