// Databricks notebook source
// MAGIC %run "/datalake/functions/unified_framework_functions"

// COMMAND ----------

// MAGIC %run "/datalake/packages/utils/contract_utils"

// COMMAND ----------

// MAGIC %run "../hashing_adapter"

// COMMAND ----------

// MAGIC %run "/datalake/packages/utils/privacy_functions"

// COMMAND ----------

import com.cxi.regulation.classes.adapter.IngestionHashAdapter
import com.cxi.utils.ContractUtils
import org.apache.spark.sql.DataFrame

// COMMAND ----------

val np = new ContractUtils(java.nio.file.Paths.get("/mnt/metadata/template/contracts/template_crypto_json_contract.json"))


// COMMAND ----------

val hashSpecs: Option[Map[String, Any]] = np.propOrNone[Map[String, Any]]("crypto")

// COMMAND ----------

def applyCryptoHashIfNeeded(hashSpecs: Option[Map[String, Any]], kvScope:String, df: DataFrame): DataFrame = {
  //if (hashSpecs.isEmpty) df else IngestionHashAdapter.hashDf(hashSpecs.get, kvScope, df, false)
  if (hashSpecs.isEmpty) df else IngestionHashAdapter.hashDf(hashSpecs.get, kvScope, df, true)
}

// COMMAND ----------

val srcDF =  spark.read.format("delta").load("/mnt/raw_zone/cxi/pos_simphony/lab/all_record_types").filter($"record_type" === "guestChecks").select($"cxi_id", $"record_value").limit(10)

// COMMAND ----------

display(srcDF)

// COMMAND ----------

val kvScope = "dev-eastus2-keyvault-scope"

// COMMAND ----------

//Authorize First cause you are writting into the Lookup
authorize()

// COMMAND ----------

val cryptoHashedDf = applyCryptoHashIfNeeded(hashSpecs, kvScope, srcDF)

// COMMAND ----------

display(cryptoHashedDf)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from privacy.lookup_table

// COMMAND ----------

//Authorize First cause you are writting into the Lookup
unauthorize("fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from privacy.lookup_table
