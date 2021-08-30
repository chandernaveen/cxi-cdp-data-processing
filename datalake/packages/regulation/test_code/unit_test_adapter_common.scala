// Databricks notebook source
// MAGIC %run "/datalake/functions/unified_framework_functions"

// COMMAND ----------

// MAGIC %run "datalake/packages/utils/contract_utils"

// COMMAND ----------

// MAGIC %run "./../hashing_adapter"

// COMMAND ----------

// MAGIC %run "../../utils/privacy_functions"

// COMMAND ----------

import com.cxi.regulation.classes.adapter.IngestionHashAdapter
import com.cxi.utils.ContractUtils
import org.apache.spark.sql.DataFrame

// COMMAND ----------

val np = new ContractUtils(java.nio.file.Paths.get("/mnt/metadata/template/contracts/landing_raw_contract.json"))


// COMMAND ----------

val hashSpecs: Option[Map[String, Any]] = np.propOrNone[Map[String, Any]]("crypto")

// COMMAND ----------

def applyCryptoHashIfNeeded(hashSpecs: Option[Map[String, Any]], kvScope:String, df: DataFrame): DataFrame = {
  if (hashSpecs.isEmpty) df else IngestionHashAdapter.hashDf(hashSpecs.get, kvScope, df, true)
}

// COMMAND ----------

val srcDF = spark.read.format("delta").load("/mnt/raw_zone/cxi/template/test/test_products/")

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

//test1 with our salt expected value: 28173b39a2ab3cd1c684cb07fbc4483ad863e8316359c1c09967dc05383628c3
//test1 with our salt received value: 28173b39a2ab3cd1c684cb07fbc4483ad863e8316359c1c09967dc05383628c3
//Pass!!

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select * from privacy.lookup_table

// COMMAND ----------

unauthorize("fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select * from privacy.lookup_table