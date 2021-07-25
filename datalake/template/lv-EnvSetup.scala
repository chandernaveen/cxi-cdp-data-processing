// Databricks notebook source
print("Hello World!")

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC mkdirs "/databricks/config/"

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC ls "/databricks/config/"

// COMMAND ----------

//Upload the file via "Data" Side-menu, remember to go to admin, workspace setting and enable dbfs navigation

// COMMAND ----------

dbutils.fs.put("/databricks/config/workspace_details.json", """
{"envType":"dev","region":"eastus2","sevicePrincipleClientId":"9851855e-dadf-4288-8746-d5111450c638","sevicePrincipleTenantId": "b3cbe2ef-48de-466e-ab1a-167995bc057b","productName":"cxi"}
""", true)

// COMMAND ----------

val df = spark.read.json("/databricks/config/workspace_details.json")

// COMMAND ----------

display(df)

// COMMAND ----------

//After creating scope, test it quickly

val kvSecretScope = "dev-eastus2-keyvault-scope"

// COMMAND ----------

val test = dbutils.secrets.get(scope = kvSecretScope, key = "sp-storage-rw-secret") 
