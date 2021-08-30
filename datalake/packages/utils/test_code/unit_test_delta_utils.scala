// COMMAND ----------

// MAGIC %run "../delta_utils"

// COMMAND ----------

import com.cxi.utils.DeltaTableFunctions

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC ls "/mnt/raw_zone/cxi/pos_simphony/lab/all_record_types/"

// COMMAND ----------

//Function tableExists (True)

if (DeltaTableFunctions.tableExists("logs.application_audit_logs")) print("Yep") else print("Nop")


// COMMAND ----------

//Function tableExists (False)

if (DeltaTableFunctions.tableExists("lv.application_audit_logs")) print("Yep") else print("Nop")


// COMMAND ----------

display(DeltaTableFunctions.getTableDescribe("logs.application_audit_logs"))

// COMMAND ----------

import java.nio.file.{Path, Paths}

// COMMAND ----------

val path = Paths.get("/mnt/raw_zone/cxi/pos_simphony/lab/all_record_types/")

display(DeltaTableFunctions.getTableDescribe(path))

// COMMAND ----------

display(DeltaTableFunctions.getTableName(path)._1)