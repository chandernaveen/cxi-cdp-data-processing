package com.cxi.cdp.data_processing
package support.packages.utils

// TODO: change to regular test
object DeltaTableFunctionsTest {
    def run(): Unit = {
        // COMMAND ----------

        // MAGIC %run "../delta_utils"

        // COMMAND ----------

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

        println(DeltaTableFunctions.getTableDescribe("logs.application_audit_logs").mkString("Array(", ", ", ")"))

        // COMMAND ----------

        import java.nio.file.Paths

        // COMMAND ----------

        val path = Paths.get("/mnt/raw_zone/cxi/pos_simphony/lab/all_record_types/")

        println(DeltaTableFunctions.getTableDescribe(path).mkString("Array(", ", ", ")"))

        // COMMAND ----------

        println(DeltaTableFunctions.getTableName(path)._1)
    }
}
