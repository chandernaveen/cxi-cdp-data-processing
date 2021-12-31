package com.cxi.cdp.data_processing
package support.cleansing

import org.apache.spark.sql.functions.udf

object udfs {

    val cleanseZipCode = udf(LocationCleansing.cleanseZipCode _)

}
