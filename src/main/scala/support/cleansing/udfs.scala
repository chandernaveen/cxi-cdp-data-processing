package com.cxi.cdp.data_processing
package support.cleansing

import org.apache.spark.sql.functions.udf

object udfs { // scalastyle:ignore object.name

    val cleanseZipCode = udf(LocationCleansing.cleanseZipCode _)

}
