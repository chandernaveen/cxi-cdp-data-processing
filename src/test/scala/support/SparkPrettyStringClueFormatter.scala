package com.cxi.cdp.data_processing
package support

import org.apache.spark.sql.Row

object SparkPrettyStringClueFormatter {

    def toPrettyString(data: Array[Row],
                       prefix: String = "\nActual data:\n",
                       delim: String = "\n-----------------------\n",
                       suffix: String = "\n\n"): String = {
        data.mkString(prefix + delim, delim, suffix)
    }

}
