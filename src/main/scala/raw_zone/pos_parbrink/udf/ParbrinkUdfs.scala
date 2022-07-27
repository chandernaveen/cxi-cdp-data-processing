package com.cxi.cdp.data_processing
package raw_zone.pos_parbrink.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object ParbrinkUdfs {

    final val FileNameDelimiter = "_"
    final val FilePathDelimiter = "/"
    final val LocationPrefix = "location="

    def recordTypeByFilePath: UserDefinedFunction = {
        udf((filePath: String) => {
            val fileName = filePath.split(FilePathDelimiter).last
            fileName.substring(0, fileName.indexOf(FileNameDelimiter))
        })
    }

    def locationByFilePath: UserDefinedFunction = {
        udf((filePath: String) => {
            val locationPart = filePath.split(FilePathDelimiter).head
            locationPart.replace(LocationPrefix, "")
        })
    }

}
