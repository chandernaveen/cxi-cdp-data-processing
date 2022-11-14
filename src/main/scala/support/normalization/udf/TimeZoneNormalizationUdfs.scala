package com.cxi.cdp.data_processing
package support.normalization.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.zone.ZoneRulesException
import java.time.ZoneId

case object TimeZoneNormalizationUdfs extends NormalizationUdfs {

    /** Create user-defined function to validate time-zone. Generally it do the same as
      * `ZoneId.of(timezone).normalized()` but add checks that zone have no format in
      * time offset (like GMT+2)
      * @return string of valid timezone
      * @throws ZoneRulesException when input cannot accept known zones
      */
    def normalizeTimezone: UserDefinedFunction =
        udf((timezone: String) => ZoneId.of(timezone).normalized().toString)

}
