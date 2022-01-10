package com.cxi.cdp.data_processing
package support.cleansing

object LocationCleansing {

    private final val zipCodeRegex = raw"(\d{5})".r
    private final val zipPlusFourCodeRegex = raw"(\d{5})-(\d{4})".r // ZIP+4 Code format

    def cleanseZipCode(zipCode: String): Option[String] = {
        zipCode match {
            case zipCodeRegex(cleansedZipCode) => Some(cleansedZipCode)
            case zipPlusFourCodeRegex(cleansedZipCode, _) => Some(cleansedZipCode)
            case _ => None
        }
    }

}
