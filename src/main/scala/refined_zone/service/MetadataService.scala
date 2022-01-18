package com.cxi.cdp.data_processing
package refined_zone.service

object MetadataService {

    // scalastyle:off magic.number
    def extractMetadata(identityType: String, originalValue: String): Map[String, String] = {
        if (originalValue == null) {
            null
        } else {
            identityType match {
                case "phone" => extractPhoneAreaCode(originalValue)
                case "email" => extractEmailDomain(originalValue)
                case _ => null
            }
        }
    }

    private def extractPhoneAreaCode(phone: String): Map[String, String] = {
        if (phone.length < 4) {
            null
        } else {
            Map("phone_area_code" -> phone.substring(0, 4))
        }
    }

    private def extractEmailDomain(email: String): Map[String, String] = {
        val index = email.indexOf("@")
        if (index == -1 || email.endsWith("@")) {
            null
        } else {
            Map("email_domain" -> email.substring(email.indexOf("@") + 1))
        }
    }

}
