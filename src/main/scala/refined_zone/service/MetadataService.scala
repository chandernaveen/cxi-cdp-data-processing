package com.cxi.cdp.data_processing
package refined_zone.service

object MetadataService {

    // scalastyle:off magic.number
    def extractMetadata(identityType: String, originalValue: String): Map[String, String] = {
        if (originalValue == null) {
            Map.empty[String, String]
        } else {
            identityType match {
                case "phone" => extractPhoneAreaCode(originalValue)
                case "email" => extractEmailDomain(originalValue)
                case _ => Map.empty[String, String]
            }
        }
    }

    private def extractPhoneAreaCode(phone: String): Map[String, String] = {
        if (phone == null || phone.length < 4) {
            Map.empty[String, String]
        } else {
            Map("phone_area_code" -> phone.substring(0, 4))
        }
    }

    private def extractEmailDomain(email: String): Map[String, String] = {
        if (email == null) {
            Map.empty[String, String]
        } else {
            val index = email.indexOf("@")
            if (index == -1 || email.endsWith("@")) {
                Map.empty[String, String]
            } else {
                Map("email_domain" -> email.substring(index + 1))
            }
        }
    }

}
