package com.cxi.cdp.data_processing
package curated_zone.audience.model

import enumeratum.values._

case class SignalUniverse(
    domain_name: String,
    ui_domain_label: String,
    signal_name: String,
    ui_signal_label: String,
    es_signal_name: String,
    es_type: String,
    signal_description: String,
    ui_type: String,
    create_date: java.sql.Date,
    update_date: java.sql.Date,
    is_active: Boolean,
    ui_is_signal_exposed: Boolean,
    signal_type: String,
    es_field_parent_section_name: String
)

sealed abstract class SignalType(val value: String) extends StringEnumEntry with Serializable {
    def code: String = value
}

object SignalType extends StringEnum[SignalType] {

    case object GeneralCustomerSignal extends SignalType("general_customer_signal")
    case object SpecificPartnerLocationSignal extends SignalType("specific_partner_location_signal")

    val values = findValues
}

