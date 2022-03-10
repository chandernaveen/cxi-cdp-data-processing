package com.cxi.cdp.data_processing
package curated_zone.audience.model

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
    is_active: Boolean
)


