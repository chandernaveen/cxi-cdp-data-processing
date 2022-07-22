package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity.model

case class GeoLocationRow(
    cxi_partner_id: String,
    location_id: String,
    maid: String,
    maid_type: String,
    latitude: Double,
    longitude: Double,
    horizontal_accuracy: Double,
    geo_timestamp: java.sql.Timestamp,
    geo_date: java.sql.Date,
    distance_to_store: Double,
    max_distance_to_store: Double
)
