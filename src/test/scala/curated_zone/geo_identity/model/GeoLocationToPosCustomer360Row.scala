package com.cxi.cdp.data_processing
package curated_zone.geo_identity.model

case class GeoLocationToPosCustomer360Row(
    maid: String,
    maid_type: String,
    customer_360_id: String,
    frequency_linked: Option[Int],
    profile_maid_link_score: Option[Double],
    total_profiles_per_maid: Option[Long],
    total_score_per_maid: Option[Double],
    total_maids_per_profile: Option[Long],
    total_score_per_profile: Option[Double],
    confidence_score: Option[Double]
)
