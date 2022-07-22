package com.cxi.cdp.data_processing
package curated_zone.geo_identity.model

case class GeoIdentityRow(
    cxi_identity_id: String,
    `type`: String,
    weight: String,
    metadata: Map[String, String]
)
