package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity.model

import refined_zone.hub.identity.model.IdentityId

case class GeoLocationToOrderRow(
    cxi_partner_id: String,
    location_id: String,
    ord_id: String,
    ord_date: java.sql.Date,
    cxi_identity_ids: Seq[IdentityId],
    maid: String,
    maid_type: String,
    device_score: Option[Double]
)
