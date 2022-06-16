package com.cxi.cdp.data_processing
package curated_zone.audience.model

import Customer360._

case class Customer360(
    customer_360_id: String,
    identities: Map[IdentityType, Seq[CxiIdentityId]],
    create_date: java.sql.Date,
    update_date: java.sql.Date,
    active_flag: Boolean
)

object Customer360 {
    type IdentityType = String
    type CxiIdentityId = String
}
