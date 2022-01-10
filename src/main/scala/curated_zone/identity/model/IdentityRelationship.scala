package com.cxi.cdp.data_processing
package curated_zone.identity.model

case class IdentityRelationship(
                                      source: String,
                                      source_type: String,
                                      target: String,
                                      target_type: String,
                                      relationship: String,
                                      frequency: Int,
                                      created_date: java.sql.Date,
                                      last_seen_date: java.sql.Date,
                                      active_flag: Boolean
                                  )
