package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class Tax(applied_money: TaxAppliedMoney = null,
               catalog_object_id: String = null,
               catalog_version: Option[Long] = Option.empty,
               name: String = null,
               percentage: String = null,
               scope: String = null,
               `type`: String = null,
               uid: String = null)

case class TaxAppliedMoney(amount: Option[Long] = Option.empty, currency: String = null)
