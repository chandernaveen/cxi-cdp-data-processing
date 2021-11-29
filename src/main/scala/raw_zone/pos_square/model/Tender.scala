package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class Tender(amount_money: AmountMoney = null,
                  card_details: CardDetails = null,
                  cash_details: CashDetails = null,
                  created_at: String = null,
                  customer_id: String = null,
                  id: String = null,
                  location_id: String = null,
                  payment_id: String = null,
                  processing_fee_money: ProcessingFeeMoney = null,
                  tip_money: TipMoney = null,
                  transaction_id: String = null,
                  `type`: String = null)

case class AmountMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class CardDetails(card: Card = null, entry_method: String = null, status: String = null)

case class Card(bin: String = null,
                card_brand: String = null,
                card_type: String = null,
                exp_month: Option[Long] = Option.empty,
                exp_year: Option[Long] = Option.empty,
                fingerprint: String = null,
                last_4: String = null,
                prepaid_type: String = null
               )

case class CashDetails(buyer_tendered_money: BuyerTenderedMoney = null, change_back_money: ChangeBackMoney = null)

case class BuyerTenderedMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class ChangeBackMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class ProcessingFeeMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class TipMoney(amount: Option[Long] = Option.empty, currency: String = null)

