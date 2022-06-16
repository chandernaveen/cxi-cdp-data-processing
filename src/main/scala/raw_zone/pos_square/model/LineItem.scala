package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class LineItem(
    applied_discounts: Array[AppliedDiscount] = null,
    applied_taxes: Array[AppliedTax] = null,
    base_price_money: BasePriceMoney = null,
    catalog_object_id: String = null,
    catalog_version: Option[Long] = Option.empty,
    gross_sales_money: GrossSalesMoney = null,
    item_type: String = null,
    modifiers: Array[Modifier] = null,
    name: String = null,
    note: String = null,
    pricing_blocklists: PricingBlocklists = null,
    quantity: String = null,
    total_discount_money: TotalDiscountMoney = null,
    total_money: TotalMoney = null,
    total_tax_money: TotalTaxMoney = null,
    uid: String = null,
    variation_name: String = null,
    variation_total_price_money: VariationTotalPriceMoney = null
)

case class AppliedDiscount(applied_money: AppliedMoney = null, discount_uid: String = null, uid: String = null)

case class AppliedMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class BasePriceMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class GrossSalesMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class TotalMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class TotalDiscountMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class TotalTaxMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class TotalPriceMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class VariationTotalPriceMoney(amount: Option[Long] = Option.empty, currency: String = null)

case class PricingBlocklists(blocked_discounts: Array[BlockedDiscount] = null, blocked_taxes: Array[BlockedTax] = null)

case class BlockedDiscount(discount_catalog_object_id: String = null, uid: String = null)

case class BlockedTax(tax_catalog_object_id: String = null, uid: String = null)

case class AppliedTax(applied_money: AppliedMoney = null, tax_uid: String = null, uid: String = null)

case class Modifier(
    base_price_money: BasePriceMoney = null,
    catalog_object_id: String = null,
    catalog_version: Option[Long] = Option.empty,
    name: String = null,
    total_price_money: TotalPriceMoney = null,
    uid: String = null
)
