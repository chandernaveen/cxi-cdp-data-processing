package com.cxi.cdp.data_processing
package model.qu_beyond

case class Data(
    cash_deposits: Array[String] = null,
    category: Array[Category] = null,
    check: Array[Check] = null,
    discount: Array[Discount] = null,
    employee: Array[Employee] = null,
    item: Array[Item] = null,
    item_group: Array[ItemGroup] = null,
    job: Array[Job] = null,
    labor: Array[Labor] = null,
    location: Array[Location] = null,
    order_channel: Array[OrderChannel] = null,
    order_type: Array[OrderType] = null,
    over_under: Option[Double] = Option.empty,
    paid_in_total: Option[Long] = Option.empty,
    paid_inout: Array[PaidInout] = null,
    paid_inouts: Array[PaidInouts] = null,
    paid_out_total: Option[Long] = Option.empty,
    payment_type: Array[PaymentType] = null,
    payroll_profile: Array[PayrollProfile] = null,
    portion: Array[Portion] = null,
    service_charge: Array[ServiceCharge] = null,
    tax: Array[Tax] = null,
    tills: Array[Tills] = null,
    time_entry_reasons: Array[TimeEntryReasons] = null,
    void: Array[Void] = null
)

case class ItemPrice(base: Option[Double] = Option.empty)

case class Item(
    barcode: String = null,
    id: Option[Long] = Option.empty,
    name: String = null,
    plu: String = null,
    price: ItemPrice = null
)

case class Discount(
    check_title: String = null,
    discount_category: String = null,
    id: Option[Long] = Option.empty,
    is_deleted: Option[Boolean] = Option.empty,
    modified_at: String = null,
    name: String = null
)

case class Category(id: Option[Long] = Option.empty, name: String = null)

case class Employee(
    clock_in_required: Option[Boolean] = Option.empty,
    email: String = null,
    employee_number: String = null,
    first_name: String = null,
    hire_date: String = null,
    id: Option[Long] = Option.empty,
    job: Array[EmployeeJob] = null,
    last_name: String = null,
    mag_stripe_card_id: String = null,
    pin: String = null
)

case class EmployeeJob(
    end_date: String = null,
    id: Option[Long] = Option.empty,
    name: String = null,
    payroll_profile_id: Option[Long] = Option.empty,
    start_date: String = null,
    wage: Option[Double] = Option.empty
)

case class Check(
    add_on_tax: Option[Double] = Option.empty,
    auto_gratuity: Option[Long] = Option.empty,
    business_date: String = null,
    business_time: String = null,
    cash_tip: Option[Long] = Option.empty,
    charged_tip: Option[Long] = Option.empty,
    check_guid: String = null,
    check_id: String = null,
    check_number: String = null,
    check_state: String = null,
    closed_at: String = null,
    discount: Array[CheckDiscount] = null,
    discount_total: Option[Double] = Option.empty,
    employee_id: Option[Long] = Option.empty,
    gift_cards_sold: Option[Long] = Option.empty,
    item: Array[CheckItem] = null,
    last_modified_at: String = null,
    location_id: Option[Long] = Option.empty,
    opened_at: String = null,
    order_channel_id: Option[Long] = Option.empty,
    order_type_id: Option[Long] = Option.empty,
    payment: Array[Payment] = null,
    payment_refund: Array[PaymentRefund] = null,
    previous_instances: Array[PreviousInstances] = null,
    reopened_by_id: String = null,
    service_charge: Array[CheckServiceCharge] = null,
    service_charge_total: Option[Long] = Option.empty,
    tax: Array[CheckTax] = null,
    tax_exempt: Option[Double] = Option.empty,
    tax_exempt_ref: String = null,
    terminal_id: Option[Long] = Option.empty,
    total: Option[Double] = Option.empty,
    void_total: Option[Double] = Option.empty
)

case class PreviousInstances(
    add_on_tax: Option[Double] = Option.empty,
    auto_gratuity: Option[Long] = Option.empty,
    business_date: String = null,
    business_time: String = null,
    cash_tip: Option[Long] = Option.empty,
    charged_tip: Option[Long] = Option.empty,
    check_guid: String = null,
    check_id: String = null,
    check_number: String = null,
    check_state: String = null,
    closed_at: String = null,
    discount_total: Option[Long] = Option.empty,
    employee_id: Option[Long] = Option.empty,
    gift_cards_sold: Option[Long] = Option.empty,
    item: Array[PreviousInstancesItem] = null,
    last_modified_at: String = null,
    location_id: Option[Long] = Option.empty,
    opened_at: String = null,
    order_channel_id: Option[Long] = Option.empty,
    order_type_id: Option[Long] = Option.empty,
    payment: Array[Payment] = null,
    reopened_at: String = null,
    service_charge_total: Option[Long] = Option.empty,
    tax: Array[CheckTax] = null,
    tax_exempt: Option[Long] = Option.empty,
    terminal_id: Option[Long] = Option.empty,
    total: Option[Double] = Option.empty,
    void_total: Option[Long] = Option.empty
)

case class PreviousInstancesItem(
    amount: Option[Double] = Option.empty,
    category_id: Option[Long] = Option.empty,
    is_return: Option[Boolean] = Option.empty,
    is_void: Option[Boolean] = Option.empty,
    item_id: Option[Long] = Option.empty,
    price: Option[Double] = Option.empty,
    quantity: Option[Long] = Option.empty,
    tax: Array[CheckTax] = null
)

case class Payment(
    auto_gratuity: Option[Long] = Option.empty,
    cash_tip: Option[Long] = Option.empty,
    change: Option[Long] = Option.empty,
    charged_tip: Option[Long] = Option.empty,
    credit_card_type: String = null,
    payment_time: String = null,
    payment_type_id: String = null,
    received: Option[Double] = Option.empty,
    tendered: Option[Double] = Option.empty,
    till_id: String = null,
    tip_processing_fee: Option[Long] = Option.empty,
    total: Option[Double] = Option.empty
)

case class PaymentRefund(
    auto_gratuity: Option[Long] = Option.empty,
    cash_tip: Option[Long] = Option.empty,
    change: Option[Long] = Option.empty,
    charged_tip: Option[Long] = Option.empty,
    payment_time: String = null,
    payment_type_id: String = null,
    received: Option[Double] = Option.empty,
    tendered: Option[Long] = Option.empty,
    till_id: String = null,
    tip_processing_fee: Option[Long] = Option.empty,
    total: Option[Double] = Option.empty
)

case class CheckDiscount(
    amount: Option[Double] = Option.empty,
    applied_at: String = null,
    discount_id: Option[Long] = Option.empty,
    quantity: Option[Long] = Option.empty
)

case class CheckChildItem(
    amount: Option[Double] = Option.empty,
    child_item: Array[CheckChildItemLevel2] = null,
    is_return: Option[Boolean] = Option.empty,
    is_void: Option[Boolean] = Option.empty,
    item_id: Option[Long] = Option.empty,
    portion_id: Option[Long] = Option.empty,
    price: Option[Double] = Option.empty,
    quantity: Option[Long] = Option.empty,
    tax: Array[CheckTax] = null
)

case class CheckChildItemLevel2(
    amount: Option[Long] = Option.empty,
    is_return: Option[Boolean] = Option.empty,
    is_void: Option[Boolean] = Option.empty,
    item_id: Option[Long] = Option.empty,
    portion_id: Option[Long] = Option.empty,
    price: Option[Long] = Option.empty,
    quantity: Option[Long] = Option.empty,
    tax: Array[CheckTax] = null
)

case class CheckTax(
    amount: Option[Double] = Option.empty,
    exempted: Option[Boolean] = Option.empty,
    quantity: Option[Long] = Option.empty,
    tax_id: Option[Long] = Option.empty
)

case class CheckVoid(applied_at: String = null, approver_id: Option[Long] = Option.empty, void_id: String = null)

case class CheckItem(
    amount: Option[Double] = Option.empty,
    category_id: Option[Long] = Option.empty,
    child_item: Array[CheckChildItem] = null,
    discount: Array[CheckDiscount] = null,
    is_return: Option[Boolean] = Option.empty,
    is_void: Option[Boolean] = Option.empty,
    item_id: Option[Long] = Option.empty,
    price: Option[Double] = Option.empty,
    quantity: Option[Long] = Option.empty,
    service_charge: Array[CheckServiceCharge] = null,
    tax: Array[CheckTax] = null,
    void: CheckVoid = null
)

case class CheckServiceCharge(
    amount: Option[Double] = Option.empty,
    quantity: Option[Long] = Option.empty,
    service_charge_id: Option[Long] = Option.empty
)

case class ItemGroup(id: Option[Long] = Option.empty, items: Array[Long] = null, name: String = null)

case class Job(id: Option[Long] = Option.empty, name: String = null)

case class Labor(
    auto_clock_out: Option[Boolean] = Option.empty,
    employee_id: Option[Long] = Option.empty,
    end_time: String = null,
    hourly_rate: Option[Long] = Option.empty,
    hours_worked: Option[Long] = Option.empty,
    job_id: Option[Long] = Option.empty,
    labor_id: String = null,
    overtime_mins: Option[Long] = Option.empty,
    overtime_pay: Option[Long] = Option.empty,
    overtime_rate: Option[Long] = Option.empty,
    payroll_profile_id: Option[Long] = Option.empty,
    regular_hours: Option[Long] = Option.empty,
    start_time: String = null,
    time_entries: Array[LaborTimeEntries] = null,
    total_pay: Option[Long] = Option.empty
)

case class LaborTimeEntries(
    activity: String = null,
    adjustment_user_id: String = null,
    creation_user_id: String = null,
    date_time: String = null,
    date_time_created: String = null,
    date_time_modified: String = null,
    description: String = null
)

case class Location(
    address1: String = null,
    address2: String = null,
    city: String = null,
    dba_name: String = null,
    fax: String = null,
    id: Option[Long] = Option.empty,
    location_type: Option[Long] = Option.empty,
    name: String = null,
    parent_location_id: Option[Long] = Option.empty,
    phone: String = null,
    postal_code: String = null,
    start_time_since_midnight_in_min: Option[Long] = Option.empty,
    state: String = null,
    time_zone: String = null
)

case class OrderChannel(id: Option[Long] = Option.empty, name: String = null)
case class OrderType(id: Option[Long] = Option.empty, name: String = null)
case class PaidInout(id: String = null, name: String = null)
case class PaidInouts(
    amount: Option[Long] = Option.empty,
    applied_at: String = null,
    employee_id: Option[Long] = Option.empty,
    paid_inout_id: String = null,
    reason_text: String = null
)
case class PaymentType(
    id: String = null,
    is_deleted: Option[Boolean] = Option.empty,
    modified_at: String = null,
    name: String = null,
    payment_type_category: String = null
)

case class PayrollProfile(
    break_type: String = null,
    calculate_daily: Option[Boolean] = Option.empty,
    calculate_weekly: Option[Boolean] = Option.empty,
    daily_hours: Option[Long] = Option.empty,
    double_daily_hours: Option[Long] = Option.empty,
    double_overtime_multiplier: Option[Long] = Option.empty,
    id: Option[Long] = Option.empty,
    max_paid_break: Option[Long] = Option.empty,
    min_paid_break: Option[Long] = Option.empty,
    min_time_before_paid_break: Option[Long] = Option.empty,
    name: String = null,
    overtime_multiplier: Option[Double] = Option.empty,
    payroll_type: String = null,
    weekly_hours: Option[Long] = Option.empty
)

case class Portion(id: Option[Long] = Option.empty, name: String = null)

case class ServiceCharge(
    check_title: String = null,
    id: Option[Long] = Option.empty,
    is_deleted: Option[Boolean] = Option.empty,
    modified_at: String = null,
    name: String = null,
    service_charge_category: String = null
)

case class Tax(
    check_title: String = null,
    id: Option[Long] = Option.empty,
    is_deleted: Option[Boolean] = Option.empty,
    modified_at: String = null,
    name: String = null,
    tax_category: String = null
)

case class Tills(
    auto_closed: Option[Boolean] = Option.empty,
    cash_drawer_id: Option[Long] = Option.empty,
    claimed_employee_id: Option[Long] = Option.empty,
    claimed_employee_name: String = null,
    closed_employee_id: Option[Long] = Option.empty,
    closed_employee_name: String = null,
    ending_cash: Option[Double] = Option.empty,
    paid_ins: Array[PaidIns] = null,
    paid_outs: Array[PaidOuts] = null,
    reconciliation: Reconciliation = null,
    skims: Array[Skims] = null,
    starting_cash: Option[Long] = Option.empty,
    terminal_id: Option[Long] = Option.empty,
    till_claim_date_time: String = null,
    till_close_date_time: String = null,
    till_id: String = null
)

case class PaidIns(
    amount: Option[Long] = Option.empty,
    comment: String = null,
    date_time: String = null,
    employee_id: Option[Long] = Option.empty,
    ending_cash: Option[Long] = Option.empty,
    paid_inout_id: String = null,
    reason: String = null,
    starting_cash: Option[Long] = Option.empty
)

case class PaidOuts(
    amount: Option[Long] = Option.empty,
    comment: String = null,
    date_time: String = null,
    employee_id: Option[Long] = Option.empty,
    ending_cash: Option[Long] = Option.empty,
    paid_inout_id: String = null,
    reason: String = null,
    starting_cash: Option[Long] = Option.empty
)

case class Reconciliation(
    actual_cash: Option[Long] = Option.empty,
    date_time: String = null,
    expected_cash: Option[Double] = Option.empty,
    over_under: Option[Double] = Option.empty,
    tips: Option[Long] = Option.empty
)

case class Skims(
    amount: Option[Long] = Option.empty,
    date_time: String = null,
    employee_id: Option[Long] = Option.empty,
    ending_cash: Option[Long] = Option.empty,
    starting_cash: Option[Long] = Option.empty
)

case class TimeEntryReasons(id: String = null, name: String = null, `type`: String = null)

case class Void(id: String = null, name: String = null)

case class DataDelta(date: String = null, time: String = null)

case class QuBeyondLandingZoneModel(
    data: Data = null,
    data_delta: DataDelta = null,
    req_customer_id: Option[Long] = Option.empty,
    req_data_type: String = null,
    req_end_date: String = null,
    req_location_id: Option[Long] = Option.empty,
    req_start_date: String = null,
    req_sub_data_type: String = null,
    feed_date: String = null,
    file_name: String = null,
    cxi_id: String = null
)

case class QuBeyondRawZoneModel(
    record_type: String = null,
    record_value: String = null,
    feed_date: String = null,
    file_name: String = null,
    cxi_id: String = null,
    req_customer_id: Option[Long] = Option.empty,
    req_location_id: Option[Long] = Option.empty,
    req_data_type: String = null,
    req_sub_data_type: String = null,
    data_delta: DataDelta = null,
    req_start_date: String = null,
    req_end_date: String = null
)
