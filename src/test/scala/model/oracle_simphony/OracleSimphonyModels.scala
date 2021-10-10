package com.cxi.cdp.data_processing
package model.oracle_simphony

case class Employee(employeeId: Option[Long] = Option.empty,
                    fName: String = null,
                    homeLocRef: String = null,
                    lName: String = null,
                    num: Option[Long] = Option.empty,
                    payrollId: String = null)

case class ServiceChargeNum(svcChgNum: Option[Long] = Option.empty)

case class DetailLineTenderMedia(tmedNum: Option[Long] = Option.empty)

case class GuestCheck(balDueTtl: String = null,
                      chkNum: Option[Long] = Option.empty,
                      chkTtl: Option[Double] = Option.empty,
                      clsdBusDt: String = null,
                      clsdFlag: Option[Boolean] = Option.empty,
                      clsdUTC: String = null,
                      detailLines: Array[DetailLine] = null,
                      dscTtl: Option[Long] = Option.empty,
                      empNum: Option[Long] = Option.empty,
                      gstCnt: Option[Long] = Option.empty,
                      guestCheckId: Option[Long] = Option.empty,
                      lastUpdatedUTC: String = null,
                      nonTxblSlsTtl: String = null,
                      opnBusDt: String = null,
                      opnUTC: String = null,
                      otNum: Option[Long] = Option.empty,
                      payTtl: Option[Double] = Option.empty,
                      rvcNum: Option[Long] = Option.empty,
                      subTtl: Option[Double] = Option.empty,
                      svcChgTtl: Option[Double] = Option.empty)

case class DetailLineMenuItem(activeTaxes: String = null,
                              miNum: Option[Long] = Option.empty,
                              prcLvl: Option[Long] = Option.empty)

case class Other(detailNum: Option[Long] = Option.empty,
                 detailType: Option[Long] = Option.empty)

case class DetailLine(aggQty: Option[Long] = Option.empty,
                      aggTtl: Option[Double] = Option.empty,
                      busDt: String = null,
                      chkEmpId: Option[Long] = Option.empty,
                      detailUTC: String = null,
                      dspQty: Option[Long] = Option.empty,
                      dspTtl: Option[Double] = Option.empty,
                      dtlId: Option[Long] = Option.empty,
                      guestCheckLineItemId: Option[Long] = Option.empty,
                      lineNum: Option[Long] = Option.empty,
                      menuItem: MenuItem = null,
                      other: Other = null,
                      parDtlId: Option[Long] = Option.empty,
                      refInfo1: String = null,
                      serviceCharge: ServiceChargeNum = null,
                      svcRndNum: Option[Long] = Option.empty,
                      tenderMedia: TenderMedia = null,
                      wsNum: Option[Long] = Option.empty
                     )

case class Workstation(wsName: String = null, wsNum: Option[Long] = Option.empty)

case class Location(active: Option[Boolean] = Option.empty,
                    locRef: String = null,
                    name: String = null,
                    openDt: String = null,
                    srcName: String = null,
                    srcVer: String = null,
                    tz: String = null,
                    workstations: Array[Workstation] = null)

case class MenuItemPrice(effFrDt: String = null,
                         num: Option[Long] = Option.empty,
                         prcLvlName: String = null,
                         prcLvlNum: Option[Long] = Option.empty,
                         price: Option[Double] = Option.empty,
                         rvcNum: Option[Long] = Option.empty)

case class MenuItem(famGrpName: String = null,
                    famGrpNum: Option[Long] = Option.empty,
                    majGrpName: String = null,
                    majGrpNum: Option[Long] = Option.empty,
                    name: String = null,
                    name2: String = null,
                    num: Option[Long] = Option.empty)

case class NonSalesTransaction(chkNum: Option[Long] = Option.empty,
                               empNum: Option[Long] = Option.empty,
                               guestCheckId: String = null,
                               rvcNum: Option[Long] = Option.empty,
                               transType: Option[Long] = Option.empty,
                               transUTC: String = null,
                               value: Option[Double] = Option.empty,
                               wsNum: Option[Long] = Option.empty)

case class OrderType(name: String = null, num: Option[Long] = Option.empty)

case class ReasonCode(name: String = null, num: Option[Long] = Option.empty)

case class LogDetail(empNum: Option[Long] = Option.empty,
                     guestCheckId: Option[Long] = Option.empty,
                     journalTxt: String = null,
                     transId: Option[Long] = Option.empty,
                     transUTC: String = null,
                     `type`: Option[Long] = Option.empty,
                     wsNum: Option[Long] = Option.empty)

case class RevenueCenter(logDetails: Array[LogDetail] = null,
                         name: String = null,
                         num: Option[Long] = Option.empty,
                         rvcNum: Option[Long] = Option.empty)

case class ServiceCharge(chrgTipsFlag: Option[Boolean] = Option.empty,
                         name: String = null,
                         num: Option[Long] = Option.empty,
                         posPercent: Option[Long] = Option.empty,
                         revFlag: Option[Boolean] = Option.empty)

case class Tax(name: String = null,
               num: Option[Long] = Option.empty,
               taxRate: Option[Long] = Option.empty,
               `type`: Option[Long] = Option.empty)

case class TenderMedia(name: String = null,
                       num: Option[Long] = Option.empty,
                       `type`: Option[Long] = Option.empty)

case class OracleSimphonyLabLandingModel(busDt: String = null,
                                         cashMgmtItems: Array[String] = null,
                                         cashiers: Array[String] = null,
                                         curUTC: String = null,
                                         discounts: Array[Discount] = null,
                                         employees: Array[Employee] = null,
                                         guestChecks: Array[GuestCheck] = null,
                                         jobCodes: Array[String] = null,
                                         latestBusDt: String = null,
                                         locRef: String = null,
                                         locations: Array[Location] = null,
                                         menuItemPrices: Array[MenuItemPrice] = null,
                                         menuItems: Array[MenuItem] = null,
                                         nonSalesTransactions: Array[NonSalesTransaction] = null,
                                         opnBusDt: String = null,
                                         orderTypes: Array[OrderType] = null,
                                         reasonCodes: Array[ReasonCode] = null,
                                         revenueCenters: Array[ReasonCode] = null,
                                         serviceCharges: Array[ServiceCharge] = null,
                                         taxes: Array[Tax] = null,
                                         tenderMedias: Array[TenderMedia] = null,
                                         feed_date: String = null,
                                         file_name: String = null,
                                         cxi_id: String = null)

case class OracleSimphonyLabRawModel(cur_utc: String = null,
                                     loc_ref: String = null,
                                     bus_dt: String = null,
                                     opn_bus_dt: String = null,
                                     latest_bus_dt: String = null,
                                     record_type: String = null,
                                     record_value: String = null,
                                     feed_date: String = null,
                                     cxi_id: String = null,
                                     file_name: String = null)

case class Discount(name: String = null,
                    num: Option[Long] = Option.empty,
                    posPercent: Option[Long] = Option.empty)
