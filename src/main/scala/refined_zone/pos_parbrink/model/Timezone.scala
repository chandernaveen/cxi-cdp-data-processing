package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class Timezone(val value: Int, val desc: String, val name: String)
    extends IntEnumEntry
    with Serializable

// scalastyle:off magic.number
// scalastyle:off number.of.methods
// scalastyle:off number.of.types
object Timezone extends IntEnum[Timezone] {

    case object Hawaii extends Timezone(1, "Hawaiian Standard Time", "Pacific/Honolulu")
    case object Alaska extends Timezone(2, "Alaskan Standard Time", "America/Anchorage")
    case object Pacific extends Timezone(3, "Pacific Standard Time", "America/Los_Angeles")
    case object Arizona extends Timezone(4, "US Mountain Standard Time", "America/Phoenix")
    case object Mountain extends Timezone(5, "Mountain Standard Time", "America/Denver")
    case object Central extends Timezone(6, "Central Standard Time", "America/Chicago")
    case object Eastern extends Timezone(7, "Eastern Standard Time", "America/Cancun")
    case object GeorgetownLaPazManausSanJuan extends Timezone(8, "Atlantic Standard Time", "America/Halifax")
    case object CanadaCentral extends Timezone(9, "Canada Central Standard Time", "America/Winnipeg")
    case object AleutianStandard extends Timezone(10, "Aleutian Standard Time", "America/Adak")
    case object EasterIslandStandard extends Timezone(11, "Easter Island Standard Time", "Pacific/Easter")
    case object SaPacificStandard extends Timezone(12, "SA Pacific Standard Time", "America/Bogota")
    case object HaitiStandard extends Timezone(13, "Haiti Standard Time", "America/Port-au-Prince")
    case object UsEasternStandard extends Timezone(14, "US Eastern Standard Time", "America/New_York")
    case object NewfoundlandStandard extends Timezone(15, "Newfoundland Standard Time", "America/St_Johns")
    case object SaintPierreStandard extends Timezone(16, "Saint Pierre Standard Time", "America/Miquelon")
    case object MidAtlanticStandard extends Timezone(17, "Mid-Atlantic Standard Time", "America/Noronha")
    case object WestPacificStandard extends Timezone(18, "West Pacific Standard Time", "Pacific/Guam")
    case object DatelineStandard extends Timezone(19, "Dateline Standard Time", "Etc/GMT+12")
    case object UTCMinus11 extends Timezone(20, "UTC-11", "Pacific/Pago_Pago")
    case object MarquesasStandard extends Timezone(21, "Marquesas Standard Time", "Pacific/Marquesas")
    case object UTCMinus09 extends Timezone(22, "UTC-09", "Pacific/Gambier")
    case object PacificStandardMexico extends Timezone(23, "Pacific Standard Time (Mexico)", "America/Tijuana")
    case object UTCMinus08 extends Timezone(24, "UTC-08", "Pacific/Pitcairn")
    case object MountainStandardMexico extends Timezone(25, "Mountain Standard Time (Mexico)", "America/Hermosillo")
    case object CentralAmericaStandard extends Timezone(26, "Central America Standard Time", "America/Guatemala")
    case object CentralStandardMexico extends Timezone(27, "Central Standard Time (Mexico)", "America/Mexico_City")
    case object EasternStandardMexico extends Timezone(28, "Eastern Standard Time (Mexico)", "America/Cancun")
    case object CubaStandard extends Timezone(29, "Cuba Standard Time", "America/Havana")
    case object TurksAndCaicosStandard extends Timezone(30, "Turks And Caicos Standard Time", "America/Grand_Turk")
    case object ParaguayStandard extends Timezone(31, "Paraguay Standard Time", "America/Asuncion")
    case object VenezuelaStandard extends Timezone(32, "Venezuela Standard Time", "America/Caracas")
    case object CentralBrazilianStandard extends Timezone(33, "Central Brazilian Standard Time", "America/Cuiaba")
    case object SAWesternStandard extends Timezone(34, "SA Western Standard Time", "America/La_Paz")
    case object PacificSAStandard extends Timezone(35, "Pacific SA Standard Time", "America/Santiago")
    case object TocantinsStandard extends Timezone(36, "Tocantins Standard Time", "America/Araguaina")
    case object EastSouthAmericaStandard extends Timezone(37, "E. South America Standard Time", "America/Sao_Paulo")
    case object SAEasternStandard extends Timezone(38, "SA Eastern Standard Time", "America/Cayenne")
    case object ArgentinaStandard extends Timezone(39, "Argentina Standard Time", "America/Argentina/Buenos_Aires")
    case object GreenlandStandard extends Timezone(40, "Greenland Standard Time", "America/Nuuk")
    case object MontevideoStandard extends Timezone(41, "Montevideo Standard Time", "America/Montevideo")
    case object MagallanesStandard extends Timezone(42, "Magallanes Standard Time", "America/Punta_Arenas")
    case object BahiaStandard extends Timezone(43, "Bahia Standard Time", "America/Bahia")
    case object UTCMinus02 extends Timezone(44, "UTC-02", "America/Noronha")
    case object AzoresStandard extends Timezone(45, "Azores Standard Time", "Atlantic/Azores")
    case object CapeVerdeStandard extends Timezone(46, "Cape Verde Standard Time", "Atlantic/Cape_Verde")
    case object UTC extends Timezone(47, "UTC", "UTC")
    case object GMTStandard extends Timezone(48, "GMT Standard Time", "Europe/London")
    case object GreenwichStandard extends Timezone(49, "Greenwich Standard Time", "Etc/Greenwich")
    case object SaoTomeStandard extends Timezone(50, "Sao Tome Standard Time", "Africa/Sao_Tome")
    case object MoroccoStandard extends Timezone(51, "Morocco Standard Time", "Africa/Casablanca")
    case object WestEuropeStandard extends Timezone(52, "W. Europe Standard Time", "WET")
    case object CentralEuropeStandard extends Timezone(53, "Central Europe Standard Time", "Europe/Budapest")
    case object RomanceStandard extends Timezone(54, "Romance Standard Time", "Europe/Paris")
    case object CentralEuropeanStandard extends Timezone(55, "Central European Standard Time", "Europe/Warsaw")
    case object WestCentralAfricaStandard extends Timezone(56, "W. Central Africa Standard Time", "Africa/Lagos")
    case object JordanStandard extends Timezone(57, "Jordan Standard Time", "Asia/Amman")
    case object GTBStandard extends Timezone(58, "GTB Standard Time", "Europe/Bucharest")
    case object MiddleEastStandard extends Timezone(59, "Middle East Standard Time", "Asia/Beirut")
    case object EgyptStandard extends Timezone(60, "Egypt Standard Time", "Africa/Cairo")
    case object EastEuropeStandard extends Timezone(61, "E. Europe Standard Time", "Europe/Chisinau")
    case object SyriaStandard extends Timezone(62, "Syria Standard Time", "Asia/Damascus")
    case object WestBankStandard extends Timezone(63, "West Bank Standard Time", "Asia/Hebron")
    case object SouthAfricaStandard extends Timezone(64, "South Africa Standard Time", "Africa/Johannesburg")
    case object FLEStandard extends Timezone(65, "FLE Standard Time", "Europe/Kiev")
    case object IsraelStandard extends Timezone(66, "Israel Standard Time", "Asia/Jerusalem")
    case object KaliningradStandard extends Timezone(67, "Kaliningrad Standard Time", "Europe/Kaliningrad")
    case object SudanStandard extends Timezone(68, "Sudan Standard Time", "Africa/Khartoum")
    case object LibyaStandard extends Timezone(69, "Libya Standard Time", "Africa/Tripoli")
    case object NamibiaStandard extends Timezone(70, "Namibia Standard Time", "Africa/Windhoek")
    case object ArabicStandard extends Timezone(71, "Arabic Standard Time", "Asia/Baghdad")
    case object TurkeyStandard extends Timezone(72, "Turkey Standard Time", "Europe/Istanbul")
    case object ArabStandard extends Timezone(73, "Arab Standard Time", "Asia/Riyadh")
    case object BelarusStandard extends Timezone(74, "Belarus Standard Time", "Europe/Minsk")
    case object RussianStandard extends Timezone(75, "Russian Standard Time", "Europe/Moscow")
    case object EastAfricaStandard extends Timezone(76, "E. Africa Standard Time", "Africa/Nairobi")
    case object IranStandard extends Timezone(77, "Iran Standard Time", "Asia/Tehran")
    case object ArabianStandard extends Timezone(78, "Arabian Standard Time", "Asia/Dubai")
    case object AstrakhanStandard extends Timezone(79, "Astrakhan Standard Time", "Europe/Astrakhan")
    case object AzerbaijanStandard extends Timezone(80, "Azerbaijan Standard Time", "Asia/Baku")
    case object RussiaTimeZone3 extends Timezone(81, "Russia Time Zone 3", "Europe/Samara")
    case object MauritiusStandard extends Timezone(82, "Mauritius Standard Time", "Indian/Mauritius")
    case object SaratovStandard extends Timezone(83, "Saratov Standard Time", "Europe/Saratov")
    case object GeorgianStandard extends Timezone(84, "Georgian Standard Time", "Asia/Tbilisi")
    case object VolgogradStandard extends Timezone(85, "Volgograd Standard Time", "Europe/Volgograd")
    case object CaucasusStandard extends Timezone(86, "Caucasus Standard Time", "Asia/Yerevan")
    case object AfghanistanStandard extends Timezone(87, "Afghanistan Standard Time", "Asia/Kabul")
    case object WestAsiaStandard extends Timezone(88, "West Asia Standard Time", "Asia/Tashkent")
    case object EkaterinburgStandard extends Timezone(89, "Ekaterinburg Standard Time", "Asia/Yekaterinburg")
    case object PakistanStandard extends Timezone(90, "Pakistan Standard Time", "Asia/Karachi")
    case object QyzylordaStandard extends Timezone(91, "Qyzylorda Standard Time", "Asia/Qyzylorda")
    case object IndiaStandard extends Timezone(92, "India Standard Time", "Asia/Kolkata")
    case object SriLankaStandard extends Timezone(93, "Sri Lanka Standard Time", "Asia/Colombo")
    case object NepalStandard extends Timezone(94, "Nepal Standard Time", "Asia/Kathmandu")
    case object CentralAsiaStandard extends Timezone(95, "Central Asia Standard Time", "Asia/Novosibirsk")
    case object BangladeshStandard extends Timezone(96, "Bangladesh Standard Time", "Asia/Dhaka")
    case object OmskStandard extends Timezone(97, "Omsk Standard Time", "Asia/Omsk")
    case object MyanmarStandard extends Timezone(98, "Myanmar Standard Time", "Asia/Yangon")
    case object SEAsiaStandard extends Timezone(99, "SE Asia Standard Time", "Asia/Bangkok")
    case object AltaiStandard extends Timezone(100, "Altai Standard Time", "Asia/Barnaul")
    case object WestMongoliaStandard extends Timezone(101, "W. Mongolia Standard Time", "Asia/Hovd")
    case object NorthAsiaStandard extends Timezone(102, "North Asia Standard Time", "Asia/Krasnoyarsk")
    case object NorthCentralAsiaStandard extends Timezone(103, "N. Central Asia Standard Time", "Asia/Novosibirsk")
    case object TomskStandard extends Timezone(104, "Tomsk Standard Time", "Asia/Tomsk")
    case object ChinaStandard extends Timezone(105, "China Standard Time", "Asia/Shanghai")
    case object NorthAsiaEastStandard extends Timezone(106, "North Asia East Standard Time", "Asia/Irkutsk")
    case object SingaporeStandard extends Timezone(107, "Singapore Standard Time", "Asia/Singapore")
    case object WestAustraliaStandard extends Timezone(108, "W. Australia Standard Time", "\tAustralia/Perth")
    case object TaipeiStandard extends Timezone(109, "Taipei Standard Time", "Asia/Taipei")
    case object UlaanbaatarStandard extends Timezone(110, "Ulaanbaatar Standard Time", "Asia/Ulaanbaatar")
    case object AusCentralWestStandard extends Timezone(111, "Aus Central W. Standard Time", "Australia/Eucla")
    case object TransBaikalStandard extends Timezone(112, "Transbaikal Standard Time", "Asia/Chita")
    case object TokyoStandard extends Timezone(113, "Tokyo Standard Time", "Asia/Tokyo")
    case object NorthKoreaStandard extends Timezone(114, "North Korea Standard Time", "Asia/Pyongyang")
    case object KoreaStandard extends Timezone(115, "Korea Standard Time", "Asia/Seoul")
    case object YakutskStandard extends Timezone(116, "Yakutsk Standard Time", "Asia/Yakutsk")
    case object CenAustraliaStandard extends Timezone(117, "Cen. Australia Standard Time", "Australia/Adelaide")
    case object AusCentralStandard extends Timezone(118, "AUS Central Standard Time", "Australia/Darwin")
    case object EastAustraliaStandard extends Timezone(119, "E. Australia Standard Time", "Australia/Brisbane")
    case object AusEasternStandard extends Timezone(120, "AUS Eastern Standard Time", "Australia/Sydney")
    case object TasmaniaStandard extends Timezone(121, "Tasmania Standard Time", "Australia/Hobart")
    case object VladivostokStandard extends Timezone(122, "Vladivostok Standard Time", "Asia/Vladivostok")
    case object LordHoweStandard extends Timezone(123, "Lord Howe Standard Time", "Australia/Lord_Howe")
    case object BougainvilleStandard extends Timezone(124, "Bougainville Standard Time", "Pacific/Bougainville")
    case object RussiaTimeZone10 extends Timezone(125, "Russia Time Zone 10", "Asia/Srednekolymsk")
    case object MagadanStandard extends Timezone(126, "Magadan Standard Time", "Asia/Magadan")
    case object NorfolkStandard extends Timezone(127, "Norfolk Standard Time", "Pacific/Norfolk")
    case object SakhalinStandard extends Timezone(128, "Sakhalin Standard Time", "Asia/Sakhalin")
    case object CentralPacificStandard extends Timezone(129, "Central Pacific Standard Time", "Pacific/Guadalcanal")
    case object RussiaTimeZone11 extends Timezone(130, "Russia Time Zone 11", "Asia/Kamchatka")
    case object NewZealandStandard extends Timezone(131, "New Zealand Standard Time", "Pacific/Auckland")
    case object UTCPlus12 extends Timezone(132, "UTC+12", "Pacific/Tarawa")
    case object FijiStandard extends Timezone(133, "Fiji Standard Time", "Pacific/Fiji")
    case object KamchatkaStandard extends Timezone(134, "Kamchatka Standard Time", "Asia/Kamchatka")
    case object ChathamIslandsStandard extends Timezone(135, "Chatham Islands Standard Time", "Pacific/Chatham")
    case object UTCPlus13 extends Timezone(136, "UTC+13", "Etc/GMT-13")
    case object TongaStandard extends Timezone(137, "Tonga Standard Time", "Pacific/Tongatapu")
    case object SamoaStandard extends Timezone(138, "Samoa Standard Time", "Pacific/Apia")
    case object LineIslandsStandard extends Timezone(139, "Line Islands Standard Time", "Pacific/Kiritimati")

    def values: immutable.IndexedSeq[Timezone] = findValues
}
