package com.github.chenharryhua.nanjin.http.client.auth

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class AdobeMetascope(val name: String) extends EnumEntry with Product

// https://developer.adobe.com/developer-console/docs/guides/authentication/JWT/Scopes/
object AdobeMetascope
    extends Enum[AdobeMetascope] with CatsEnum[AdobeMetascope] with CirceEnum[AdobeMetascope] {
  override val values: immutable.IndexedSeq[AdobeMetascope] = findValues
  case object AdobeAnalytics extends AdobeMetascope("ent_analytics_bulk_ingest_sdk")
  case object ExperiencePlatform extends AdobeMetascope("ent_dataservices_sdk")
  case object Campaign extends AdobeMetascope("ent_campaign_sdk")
  case object Target extends AdobeMetascope("ent_marketing_sdk")
  case object EplAdmin extends AdobeMetascope("ent_reactor_admin_sdk")
  case object EplITAdmin extends AdobeMetascope("ent_reactor_it_admin_sdk")
  case object EplDeveloper extends AdobeMetascope("ent_reactor_developer_sdk")
  case object EplApprover extends AdobeMetascope("ent_reactor_approver_sdk")
  case object EplPublisher extends AdobeMetascope("ent_reactor_publisher_sdk")
  case object EplExtensionDeveloper extends AdobeMetascope("ent_reactor_extension_developer_sdk")
  case object Gdpr extends AdobeMetascope("ent_gdpr_sdk")
  case object SmartContent extends AdobeMetascope("ent_smartcontent_sdk")
  case object AutoCrop extends AdobeMetascope("ent_sensei_image_sdk")
  case object UserManagement extends AdobeMetascope("ent_user_sdk")
  case object AEMBrandportal extends AdobeMetascope("ent_brand_portal_sdk")
  case object Places extends AdobeMetascope("ent_places_sdk")
  case object CloudManager extends AdobeMetascope("ent_cloudmgr_sdk")
}
