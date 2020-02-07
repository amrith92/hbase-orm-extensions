package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.CampaignDao
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord
import spock.lang.Unroll

import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.campaign
import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.record

class CampaignComponentTest extends AbstractComponentSpec {
    def campaignDao = new CampaignDao(bigTableHelper.connect())

    def "correct is correct"() {
        given:
        campaignActionsBigTableUtil.insertAction([customerId: 'some-customer-id', campaignId: 'some-campaign-1', recoType: 'RANDOM', products: [1: "bhhkjgi"]])
        campaignActionsBigTableUtil.insertAction([customerId: 'some-customer-id', campaignId: 'some-campaign-2', recoType: 'PERSONALIZED', products: [1: "1233124"]])

        when:
        def record = campaignDao.get("pfx#some-customer-id")

        then:
        record
    }

    def "Store and reading dynamic column works"() {
        given:
        def campain = new Campaign("camId1", "custId2")
        def campain2 = new Campaign("camId2", "custId2")
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setCampaigns([campain, campain2])

        when:
        campaignDao.persist([campaignRecord])

        and:
        def record = campaignDao.get("pfx#custId2")

        then:
        record.customerId == "custId2"
        record.campaigns.size() == 2
        record.campaigns.collect { it.campaignId }.containsAll(["camId2", "camId1"])
        record.campaigns.collect { it.customerId }.containsAll(["custId2"])
    }

    @Unroll
    def "Invalid entries will be logged and do not break persisting valid entries"() {
        given:
        def customerId = "the-customer-id-2"
        def campaignInvalid = campaign(campaignId: campaignId, customerId: customerId)
        def campaignValid = campaign(campaignId: "valid-campaign", customerId: customerId)
        def campaignRecord = record(customerId: customerId, campaigns: [campaignInvalid, campaignValid])

        when:
        campaignDao.persist([campaignRecord])

        and:
        def campaignRecordResult = campaignDao.get("pfx#" + customerId)

        then:
        campaignRecordResult.customerId == customerId
        campaignRecordResult.campaigns.size() == 1
        campaignRecordResult.campaigns.collect { it.campaignId }.containsAll(["valid-campaign"])
        campaignRecordResult.campaigns.collect { it.customerId }.containsAll([customerId])

        where:
        campaignId | _
        null       | _
        ""         | _
        "   "      | _
    }

    def "Retrieving a specific campaign works"() {
        def customerId = "custId2"
        given:
        def campain = new Campaign("camId1", customerId)
        def campain2 = new Campaign("camId2", customerId)
        def campain3 = new Campaign("camId3", customerId)
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId(customerId)
        campaignRecord.setCampaigns([campain, campain2, campain3])

        when:
        campaignDao.persist([campaignRecord])

        and:
        def record = campaignDao.getCampaign("pfx#custId2", "camId3")

        then:
        record.isPresent()
        record.get().customerId == customerId
        record.get().campaigns.size() == 1
        record.get().campaigns.collect { it.campaignId }.containsAll(["camId3"])
        record.get().campaigns.collect { it.customerId }.containsAll([customerId])
    }
}
