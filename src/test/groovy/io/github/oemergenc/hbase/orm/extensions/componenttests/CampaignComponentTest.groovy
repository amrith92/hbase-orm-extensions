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
//        null       | _
        ""         | _
        "   "      | _
    }

    def "Retrieving a specific campaign works"() {
        def customerId = "custId2"
        given:
        def campaign1 = campaign(campaignId: "campaign1", customerId: customerId)
        def campaign2 = campaign(campaignId: "campaign2", customerId: customerId)
        def campaign3 = campaign(campaignId: "campaign3", customerId: customerId)
        def campaignRecord = record(customerId: customerId, campaigns: [campaign1, campaign2, campaign3])

        when:
        campaignDao.persist([campaignRecord])

        and:
        def record = campaignDao.getCampaign("pfx#custId2", "campaign3")

        then:
        record.isPresent()
        record.get().customerId == customerId
        record.get().campaigns.size() == 1
        record.get().campaigns.collect { it.campaignId }.containsAll(["campaign3"])
        record.get().campaigns.collect { it.customerId }.containsAll([customerId])

        when:
        def record2 = campaignDao.getCampaign("pfx#custId2", "campaign2")

        then:
        record2.isPresent()
        record2.get().customerId == customerId
        record2.get().campaigns.size() == 1
        record2.get().campaigns.collect { it.campaignId }.containsAll(["campaign2"])
        record2.get().campaigns.collect { it.customerId }.containsAll([customerId])

        when:
        def record3 = campaignDao.getCampaign("pfx#custId2", "campaign1")

        then:
        record3.isPresent()
        record3.get().customerId == customerId
        record3.get().campaigns.size() == 1
        record3.get().campaigns.collect { it.campaignId }.containsAll(["campaign1"])
        record3.get().campaigns.collect { it.customerId }.containsAll([customerId])
    }

    def "Retrieving a specific campaign which does not exist does not throw exception"() {
        given:
        def customerId = "custId2"
        def campaign1 = campaign(campaignId: "campaign1", customerId: customerId)
        def campaign2 = campaign(campaignId: "campaign2", customerId: customerId)
        def campaign3 = campaign(campaignId: "campaign3", customerId: customerId)
        def campaignRecord = record(customerId: customerId, campaigns: [campaign1, campaign2, campaign3])

        when:
        campaignDao.persist([campaignRecord])

        and:
        def record = campaignDao.getCampaign("pfx#custId2", "camId4")

        then:
        !record.isPresent()
    }
}
