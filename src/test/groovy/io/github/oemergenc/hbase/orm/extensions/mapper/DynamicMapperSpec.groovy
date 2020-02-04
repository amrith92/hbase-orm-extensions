package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord
import spock.lang.Specification

import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.campaign
import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.record

class DynamicMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Converting model class works"() {
        given:
        def campain = new Campaign("camId1", "custId2")
        def campain2 = new Campaign("camId2", "custId2")
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setCampaigns([campain, campain2])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        and:
        def campaignRecordResult = mapper.readValue(result, CampaignRecord.class)

        then:
        result
        campaignRecordResult.customerId == "custId2"
        campaignRecordResult.campaigns.size() == 2
        campaignRecordResult.campaigns.collect { it.campaignId }.containsAll(["camId2", "camId1"])
        campaignRecordResult.campaigns.collect { it.customerId }.containsAll(["custId2"])
    }

    def "Invalid entries do no break persisting valid objects"() {
        given:
        def campaignInvalid = campaign(campaignId: null, customerId: "the-customer-id")
        def campaignValid = campaign(campaignId: "valid-campaign", customerId: "the-customer-id")
        def campaignRecord = record(campaigns: [campaignInvalid, campaignValid])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        and:
        def campaignRecordResult = mapper.readValue(result, CampaignRecord.class)

        then:
        result
        campaignRecordResult.customerId == "the-customer-id"
        campaignRecordResult.campaigns.size() == 1
        campaignRecordResult.campaigns.collect { it.campaignId }.containsAll(["valid-campaign"])
        campaignRecordResult.campaigns.collect { it.customerId }.containsAll(["the-customer-id"])
    }
}
