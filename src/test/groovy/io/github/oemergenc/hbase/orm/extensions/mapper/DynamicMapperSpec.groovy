package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord
import spock.lang.Specification

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
}
