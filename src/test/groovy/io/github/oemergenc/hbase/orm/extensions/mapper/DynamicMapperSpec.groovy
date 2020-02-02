package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord
import spock.lang.Specification

class DynamicMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "correct is correct"() {
        given:
        def campain = new Campaign("camId1", "custId2")
        def campain2 = new Campaign("camId2", "custId2")
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setCampaigns([campain, campain2])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        and:
        def campaignRecord1 = mapper.readValue(result, CampaignRecord.class)

        then:
        result
        campaignRecord1
    }

    def "correct is correct2"() {
        given:
        def campain = new Campaign("camId1", "custId2")
        def campain2 = new Campaign("camId2", "custId2")
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setCampaigns([campain, campain2])

        when:
        def put = mapper.writeValueAsPut(campaignRecord)

        then:
        put.familyCellMap
        println(put.familyCellMap)
    }
}
