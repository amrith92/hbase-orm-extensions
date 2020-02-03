package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.CampaignDao
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord

class ComponentTest extends AbstractComponentSpec {
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
}
