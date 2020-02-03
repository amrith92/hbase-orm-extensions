package io.github.oemergenc.hbase.orm.extensions.data

import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord

class CampaignContent {
    def static campaign(Map params = [:]) {
        Map values = [
                campaignId: "91828282",
                customerId: "the-customer-id",
                products  : ["1": "iuhiuhiu", "2": "3129837921"]
        ] << params
        def campaign = new Campaign(values.campaignId, values.customerId)
        campaign.setProducts(values.products)
        campaign
    }

    def static record(Map params = [:]) {
        Map values = [
                customerId: "the-customer-id",
                campaigns : [campaign(params)]
        ] << params
        new CampaignRecord(values.customerId, values.campaigns)
    }
}
