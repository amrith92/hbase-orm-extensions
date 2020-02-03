package io.github.oemergenc.hbase.orm.extensions.componenttests.utils

import groovy.json.JsonOutput
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put

class CampaignActionsBigTableUtil {

    private final BigTableHelper bigTableHelper

    CampaignActionsBigTableUtil(BigTableHelper bigTableHelper) {
        this.bigTableHelper = bigTableHelper
    }

    def insertAction(Map marketingAction) {
        def connection = this.bigTableHelper.connect()
        def table = connection.getTable(TableName.valueOf('campaigns'))
        def put = new Put("pfx#${marketingAction.customerId}".bytes)
        put.addColumn("campaign".bytes, ("id#" + marketingAction.campaignId).bytes, JsonOutput.toJson(marketingAction).bytes)
        table.put(put)
        connection.close()
    }
}
