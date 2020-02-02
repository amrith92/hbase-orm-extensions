package io.github.oemergenc.hbase.orm.extensions.componenttests.utils

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan

class CampaignActionsBigTableUtil {

    JsonSlurper jsonSlurper = new JsonSlurper()

    private final BigTableHelper bigTableHelper

    CampaignActionsBigTableUtil(BigTableHelper bigTableHelper) {
        this.bigTableHelper = bigTableHelper
    }

    public void insertAction(Map marketingAction) {
        def connection = this.bigTableHelper.connect()
        def table = connection.getTable(TableName.valueOf('campaigns'))
        def put = new Put("pfx#${marketingAction.customerId}".bytes)
        put.addColumn("campaign".bytes, ("id#" + marketingAction.campaignId).bytes, JsonOutput.toJson(marketingAction).bytes)
        table.put(put)
    }

    def getAllActions() {
        def connection = this.bigTableHelper.connect()
        def table = connection.getTable(TableName.valueOf('campaigns'))
        def scan = new Scan()
        scan.addFamily("campaign".bytes)

        def allCampaigns = table.getScanner(scan).asList().collect { c ->
            println(c)
            jsonSlurper.parse(c.value()) as Map
        }
        connection.close()

        return allCampaigns
    }

    def getKey() {
        def connection = this.bigTableHelper.connect()
        def table = connection.getTable(TableName.valueOf('campaigns'))
        def scan = new Scan()
        scan.addFamily("campaign".bytes)

        def get = new Get("pfx#custId2".bytes)
        def allCampaigns = table.get(get)
        connection.close()

        return allCampaigns
    }

    String getRowKey(final String customerPid) {
        return "pfx#" + customerPid
    }
}
