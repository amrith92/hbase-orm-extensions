package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.BigTableContainer
import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.BigTableHelper
import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.CampaignActionsBigTableUtil
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

abstract class AbstractComponentSpec extends Specification {

    protected PollingConditions pollingConditions = new PollingConditions(timeout: 10)

    @Shared
    static BigTableContainer bigTableContainer = new BigTableContainer()

    @Shared
    static BigTableHelper bigTableHelper

    @Shared
    static CampaignActionsBigTableUtil campaignActionsBigTableUtil

    static {
        bigTableContainer.start()

        def bigTablePort = bigTableContainer.getMappedPort(8080)
        def bigTableHost = bigTableContainer.containerIpAddress + ""

        def bigTableProjectId = 'irrelevant'
        def bigTableInstanceId = 'irrelevant'

        System.setProperty('bigtable.port', bigTablePort as String)
        System.setProperty('bigtable.host', bigTableHost)
        System.setProperty('bigtable.projectId', bigTableProjectId)
        System.setProperty('bigtable.instanceId', bigTableInstanceId)

        bigTableHelper = new BigTableHelper(bigTablePort,
                bigTableHost,
                bigTableProjectId,
                bigTableInstanceId)
        campaignActionsBigTableUtil = new CampaignActionsBigTableUtil(bigTableHelper)

        def connection = bigTableHelper.connect()

        def tableDescriptor = new HTableDescriptor(TableName.valueOf('campaigns'))
        tableDescriptor.addFamily(new HColumnDescriptor("campaign"))
        connection.admin.createTable(tableDescriptor)
        connection.close()
    }

    def setupSpec() {

    }

    def setup() {

    }

    def clean() {

    }

    def cleanupSpec() {

    }
}
