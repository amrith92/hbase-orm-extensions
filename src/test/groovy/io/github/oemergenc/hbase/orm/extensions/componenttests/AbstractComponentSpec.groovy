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
    @Shared
    protected PollingConditions pollingConditions = new PollingConditions(timeout: 10)

    @Shared
    static BigTableContainer bigTableContainer = new BigTableContainer()

    @Shared
    static BigTableHelper bigTableHelper

    @Shared
    static CampaignActionsBigTableUtil campaignActionsBigTableUtil

    static {
        def bigTableProjectId = 'irrelevant'
        def bigTableInstanceId = 'irrelevant'

        if (System.getenv("BIGTABLE_EMULATOR_HOST") == null) {
            bigTableHelper = onRunOnCiServer(bigTableProjectId, bigTableInstanceId)
        } else {
            bigTableHelper = onRunOnDev(bigTableProjectId, bigTableInstanceId)
        }

        campaignActionsBigTableUtil = new CampaignActionsBigTableUtil(bigTableHelper)
        def connection = bigTableHelper.connect()
        def tableDescriptor = new HTableDescriptor(TableName.valueOf('campaigns'))
        tableDescriptor.addFamily(new HColumnDescriptor("campaign"))
        connection.admin.createTable(tableDescriptor)

        def tableDescriptor2 = new HTableDescriptor(TableName.valueOf('users'))
        tableDescriptor2.addFamily(new HColumnDescriptor("address"))
        tableDescriptor2.addFamily(new HColumnDescriptor("optional"))
        connection.admin.createTable(tableDescriptor2)

        def tableDescriptor3 = new HTableDescriptor(TableName.valueOf('bd_omm_prp_campaigns'))
        tableDescriptor3.addFamily(new HColumnDescriptor("campaigns"))
        tableDescriptor3.addFamily(new HColumnDescriptor("optional"))
        connection.admin.createTable(tableDescriptor3)
    }

    def setupSpec() {

    }

    def setup() {

    }

    def clean() {

    }

    def cleanupSpec() {

    }

    public static BigTableHelper onRunOnCiServer(String bigTableProjectId, String bigTableInstanceId) {
        bigTableContainer.start()

        def bigTablePort = bigTableContainer.getMappedPort(8086)
        def bigTableHost = bigTableContainer.containerIpAddress + ""

        System.setProperty('bigtable.port', bigTablePort as String)
        System.setProperty('bigtable.host', bigTableHost)
        System.setProperty('bigtable.projectId', bigTableProjectId)
        System.setProperty('bigtable.instanceId', bigTableInstanceId)

        bigTableHelper = new BigTableHelper(bigTablePort,
                bigTableHost,
                bigTableProjectId,
                bigTableInstanceId)
        bigTableHelper
    }

    public static BigTableHelper onRunOnDev(String bigTableProjectId, String bigTableInstanceId) {
        bigTableHelper = new BigTableHelper(bigTableProjectId, bigTableInstanceId)
        bigTableHelper
    }

}
