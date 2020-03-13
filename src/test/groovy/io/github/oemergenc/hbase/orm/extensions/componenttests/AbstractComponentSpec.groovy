package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.BigTableContainer
import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.BigTableHelper
import io.github.oemergenc.hbase.orm.extensions.componenttests.utils.CampaignActionsBigTableUtil
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
        bigTableHelper.createTable("campaigns", ["campaign"])
        bigTableHelper.createTable("users", ["address", "optional"])
        bigTableHelper.createTable("bd_omm_prp_campaigns", ["campaigns", "days", "optional"])
        bigTableHelper.createTable("multiple_dynamic_columns_table", ["staticFamily", "dynamicFamily1", "dynamicFamily2", "dynamicFamily3", "dynamicFamily4"])
        bigTableHelper.createTable("multiple_dynamic_columns_table_with_same_family", ["staticFamily", "dynamicFamily1"])
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
