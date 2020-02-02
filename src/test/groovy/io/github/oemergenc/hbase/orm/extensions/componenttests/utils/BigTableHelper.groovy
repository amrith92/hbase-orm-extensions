package io.github.oemergenc.hbase.orm.extensions.componenttests.utils

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection

class BigTableHelper {
    final int bigTablePort
    final String bigTableHost
    final String bigTableProjectId
    final String bigTableInstanceId

    BigTableHelper(final int bigTablePort,
                   final String bigTableHost,
                   final String bigTableProjectId,
                   final String bigTableInstanceId) {
        this.bigTablePort = bigTablePort
        this.bigTableHost = bigTableHost
        this.bigTableProjectId = bigTableProjectId
        this.bigTableInstanceId = bigTableInstanceId
    }

    Connection connect() {
        Configuration conf = BigtableConfiguration.configure(bigTableProjectId, bigTableInstanceId)

        conf.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, bigTableHost + ":" + bigTablePort)
        conf.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, bigTableHost + ":" + bigTablePort)
        conf.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, bigTableHost)
        conf.set(BigtableOptionsFactory.BIGTABLE_PORT_KEY, bigTablePort as String)
        conf.set(BigtableOptionsFactory.PROJECT_ID_KEY, bigTableProjectId)
        conf.set(BigtableOptionsFactory.INSTANCE_ID_KEY, bigTableInstanceId)
        conf.set(BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION, "true")
        BigtableConfiguration.connect(conf)
    }
}