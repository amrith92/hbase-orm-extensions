package io.github.oemergenc.hbase.orm.extensions.componenttests


import io.github.oemergenc.hbase.orm.extensions.dao.NoAliasDynamicColumsDao
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithMap
import io.github.oemergenc.hbase.orm.extensions.domain.records.NoAliasHBDynamicColumnRecord

class NoAliasDynamicColumnsDaoComponentTest extends AbstractComponentSpec {
    def dao = new NoAliasDynamicColumsDao(bigTableHelper.connect())

    def "Store and reading dynamic column works"() {
        given:
        def expectedRowKey = "theRowKey"
        def dynamicValues1 = [
                new DependentWithMap("dv1_dp1_1", ["k1": "v1"]),
                new DependentWithMap("dv1_dp1_2", ["k2": "v2"]),
        ]

        def record = new NoAliasHBDynamicColumnRecord(expectedRowKey, dynamicValues1)

        when:
        def rowKey = dao.persist(record)

        then:
        rowKey == expectedRowKey

        when:
        def recordResult = dao.get(expectedRowKey)

        then:
        recordResult
        recordResult.staticId == rowKey

        and:
        recordResult.noAliasDynamicFamily.collect { it.dynamicId }.containsAll(["dv1_dp1_1", "dv1_dp1_2"])
        recordResult.noAliasDynamicFamily.collect { it.products }.containsAll(["k1": "v1"], ["k2": "v2"])
    }
}
