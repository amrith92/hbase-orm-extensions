package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.MultipleDynamicColumsWithSameFamilyDao

import static io.github.oemergenc.hbase.orm.extensions.data.TestContent.getDefaultMultiHBDynamicColumWithSameFamilyRecord

class MultiDynamicColumnWithSameFamilyComponentSpec extends AbstractComponentSpec {
    def dao = new MultipleDynamicColumsWithSameFamilyDao(bigTableHelper.connect())

    def "Converting to get for dynamic qualifiers works"() {
        given:
        def staticId = UUID.randomUUID() as String
        def record = getDefaultMultiHBDynamicColumWithSameFamilyRecord(staticId)

        when:
        dao.persist(record)

        then:
        def resultResult = dao.getRecordForDynamicFamily(staticId, queriedQualifierParts)

        then:
        resultResult.isPresent() == isPresent

        where:
        queriedQualifierParts      | isPresent
        ["dv1_dp1_1", "otherId_1"] | true
        ["dv1_dp1_2", "otherId_2"] | true
        ["dv2_dp1_1", "otherId_1"] | true
        ["dv2_dp1_2", "otherId_2"] | true
    }
}
