package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithMap
import io.github.oemergenc.hbase.orm.extensions.domain.records.NoAliasHBDynamicColumnRecord
import spock.lang.Specification

class NoAliasDynamicColumnMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Converting a record with a no alias record works"() {
        given:
        def staticId = "staticId"
        def dynamicFamilyList = [
                new DependentWithMap("dynamicId2", ["1sja": "bsjcasla", "12321": "csdckqewwqe"]),
                new DependentWithMap("dynamicId1", ["1": "bla", "12321": "qewwqe"])
        ]
        def record = new NoAliasHBDynamicColumnRecord(staticId, dynamicFamilyList)

        when:
        def result = mapper.writeValueAsResult(record)

        then:
        result
        result.getFamilyMap("noAliasDynamicFamily".bytes)["dynamicId1".bytes]
        result.getFamilyMap("noAliasDynamicFamily".bytes)["dynamicId2".bytes]
        result.getFamilyMap("staticFamily".bytes)['staticId'.bytes]

        when:
        def recordResult = mapper.readValue(result, NoAliasHBDynamicColumnRecord.class)

        then:
        recordResult
        recordResult.staticId == staticId
        recordResult.noAliasDynamicFamily.size() == 2
        recordResult.noAliasDynamicFamily.collect { it.dynamicId }.containsAll(["dynamicId1", "dynamicId2"])
    }
}
