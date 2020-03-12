package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsRecord
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsWithSameFamilyRecord
import spock.lang.Specification

import static io.github.oemergenc.hbase.orm.extensions.data.TestContent.getDefaultMultiHBDynamicColumWithSameFamilyRecord

class MultiDynamicColumnWithSameFamilyMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Converting a record with multiple dynamic columns works"() {
        given:
        def staticId = UUID.randomUUID() as String
        def record = getDefaultMultiHBDynamicColumWithSameFamilyRecord(staticId)

        when:
        def result = mapper.writeValueAsResult(record)

        then:
        result
        result.getFamilyMap("dynamicFamily1".bytes)["df1#dv1_dp1_1:otherId_1".bytes]
        result.getFamilyMap("dynamicFamily1".bytes)["df1#dv1_dp1_2:otherId_2".bytes]
        result.getFamilyMap("dynamicFamily1".bytes)["df2#dv2_dp1_1:otherId_1".bytes]
        result.getFamilyMap("dynamicFamily1".bytes)["df2#dv2_dp1_2:otherId_2".bytes]
        result.getFamilyMap("staticFamily".bytes)['staticId'.bytes]

        when:
        def recordResult = mapper.readValue(result, MultipleHBDynamicColumnsWithSameFamilyRecord.class)

        then:
        recordResult
        recordResult.staticId == staticId

        and:
        recordResult.dynamicValues1.collect { it.dynamicPart1 }.containsAll(["dv1_dp1_1", "dv1_dp1_2"])
        recordResult.dynamicValues1.collect { it.otherId }.containsAll(["otherId_1", "otherId_2"])
        recordResult.dynamicValues1.collect { it.aString }.containsAll(["aString_1", "aString_2"])

        and:
        recordResult.dynamicValues2.collect { it.dynamicPart1 }.containsAll(["dv2_dp1_1", "dv2_dp1_2"])
        recordResult.dynamicValues2.collect { it.otherId }.containsAll(["otherId_1", "otherId_2"])
        recordResult.dynamicValues2.collect { it.aString }.containsAll(["aString_1", "aString_2"])
        recordResult.dynamicValues2.flatten { it.nodes } == []
    }

    def "Converting to get for dynamic qualifiers works"() {
        given:
        def staticId = UUID.randomUUID() as String
        def record = getDefaultMultiHBDynamicColumWithSameFamilyRecord(staticId)

        when:
        mapper.writeValueAsResult(record)

        then:
        def get = mapper.getAsGet(MultipleHBDynamicColumnsRecord.class, staticId.bytes, "dynamicFamily1", queriedQualifierParts)

        then:
        get
        get.numFamilies() == expectedNumSize

        where:
        queriedQualifierParts      | expectedNumSize
        ["dynamicId1"]             | 1
        ["dv1_dp1_1", "dv1_dp2_1"] | 1
        ["237912837"]              | 1
    }
}
