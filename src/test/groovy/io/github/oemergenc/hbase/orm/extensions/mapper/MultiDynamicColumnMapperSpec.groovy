package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithListType
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithPrimitiveTypes
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsRecord
import spock.lang.Specification

class MultiDynamicColumnMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Converting a record with multiple dynamic columns works"() {
        given:
        def staticId = "staticId"
        def dynamicValues1 = [
                new DependentWithPrimitiveTypes("dv1_dp1_1", "dv1_dp2_1", "dv1_position_1", "dv1_recipeId_1", "<html>dv1_html_1</html>"),
                new DependentWithPrimitiveTypes("dv1_dp1_2", "dv1_dp2_2", "dv1_position_2", "dv1_recipeId_2", "<html>dv1_html_2</html>")
        ]
        def dynamicValues2 = [
                new DependentWithListType("dv2_dp1_1", "otherId_1", [], "aString_1"),
                new DependentWithListType("dv2_dp1_2", "otherId_2", [], "aString_2"),
        ]
        def dynamicValues3 = [
                new DependentWithPrimitiveTypes("dv3_dp1_1", "dv3_dp2_1", "dv3_position_1", "dv3_recipeId_1", "<html>dv3_html_1</html>"),
                new DependentWithPrimitiveTypes("dv3_dp1_2", "dv3_dp2_2", "dv3_position_2", "dv3_recipeId_2", "<html>dv3_html_2</html>")
        ]
        def dynamicValues4 = [
                new DependentWithPrimitiveTypes("dv4_dp1_1", "dv4_dp2_1", "dv4_position_1", "dv4_recipeId_1", "<html>dv4_html_1</html>"),
                new DependentWithPrimitiveTypes("dv4_dp1_2", "dv4_dp2_2", "dv4_position_2", "dv4_recipeId_2", "<html>dv4_html_2</html>")
        ]

        def record = new MultipleHBDynamicColumnsRecord(staticId, dynamicValues1, dynamicValues2, dynamicValues3, dynamicValues4)

        when:
        def result = mapper.writeValueAsResult(record)

        then:
        result
        result.getFamilyMap("dynamicFamily1".bytes)["df1#dv1_dp1_1:dv1_dp2_1".bytes]
        result.getFamilyMap("dynamicFamily2".bytes)["df2#dv2_dp1_1".bytes]
        result.getFamilyMap("dynamicFamily3".bytes)["df3#dv3_dp1_1:dv3_dp2_1".bytes]
        result.getFamilyMap("dynamicFamily4".bytes)["df4#dv4_dp1_1".bytes]
        result.getFamilyMap("staticFamily".bytes)['staticId'.bytes]

        when:
        def recordResult = mapper.readValue(result, MultipleHBDynamicColumnsRecord.class)

        then:
        recordResult
        recordResult.staticId == staticId

        and:
        recordResult.dynamicValues1.collect { it.dynamicPart1 }.containsAll(["dv1_dp1_1", "dv1_dp1_2"])
        recordResult.dynamicValues1.collect { it.dynamicPart2 }.containsAll(["dv1_dp2_1", "dv1_dp2_2"])
        recordResult.dynamicValues1.collect { it.position }.containsAll(["dv1_position_1", "dv1_position_2"])
        recordResult.dynamicValues1.collect { it.recipeId }.containsAll(["dv1_recipeId_1", "dv1_recipeId_2"])
        recordResult.dynamicValues1.collect { it.html }.containsAll(["<html>dv1_html_1</html>", "<html>dv1_html_2</html>"])

        and:
        recordResult.dynamicValues2.collect { it.dynamicPart1 }.containsAll(["dv2_dp1_1", "dv2_dp1_2"])
        recordResult.dynamicValues2.collect { it.otherId }.containsAll(["otherId_1", "otherId_2"])
        recordResult.dynamicValues2.collect { it.aString }.containsAll(["aString_1", "aString_2"])
        recordResult.dynamicValues2.flatten { it.nodes } == []

        and:
        recordResult.dynamicValues3.collect { it.dynamicPart1 }.containsAll(["dv3_dp1_1", "dv3_dp1_2"])
        recordResult.dynamicValues3.collect { it.dynamicPart2 }.containsAll(["dv3_dp2_1", "dv3_dp2_2"])
        recordResult.dynamicValues3.collect { it.position }.containsAll(["dv3_position_1", "dv3_position_2"])
        recordResult.dynamicValues3.collect { it.recipeId }.containsAll(["dv3_recipeId_1", "dv3_recipeId_2"])
        recordResult.dynamicValues3.collect { it.html }.containsAll(["<html>dv3_html_1</html>", "<html>dv3_html_2</html>"])

        and:
        recordResult.dynamicValues4.collect { it.dynamicPart1 }.containsAll(["dv4_dp1_1", "dv4_dp1_2"])
        recordResult.dynamicValues4.collect { it.dynamicPart2 }.containsAll(["dv4_dp2_1", "dv4_dp2_2"])
        recordResult.dynamicValues4.collect { it.position }.containsAll(["dv4_position_1", "dv4_position_2"])
        recordResult.dynamicValues4.collect { it.recipeId }.containsAll(["dv4_recipeId_1", "dv4_recipeId_2"])
        recordResult.dynamicValues4.collect { it.html }.containsAll(["<html>dv4_html_1</html>", "<html>dv4_html_2</html>"])
    }
}
