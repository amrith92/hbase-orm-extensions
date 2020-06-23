package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.NoAliasMixedDynamicColumsDao
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithMap
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithPrimitiveTypes
import io.github.oemergenc.hbase.orm.extensions.domain.records.NoAliasMixedHBDynamicColumnRecord

class NoAliasMixedDynamicColumnsDaoComponentTest extends AbstractComponentSpec {
    def dao = new NoAliasMixedDynamicColumsDao(bigTableHelper.connect())

    def "Store and reading dynamic column with no alias column works"() {
        given:
        def expectedRowKey = "theRowKey"
        def dynamicValues1 = [
                new DependentWithMap("dv1_dp1_1", ["k1": "v1"]),
                new DependentWithMap("dv1_dp1_2", ["k2": "v2"]),
        ]
        def dynamicValues2 = [
                new DependentWithPrimitiveTypes("dv1_dp1_1", "dv1_dp2_1", "dv1_position_1", "dv1_recipeId_1", "<html>dv1_html_1</html>"),
                new DependentWithPrimitiveTypes("dv1_dp1_2", "dv1_dp2_2", "dv1_position_2", "dv1_recipeId_2", "<html>dv1_html_2</html>")
        ]

        def record = new NoAliasMixedHBDynamicColumnRecord(expectedRowKey, dynamicValues1, dynamicValues2)

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

        and:
        recordResult.dynamicValues.collect { it.dynamicPart1 }.containsAll(["dv1_dp1_1", "dv1_dp1_2"])
        recordResult.dynamicValues.collect { it.dynamicPart2 }.containsAll(["dv1_dp2_1", "dv1_dp2_2"])
        recordResult.dynamicValues.collect { it.position }.containsAll(["dv1_position_1", "dv1_position_2"])
        recordResult.dynamicValues.collect { it.recipeId }.containsAll(["dv1_recipeId_1", "dv1_recipeId_2"])
        recordResult.dynamicValues.collect { it.html }.containsAll(["<html>dv1_html_1</html>", "<html>dv1_html_2</html>"])
    }
}
