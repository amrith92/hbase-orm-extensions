package io.github.oemergenc.hbase.orm.extensions.data

import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithListType
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithPrimitiveTypes
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsRecord

class TestContent {
    static def getStubDependend() {
        getStubDependend([
                dynamicPart1: "dv1_dp1_2",
                dynamicPart2: "d1_dp2_2",
                position    : "dv1_position_2",
                recipeId    : "dv1_recipeId_2",
                html        : "<html>dv1_html_2</html>"
        ])
    }

    static def getStubDependend(Map params) {
        Map values = [
                dynamicPart1: "dv1_dp1_2",
                dynamicPart2: "dv1_dp2_2",
                position    : "dv1_position_2",
                recipeId    : "dv1_recipeId_2",
                html        : "<html>dv1_html_2</html>"
        ] << params
        new DependentWithPrimitiveTypes(
                values.dynamicPart1,
                values.dynamicPart2,
                values.position,
                values.recipeId,
                values.html)
    }

    static def getDefaultMultiHBDynamicColumRecord(String staticId) {
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
        record
    }
}
