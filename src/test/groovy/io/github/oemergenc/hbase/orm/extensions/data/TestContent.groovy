package io.github.oemergenc.hbase.orm.extensions.data

import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithPrimitiveTypes

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
}
