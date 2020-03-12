package io.github.oemergenc.hbase.orm.extensions.domain.records;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithListType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@HBTable(name = "multiple_dynamic_columns_table", families = {
        @Family(name = "dynamicFamily1"),
        @Family(name = "staticFamily")})
public class MultipleHBDynamicColumnsWithSameFamilyRecord implements HBRecord<String> {

    @HBRowKey
    @HBColumn(family = "staticFamily", column = "staticId")
    private String staticId;

    @HBDynamicColumn(family = "dynamicFamily1", alias = "df1", qualifier = @DynamicQualifier(parts = {"dynamicPart1", "otherId"}))
    private List<DependentWithListType> dynamicValues1;

    @HBDynamicColumn(family = "dynamicFamily1", alias = "df2", qualifier = @DynamicQualifier(parts = {"dynamicPart1", "otherId"}))
    private List<DependentWithListType> dynamicValues2;

    @Override
    public String composeRowKey() {
        return staticId;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.staticId = rowKey;
    }
}
