package io.github.oemergenc.hbase.orm.extensions.domain.records;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithListType;
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithPrimitiveTypes;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@HBTable(name = "multiple_dynamic_columns_table", families = {
        @Family(name = "dynamicFamily1"),
        @Family(name = "dynamicFamily2"),
        @Family(name = "staticFamily")
})
public class MultipleHBDynamicColumnsRecord implements HBRecord<String> {

    @HBRowKey
    @HBColumn(family = "staticFamily", column = "staticId")
    private String staticId;

    @HBDynamicColumn(family = "dynamicFamily1", alias = "df1", qualifier = @DynamicQualifier(parts = {"dynamicPart1", "dynamicPart2"}))
    private List<DependentWithPrimitiveTypes> dynamicValues1;

    @HBDynamicColumn(family = "dynamicFamily2", alias = "df2", qualifier = @DynamicQualifier(parts = {"dynamicPart1"}))
    private List<DependentWithListType> dynamicValues2;

    @HBDynamicColumn(family = "dynamicFamily3", alias = "df3", qualifier = @DynamicQualifier(parts = {"dynamicPart1", "dynamicPart2"}))
    private List<DependentWithPrimitiveTypes> dynamicValues3;

    @HBDynamicColumn(family = "dynamicFamily4", alias = "df4", qualifier = @DynamicQualifier(parts = {"dynamicPart1"}))
    private List<DependentWithPrimitiveTypes> dynamicValues4;

    @Override
    public String composeRowKey() {
        return staticId;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.staticId = rowKey;
    }
}
