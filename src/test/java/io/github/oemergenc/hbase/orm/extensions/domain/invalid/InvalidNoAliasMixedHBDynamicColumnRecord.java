package io.github.oemergenc.hbase.orm.extensions.domain.invalid;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import io.github.oemergenc.hbase.orm.extensions.domain.dto.DependentWithMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@HBTable(name = "invalid_no_alias_mixed_column_table", families = {
        @Family(name = "staticFamily"),
        @Family(name = "invalidNoAliasMixedDynamicFamily")
})
public class InvalidNoAliasMixedHBDynamicColumnRecord implements HBRecord<String> {

    @HBRowKey
    @HBColumn(family = "staticFamily", column = "staticId")
    private String staticId;

    @HBDynamicColumn(family = "invalidNoAliasMixedDynamicFamily", qualifier = @DynamicQualifier(parts = {"dynamicId"}))
    private List<DependentWithMap> noAliasDynamicFamily1;

    @HBDynamicColumn(family = "invalidNoAliasMixedDynamicFamily", alias = "id", qualifier = @DynamicQualifier(parts = {"dynamicId2"}))
    private List<DependentWithMap> noAliasDynamicFamily2;

    @Override
    public String composeRowKey() {
        return staticId;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.staticId = rowKey;
    }
}
