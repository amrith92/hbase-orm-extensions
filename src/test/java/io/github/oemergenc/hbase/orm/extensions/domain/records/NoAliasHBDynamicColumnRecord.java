package io.github.oemergenc.hbase.orm.extensions.domain.records;

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
@HBTable(name = "no_alias_column_table", families = {
        @Family(name = "staticFamily"),
        @Family(name = "noAliasDynamicFamily")
})
public class NoAliasHBDynamicColumnRecord implements HBRecord<String> {

    @HBRowKey
    @HBColumn(family = "staticFamily", column = "staticId")
    private String staticId;

    @HBDynamicColumn(family = "noAliasDynamicFamily", qualifier = @DynamicQualifier(parts = {"dynamicId"}))
    private List<DependentWithMap> noAliasDynamicFamily;

    @Override
    public String composeRowKey() {
        return staticId;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.staticId = rowKey;
    }
}
