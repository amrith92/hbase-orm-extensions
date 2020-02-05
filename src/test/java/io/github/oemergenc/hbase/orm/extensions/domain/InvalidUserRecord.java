package io.github.oemergenc.hbase.orm.extensions.domain;

import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@HBTable(name = "users", families = {@Family(name = "address")})
public class InvalidUserRecord implements HBRecord<String> {

    @HBRowKey
    @HBColumn(family = "optional", column = "userId")
    private String userId;

    @HBDynamicColumn(family = "addresses", qualifierField = "address")
    private List<Address> workAddresses;

    @HBDynamicColumn(family = "addresses", qualifierField = "address")
    private List<Address> homeAddresses;

    @Override
    public String composeRowKey() {
        return userId;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.userId = rowKey;
    }
}
