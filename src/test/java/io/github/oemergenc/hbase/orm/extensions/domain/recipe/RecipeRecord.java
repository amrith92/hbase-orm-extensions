package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@HBTable(name = "campaigns", families = {@Family(name = "campaign")})
public class RecipeRecord implements HBRecord<String> {
    public static final String ROW_KEY_DELIMITER = "#";
    public static final String ROW_PREFIX = "pfx";
    public static final String ROW_KEY_TEMPLATE = ROW_PREFIX + ROW_KEY_DELIMITER + "%s";
    public static final String CAMPAIGN_CF = "campaign";

    @HBRowKey
    @HBColumn(family = "campaign", column = "customerId")
    private String customerId;

    @HBDynamicColumn(family = "campaign",
            alias = "id",
            qualifier = @DynamicQualifier(parts = {"campaignId"}, composer = "composeCampaignQualifier", parser = "parseCampaignQualifier"))
    private List<Campaign> campaigns;

    @Override
    public String composeRowKey() {
        return String.format(ROW_KEY_TEMPLATE, customerId);
    }

    @Override
    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split(ROW_KEY_DELIMITER);
        this.customerId = pieces[1];
    }
}
