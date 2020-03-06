package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
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
import java.util.Set;

import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGNS_FAMILY;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGNS_TABLENAME;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.OPTIONAL_FAMILY;


@Data
@AllArgsConstructor
@NoArgsConstructor
@HBTable(name = CAMPAIGNS_TABLENAME,
        families = {@Family(name = CAMPAIGNS_FAMILY), @Family(name = OPTIONAL_FAMILY)})
public class RecipeCampaignActionRecord implements HBRecord<String> {
    public static final String CAMPAIGNS_TABLENAME = "bd_omm_prp_campaigns";
    public static final String CAMPAIGNS_FAMILY = "campaigns";
    public static final String DAYS_FAMILY = "days";
    public static final String OPTIONAL_FAMILY = "optional";
    public static final String CAMPAIGN_PREFIX = "cp";
    public static final String DAY_PREFIX = "d";
    public static final String CAMPAIGN_SEPARATOR = "#";
    public static final Set<String> EXPECTED_COLUMN_FAMILIES = Set.of(CAMPAIGNS_FAMILY, OPTIONAL_FAMILY, DAYS_FAMILY);

    @HBRowKey
    @HBColumn(family = OPTIONAL_FAMILY, column = "customerPid")
    private String customerPid;

    @HBDynamicColumn(family = CAMPAIGNS_FAMILY, alias = CAMPAIGN_PREFIX,
            qualifier = @DynamicQualifier(parts = {"campaignPosition"}, composer = "composeCampaignQualifier", parser = "parseCampaignQualifier"))
    private List<RecipeTile> recipeTiles;

    @HBDynamicColumn(family = DAYS_FAMILY, alias = DAY_PREFIX,
            qualifier = @DynamicQualifier(parts = {"dayId"}, composer = "composeDaysQualifier", parser = "parseDaysQualifier"))
    private List<ContentRecipeJsonDto> jsonRecipes;

    @Override
    public String composeRowKey() {
        return customerPid;
    }

    @Override
    public void parseRowKey(String rowKey) {
        this.customerPid = rowKey;
    }
}
