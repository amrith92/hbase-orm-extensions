package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord;
import lombok.val;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.Optional;

import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGNS_FAMILY;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGN_PREFIX;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGN_SEPARATOR;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.DAYS_FAMILY;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.DAY_PREFIX;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.DAY_SEPARATOR;


public class RecipeCampaignActionsDao extends AbstractHBDynamicDAO<String, RecipeCampaignActionRecord> {
    public RecipeCampaignActionsDao(Connection connection) {
        super(connection);
    }

    public Optional<RecipeCampaignActionRecord> getCampaignActionByPosition(String customerId, String campaignId, Integer position) throws IOException {
        Get query = getGet(customerId);
        query.addColumn(CAMPAIGNS_FAMILY.getBytes(), getCampaignPositionColumnName(campaignId, position).getBytes());
        return Optional.ofNullable(getOnGet(query));
    }

    public static String getCampaignPositionColumnName(String campaignId, Integer position) {
        val campaignPositionQualifier = getCampaignPositionQualifier(campaignId, position);
        return String.format("%s%s%s", CAMPAIGN_PREFIX, CAMPAIGN_SEPARATOR, campaignPositionQualifier);
    }

    public static String getCampaignPositionQualifier(String campaignId, Integer position) {
        return campaignId + "#" + position;
    }

    public Optional<RecipeCampaignActionRecord> getCampaignActionByDay(final String customerId, final String cellIdentifier) throws IOException {
        Get query = getGet(customerId);
        query.addColumn(DAYS_FAMILY.getBytes(), getDayColumnName(cellIdentifier).getBytes());
        return Optional.ofNullable(getOnGet(query));
    }

    public static String getDayColumnName(String day) {
        return String.format("%s%s%s", DAY_PREFIX, DAY_SEPARATOR, day);
    }
}
