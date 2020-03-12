package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.CAMPAIGNS_FAMILY;
import static io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord.DAYS_FAMILY;


public class RecipeCampaignActionsDao extends AbstractHBDynamicDAO<String, RecipeCampaignActionRecord> {
    public RecipeCampaignActionsDao(Connection connection) {
        super(connection);
    }

    public Optional<RecipeCampaignActionRecord> getCampaignActionByPosition(String customerId,
                                                                            String campaignId,
                                                                            Integer position) throws IOException {
        return Optional.ofNullable(getDynamicCell(customerId, CAMPAIGNS_FAMILY, List.of(campaignId, position.toString())));
    }

    public Optional<RecipeCampaignActionRecord> getCampaignActionByDay(final String customerId,
                                                                       final String cellIdentifier) throws IOException {
        return Optional.ofNullable(getDynamicCell(customerId, DAYS_FAMILY, cellIdentifier));
    }
}
