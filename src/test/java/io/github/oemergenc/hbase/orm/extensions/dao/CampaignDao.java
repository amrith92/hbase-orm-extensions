package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class CampaignDao extends AbstractHBDynamicDAO<String, CampaignRecord> {

    public CampaignDao(Connection connection) {
        super(connection);
    }

    public Optional<CampaignRecord> getCampaign(String customerId, String campaignId) {
        try {
            Get get = getGet(customerId);
            get.addColumn("campaign".getBytes(), ("id#" + campaignId).getBytes());
            return Optional.ofNullable(getOnGet(get));
        } catch (IOException e) {
            log.error("There was an error while trying to get campaign data of customer", e);
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
