package io.github.oemergenc.hbase.orm.extensions.dao;


import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord;
import org.apache.hadoop.hbase.client.Connection;


public class CampaignDao extends AbstractHBDynamicDAO<String, CampaignRecord> {

    public CampaignDao(Connection connection) {
        super(connection);
    }

}
