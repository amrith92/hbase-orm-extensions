package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsRecord;
import org.apache.hadoop.hbase.client.Connection;

public class MultipleDynamicColumsDao extends AbstractHBDynamicDAO<String, MultipleHBDynamicColumnsRecord> {
    public MultipleDynamicColumsDao(Connection connection) {
        super(connection);
    }
}
