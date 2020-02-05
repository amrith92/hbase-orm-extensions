package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.ValidUserRecord;
import org.apache.hadoop.hbase.client.Connection;

public class ValidUserDao extends AbstractHBDynamicDAO<String, ValidUserRecord> {

    public ValidUserDao(Connection connection) {
        super(connection);
    }

}
