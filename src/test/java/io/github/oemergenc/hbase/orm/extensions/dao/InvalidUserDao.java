package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.InvalidUserRecord;
import org.apache.hadoop.hbase.client.Connection;

public class InvalidUserDao extends AbstractHBDynamicDAO<String, InvalidUserRecord> {

    public InvalidUserDao(Connection connection) {
        super(connection);
    }

}
