package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.records.NoAliasMixedHBDynamicColumnRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class NoAliasMixedDynamicColumsDao extends AbstractHBDynamicDAO<String, NoAliasMixedHBDynamicColumnRecord> {
    public NoAliasMixedDynamicColumsDao(Connection connection) {
        super(connection);
    }
}
