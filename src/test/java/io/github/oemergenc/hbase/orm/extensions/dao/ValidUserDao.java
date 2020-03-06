package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.ValidUserRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ValidUserDao extends AbstractHBDynamicDAO<String, ValidUserRecord> {

    public ValidUserDao(Connection connection) {
        super(connection);
    }

    public Optional<ValidUserRecord> getWorkAddress(String customerId, String address) {
        try {
            Get get = getGet(customerId);
            get.addColumn("address".getBytes(), ("workAddress#" + address).getBytes());
            return Optional.ofNullable(getOnGet(get));
        } catch (IOException e) {
            log.error("There was an error while trying to get campaign data of customer", e);
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public Optional<ValidUserRecord> getHomeAddress(String customerId, String address) {
        try {
            Get get = getGet(customerId);
            get.addColumn("address".getBytes(), ("homeAddress#" + address).getBytes());
            return Optional.ofNullable(getOnGet(get));
        } catch (IOException e) {
            log.error("There was an error while trying to get campaign data of customer", e);
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
