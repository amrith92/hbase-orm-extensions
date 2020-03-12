package io.github.oemergenc.hbase.orm.extensions.dao;

import io.github.oemergenc.hbase.orm.extensions.AbstractHBDynamicDAO;
import io.github.oemergenc.hbase.orm.extensions.domain.records.MultipleHBDynamicColumnsWithSameFamilyRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Optional;

@Slf4j
public class MultipleDynamicColumsWithSameFamilyDao extends AbstractHBDynamicDAO<String, MultipleHBDynamicColumnsWithSameFamilyRecord> {
    public MultipleDynamicColumsWithSameFamilyDao(Connection connection) {
        super(connection);
    }

    public Optional<MultipleHBDynamicColumnsWithSameFamilyRecord> getRecordForDynamicFamily(String rowKey,
                                                                                            List<String> qualifierParts) {
        try {
            return Optional.ofNullable(getDynamicCell(rowKey, "dynamicFamily1", qualifierParts));
        } catch (Exception e) {
            log.error("There was an error while trying to get column family data of hbdynamic column", e);
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
