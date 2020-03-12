package io.github.oemergenc.hbase.orm.extensions;

import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractHBDynamicDAO<R extends Serializable & Comparable<R>, T extends HBRecord<R>>
        extends AbstractHBDAO<R, T> {

    private final HBDynamicColumnObjectMapper hbDynamicColumnObjectMapper;

    protected AbstractHBDynamicDAO(Connection connection) {
        super(connection);
        hbDynamicColumnObjectMapper = new HBDynamicColumnObjectMapper(new BestSuitCodec());
        hbDynamicColumnObjectMapper.validate(hbRecordClass);
    }

    public T getDynamicCell(R rowKey, String family, String cellIdentifier) throws IOException {
        return getDynamicCell(rowKey, family, List.of(cellIdentifier));
    }

    public T getDynamicCell(R rowKey, String family, List<String> qualifierParts) throws IOException {
        Get get = hbDynamicColumnObjectMapper.getAsGet(hbRecordClass, toBytes(rowKey), family, qualifierParts);
        return getOnGet(get);
    }

    public T get(R rowKey) throws IOException {
        return get(rowKey, 1);
    }

    public T get(R rowKey, int numVersionsToFetch) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.get(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
            return hbDynamicColumnObjectMapper.readValue(result, hbRecordClass);
        }
    }

    public T getOnGet(Get get) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.get(get);
            return hbDynamicColumnObjectMapper.readValue(result, hbRecordClass);
        }
    }

    @SuppressWarnings("unused")
    public List<T> getOnGets(List<Get> gets) throws IOException {
        List<T> records = new ArrayList<>(gets.size());
        try (Table table = getHBaseTable()) {
            Result[] results = table.get(gets);
            for (Result result : results) {
                records.add(hbDynamicColumnObjectMapper.readValue(result, hbRecordClass));
            }
        }
        return records;
    }


    public R persist(HBRecord<R> record) throws IOException {
        Put put = hbDynamicColumnObjectMapper.writeValueAsPut(record);
        try (Table table = getHBaseTable()) {
            table.put(put);
            return record.composeRowKey();
        }
    }

    public List<R> persist(List<T> records) throws IOException {
        List<Put> puts = new ArrayList<>(records.size());
        List<R> rowKeys = new ArrayList<>(records.size());
        for (HBRecord<R> object : records) {
            puts.add(hbDynamicColumnObjectMapper.writeValueAsPut(object));
            rowKeys.add(object.composeRowKey());
        }
        try (Table table = getHBaseTable()) {
            table.put(puts);
        }
        return rowKeys;
    }
}
