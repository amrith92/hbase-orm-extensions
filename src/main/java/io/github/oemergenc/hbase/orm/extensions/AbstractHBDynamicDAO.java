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
import java.util.Arrays;
import java.util.List;

public abstract class AbstractHBDynamicDAO<R extends Serializable & Comparable<R>, T extends HBRecord<R>> extends AbstractHBDAO<R, T> {

    private final HBDynamicColumnObjectMapper hbDynamicColumnObjectMapper;

    protected AbstractHBDynamicDAO(Connection connection) {
        super(connection);
        hbDynamicColumnObjectMapper = new HBDynamicColumnObjectMapper(new BestSuitCodec());
    }

    public T getDynamic(R rowKey) throws IOException {
        return getDynamic(rowKey, 1);
    }

    public T getDynamic(R rowKey, int numVersionsToFetch) throws IOException {
        try (Table table = getHBaseTable()) {
            Result result = table.get(new Get(toBytes(rowKey)).readVersions(numVersionsToFetch));
            return hbDynamicColumnObjectMapper.readValue(result, hbRecordClass);
        }
    }

    public R persist(HBRecord<R> record) throws IOException {
        Put put = hbObjectMapper.writeValueAsPut(record);
        Put putDynamic = hbDynamicColumnObjectMapper.writeValueAsPut(record);
        try (Table table = getHBaseTable()) {
            table.put(Arrays.asList(put, putDynamic));
            return record.composeRowKey();
        }
    }

    public List<R> persist(List<T> records) throws IOException {
        List<Put> puts = new ArrayList<>(records.size());
        List<R> rowKeys = new ArrayList<>(records.size());
        for (HBRecord<R> object : records) {
            puts.add(hbObjectMapper.writeValueAsPut(object));
            puts.add(hbDynamicColumnObjectMapper.writeValueAsPut(object));
            rowKeys.add(object.composeRowKey());
        }
        try (Table table = getHBaseTable()) {
            table.put(puts);
        }
        return rowKeys;
    }
}
