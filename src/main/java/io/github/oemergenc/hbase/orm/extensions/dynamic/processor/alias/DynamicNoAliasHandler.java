package io.github.oemergenc.hbase.orm.extensions.dynamic.processor.alias;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DynamicNoAliasHandler implements AliasHandler {

    @Override
    public String getDynamicListFieldEntryColumnName(String dynamicListFieldEntryColumnName) {
        return dynamicListFieldEntryColumnName;
    }

    @Override
    public List<Map.Entry<byte[], NavigableMap<Long, byte[]>>> getDynamicListFieldEntries(NavigableMap<byte[], NavigableMap<Long, byte[]>> navigableMapNavigableMap) {
        return new ArrayList<>(navigableMapNavigableMap.entrySet());
    }
}
