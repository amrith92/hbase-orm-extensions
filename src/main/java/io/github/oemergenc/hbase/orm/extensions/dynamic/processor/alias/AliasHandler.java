package io.github.oemergenc.hbase.orm.extensions.dynamic.processor.alias;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public interface AliasHandler {
    String getDynamicListFieldEntryColumnName(String dynamicListFieldEntryColumnName);

    List<Map.Entry<byte[], NavigableMap<Long, byte[]>>> getDynamicListFieldEntries(NavigableMap<byte[], NavigableMap<Long, byte[]>> navigableMapNavigableMap);
}
