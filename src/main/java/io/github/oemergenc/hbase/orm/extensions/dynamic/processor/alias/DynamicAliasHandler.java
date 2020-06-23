package io.github.oemergenc.hbase.orm.extensions.dynamic.processor.alias;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.exceptions.CodecException;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class DynamicAliasHandler implements AliasHandler {
    private final String alias;
    private final String prefix;
    private String separator;
    private Codec codec;

    public DynamicAliasHandler(String alias,
                               String separator,
                               Codec codec) {
        this.alias = alias;
        this.separator = separator;
        this.codec = codec;
        this.prefix = alias.concat(separator);
    }

    @Override
    public String getDynamicListFieldEntryColumnName(String dynamicListFieldEntryColumnName) {
        return prefix.concat(dynamicListFieldEntryColumnName);
    }

    @Override
    public List<Map.Entry<byte[], NavigableMap<Long, byte[]>>> getDynamicListFieldEntries(NavigableMap<byte[], NavigableMap<Long, byte[]>> navigableMapNavigableMap) {
        return navigableMapNavigableMap.entrySet().stream()
                .filter(navigableMapEntry -> {
                    Serializable serializable = byteArrayToValue(navigableMapEntry.getKey());
                    return serializable.toString().startsWith(prefix);
                }).collect(Collectors.toList());
    }

    Serializable byteArrayToValue(byte[] value) {
        try {
            return codec.deserialize(value, String.class, emptyMap());
        } catch (DeserializationException e) {
            throw new CodecException("Couldn't deserialize", e);
        }
    }
}
