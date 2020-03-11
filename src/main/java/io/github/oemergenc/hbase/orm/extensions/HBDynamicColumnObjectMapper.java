package io.github.oemergenc.hbase.orm.extensions;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.CodecException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeComposedException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeEmptyException;
import io.github.oemergenc.hbase.orm.extensions.exception.InvalidColumnQualifierFieldException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.vidageek.mirror.dsl.Mirror;
import net.vidageek.mirror.list.dsl.MirrorList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.function.Predicate.not;

@Slf4j
public class HBDynamicColumnObjectMapper extends HBObjectMapper {

    private static final BestSuitCodec CODEC = new BestSuitCodec();
    private static Mirror MIRROR = new Mirror();
    private final Codec codec;

    public HBDynamicColumnObjectMapper(Codec codec) {
        super(codec);
        this.codec = codec;
    }

    public HBDynamicColumnObjectMapper() {
        this(CODEC);
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    Put writeValueAsPut(HBRecord<R> record) {
        Put put = super.writeValueAsPut(record);
        val familyToQualifierList = convertRecordToMap(record);
        for (val familyToQualifierMap : familyToQualifierList) {
            for (val e : familyToQualifierMap.entrySet()) {
                val columnFamily = e.getKey();
                val columnQualifierToColumnValueMap = e.getValue();
                for (val columnQualifierToColumnValueEntry : columnQualifierToColumnValueMap.entrySet()) {
                    byte[] columnFamilyBytes = valueToByteArray(columnFamily);
                    byte[] columnQualifierBytes = valueToByteArray(columnQualifierToColumnValueEntry.getKey());
                    Object value = columnQualifierToColumnValueEntry.getValue();
                    put.addColumn(columnFamilyBytes, columnQualifierBytes, valueToByteArray(safeCast(value, Serializable.class)));
                }
            }
        }
        return put;
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    T readValue(Result result, Class<T> clazz) {
        T t = super.readValue(result, clazz);
        if (t != null) {
            convertMapToRecord(result.getMap(), clazz, t);
        }
        return t;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    void convertMapToRecord(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> hbRecordClazz, T record) {
        val hbDynamicColumnFields = MIRROR.on(hbRecordClazz).reflectAll().fields()
                .matching(element -> element.getAnnotation(HBDynamicColumn.class) != null);

        for (Field dynamicField : hbDynamicColumnFields) {
            val genericTypeOfList = (ParameterizedType) dynamicField.getGenericType();
            val genericObjectType = genericTypeOfList.getActualTypeArguments()[0];
            HBDynamicColumn hbDynamicColumn = MIRROR.on(hbRecordClazz).reflect()
                    .annotation(HBDynamicColumn.class).atField(dynamicField.getName());
            String family = hbDynamicColumn.family();
            String alias = Optional.of(hbDynamicColumn.alias()).filter(not(String::isBlank)).orElse(family);
            String separator = hbDynamicColumn.separator();
            String prefix = alias.concat(separator);
            byte[] columnFamilyBytes = valueToByteArray(hbDynamicColumn.family());
            val navigableMapNavigableMap = map.get(columnFamilyBytes);
            if (navigableMapNavigableMap != null) {
                val collect = navigableMapNavigableMap.entrySet().stream()
                        .filter(navigableMapEntry -> {
                            Serializable serializable = byteArrayToValue(navigableMapEntry.getKey());
                            return serializable.toString().startsWith(prefix);
                        }).collect(Collectors.toList());
                List<Serializable> genericValues = new ArrayList<>();
                for (val nav : collect) {
                    val value = nav.getValue();
                    Collection<byte[]> values = value.values();
                    for (val theVal : values) {
                        byteArrayToGenericObject(genericObjectType, theVal)
                                .ifPresent(genericValues::add);
                    }
                }
                MIRROR.on(record).set().field(dynamicField).withValue(genericValues);
            }
        }
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    Result writeValueAsResult(HBRecord<R> record) {
        val result = super.writeValueAsResult(record);
        val row = composeRowKey(record);
        val cells = result.listCells();
        List<Cell> cellList = new ArrayList<>();
        val columnFamilyToQualifierObjectList = convertRecordToMap(record);
        for (val columnFamilyToQualifierObject : columnFamilyToQualifierObjectList) {
            for (val columnFamilyToQualifierEntry : columnFamilyToQualifierObject.entrySet()) {
                val family = columnFamilyToQualifierEntry.getKey();
                for (val e : columnFamilyToQualifierEntry.getValue().entrySet()) {
                    val columnQualifier = e.getKey();
                    Object columnValue = e.getValue();
                    Optional.ofNullable(safeCast(columnValue, Serializable.class))
                            .ifPresent(serializableColumnValue -> {
                                        val cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
                                        cellBuilder.setType(Cell.Type.Put)
                                                .setRow(row)
                                                .setFamily(valueToByteArray(family))
                                                .setQualifier(valueToByteArray(columnQualifier))
                                                .setValue(valueToByteArray(serializableColumnValue));
                                        Cell cell = cellBuilder.build();
                                        cellList.add(cell);
                                    }
                            );
                }
            }
        }
        cellList.addAll(cells);
        return Result.create(cellList);
    }

    private List<Map<String, HashMap<String, Object>>> convertRecordToMap(HBRecord<?> record) {
        Class<?> clazz = record.getClass();
        MirrorList<Field> hbDynamicColumnFields = MIRROR.on(clazz).reflectAll().fields()
                .matching(element -> element.getAnnotation(HBDynamicColumn.class) != null);
        val familyToQualifierList = new ArrayList<Map<String, HashMap<String, Object>>>();
        for (Field dynamicField : hbDynamicColumnFields) {
            val familyToQualifierMap = processDynamicField(dynamicField, record);
            familyToQualifierList.add(familyToQualifierMap);
        }
        return familyToQualifierList;
    }

    private Map<String, HashMap<String, Object>> processDynamicField(Field dynamicField,
                                                                     HBRecord<?> record) {
        Class<?> hbRecordClazz = record.getClass();
        HBDynamicColumn hbDynamicColumn = MIRROR.on(hbRecordClazz).reflect()
                .annotation(HBDynamicColumn.class).atField(dynamicField.getName());
        String family = hbDynamicColumn.family();
        String alias = Optional.of(hbDynamicColumn.alias()).filter(not(String::isBlank)).orElse(family);
        String separator = hbDynamicColumn.separator();
        String prefix = alias.concat(separator);
        DynamicQualifier dynamicQualifier = hbDynamicColumn.qualifier();
        val familyToQualifierMap = new HashMap<String, HashMap<String, Object>>();
        Object field = MIRROR.on(record).get().field(dynamicField);
        if (field != null) {
            if (field instanceof List) {
                List<?> dynamicList = safeCast(field, List.class);
                val columnQualifierToColumnValueMap = processDynamicListField(prefix, dynamicList, dynamicQualifier);
                familyToQualifierMap.put(family, columnQualifierToColumnValueMap);
            }
        } else {
            log.debug("A dynamic field value was null and will be ignored");
        }
        return familyToQualifierMap;
    }

    private HashMap<String, Object> processDynamicListField(String prefix,
                                                            List<?> dynamicList,
                                                            DynamicQualifier dynamicQualifier) {
        val columnQualifierToEntryMap = new HashMap<String, Object>();
        for (Object dynamicListEntry : dynamicList) {
            try {
                String dynamicListFieldEntryColumnName = processDynamicListFieldEntry(dynamicListEntry, dynamicQualifier);
                dynamicListFieldEntryColumnName = prefix.concat(dynamicListFieldEntryColumnName);
                columnQualifierToEntryMap.put(dynamicListFieldEntryColumnName, dynamicListEntry);
            } catch (InvalidColumnQualifierFieldException ex) {
                ex.printStackTrace();
            }
        }
        return columnQualifierToEntryMap;
    }

    private String processDynamicListFieldEntry(Object dynamicListEntry,
                                                DynamicQualifier dynamicQualifier) {
        String[] parts = dynamicQualifier.parts();
        String separator = dynamicQualifier.separator();
        List<Object> columnQualifierPartList = new ArrayList<>();
        for (String part : parts) {
            Object columnQualifierPart = MIRROR.on(dynamicListEntry).get().field(part);
            if (columnQualifierPart != null) {
                columnQualifierPartList.add(columnQualifierPart);
            } else {
                throw new InvalidColumnQualifierFieldException(part, dynamicListEntry);
            }
        }

        return columnQualifierPartList.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(separator));
    }

    byte[] valueToByteArray(Serializable value) {
        return valueToByteArray(value, emptyMap());
    }

    byte[] valueToByteArray(Serializable value, Map<String, String> codecFlags) {
        try {
            return codec.serialize(value, codecFlags);
        } catch (SerializationException e) {
            throw new CodecException("Couldn't serialize", e);
        }
    }

    Serializable byteArrayToValue(byte[] value) {
        try {
            return codec.deserialize(value, String.class, emptyMap());
        } catch (DeserializationException e) {
            throw new CodecException("Couldn't deserialize", e);
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>>
    byte[] composeRowKey(HBRecord<R> record) {
        R rowKey;
        try {
            rowKey = record.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.toString().isEmpty()) {
            throw new RowKeyCantBeEmptyException();
        }
        return valueToByteArray(rowKey, emptyMap());
    }

    private Optional<Serializable> byteArrayToGenericObject(Type genericObjectType, byte[] bytes) {
        try {
            val deserialize = codec.deserialize(bytes, genericObjectType, emptyMap());
            return Optional.ofNullable(deserialize);
        } catch (DeserializationException e) {
            log.error("There was an invalid qualifier field value, which will be ignored", e);
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public static <T> T safeCast(Object o, Class<T> clazz) {
        return clazz != null && clazz.isInstance(o) ? clazz.cast(o) : null;
    }
}
