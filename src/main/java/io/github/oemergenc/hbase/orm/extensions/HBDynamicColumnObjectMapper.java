package io.github.oemergenc.hbase.orm.extensions;

import com.flipkart.hbaseobjectmapper.HBObjectMapper;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.MappedSuperClass;
import com.flipkart.hbaseobjectmapper.codec.BestSuitCodec;
import com.flipkart.hbaseobjectmapper.codec.Codec;
import com.flipkart.hbaseobjectmapper.codec.exceptions.DeserializationException;
import com.flipkart.hbaseobjectmapper.codec.exceptions.SerializationException;
import com.flipkart.hbaseobjectmapper.exceptions.AllHBColumnFieldsNullException;
import com.flipkart.hbaseobjectmapper.exceptions.BadHBaseLibStateException;
import com.flipkart.hbaseobjectmapper.exceptions.CodecException;
import com.flipkart.hbaseobjectmapper.exceptions.EmptyConstructorInaccessibleException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBePrimitiveException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeStaticException;
import com.flipkart.hbaseobjectmapper.exceptions.MappedColumnCantBeTransientException;
import com.flipkart.hbaseobjectmapper.exceptions.MissingHBColumnFieldsException;
import com.flipkart.hbaseobjectmapper.exceptions.NoEmptyConstructorException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeComposedException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCantBeEmptyException;
import com.flipkart.hbaseobjectmapper.exceptions.UnsupportedFieldTypeException;
import lombok.val;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;

public class HBDynamicColumnObjectMapper extends HBObjectMapper {

    private static final BestSuitCodec CODEC = new BestSuitCodec();
    private final Codec codec;

    public HBDynamicColumnObjectMapper(Codec codec) {
        super(codec);
        this.codec = codec;
    }

    public HBDynamicColumnObjectMapper() {
        super(CODEC);
        this.codec = CODEC;
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Put writeValueAsPut(HBRecord<R> record) {
        validateHBDynamicClass(((Class<T>) record.getClass()));
        Put put = super.writeValueAsPut(record);
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> columnValuesVersioned = e.getValue();
                if (columnValuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> versionAndValue : columnValuesVersioned.entrySet()) {
                    put.addColumn(family, columnName, versionAndValue.getKey(), versionAndValue.getValue());
                }
            }
        }
        return put;
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Result result, Class<T> clazz) {
        validateHBDynamicClass(clazz);
        T t = super.readValue(result, clazz);
        convertMapToRecord(result.getRow(), result.getMap(), clazz, t);
        return t;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void convertMapToRecord(byte[] rowKeyBytes, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> clazz, T record) {
        Collection<Field> fields = getHBDynamicColumnFields0(clazz).values();
        for (Field dynamicField : fields) {
            ParameterizedType genericTypeOfList = (ParameterizedType) dynamicField.getGenericType();
            Class<?> genericObjectType = (Class<?>) genericTypeOfList.getActualTypeArguments()[0];
            WrappedHBDynamicColumn hbDynamicColumn = new WrappedHBDynamicColumn(dynamicField);
            if (hbDynamicColumn.isPresent()) {
                NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(hbDynamicColumn.familyBytes());
                if (familyMap == null || familyMap.isEmpty()) {
                    continue;
                }
                List<byte[]> dynamicColumnBytesList = familyMap
                        .keySet()
                        .stream()
                        .filter(bytes -> {
                            try {
                                String s = (String) CODEC.deserialize(bytes, String.class, EMPTY_MAP);
                                return s.startsWith(hbDynamicColumn.getPrefix());
                            } catch (DeserializationException e) {
                                e.printStackTrace();
                            }
                            return false;
                        }).collect(Collectors.toList());
                List<Serializable> dynamicListMembers = new ArrayList<>();
                for (byte[] dynamicColumnBytes : dynamicColumnBytesList) {
                    NavigableMap<Long, byte[]> columnVersionsMap = familyMap.get(dynamicColumnBytes);
                    Map.Entry<Long, byte[]> lastEntry = columnVersionsMap.lastEntry();
                    try {
                        Serializable deserialize = codec.deserialize(lastEntry.getValue(), genericObjectType, EMPTY_MAP);
                        dynamicListMembers.add(deserialize);
                    } catch (DeserializationException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    dynamicField.setAccessible(true);
                    dynamicField.set(record, dynamicListMembers);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Map<String, Field> getHBDynamicColumnFields0(Class<T> clazz) {
        Map<String, Field> mappings = new LinkedHashMap<>();
        Class<?> thisClass = clazz;
        while (thisClass != null && thisClass != Object.class) {
            for (Field field : thisClass.getDeclaredFields()) {
                if (new WrappedHBDynamicColumn(field).isPresent()) {
                    mappings.put(field.getName(), field);
                }
            }
            Class<?> parentClass = thisClass.getSuperclass();
            thisClass = parentClass.isAnnotationPresent(MappedSuperClass.class) ? parentClass : null;
        }
        return mappings;
    }

    List<String> getHBDynamicColumnNames(Field field, String columnQualifierSelector, HBRecord record) {
        try {
            Field declaredField = record.getClass().getDeclaredField(field.getName());
            declaredField.setAccessible(true);
            Object qualifierObject = declaredField.get(record);
            if (!Collection.class.isAssignableFrom(qualifierObject.getClass())) {
                throw new RuntimeException("HBDynamicColumn Field must be a collection, but was " + qualifierObject.getClass().getSimpleName());
            }
            Collection col = (Collection) qualifierObject;
            ParameterizedType stringListType = (ParameterizedType) declaredField.getGenericType();
            Class<?> campaign = (Class<?>) stringListType.getActualTypeArguments()[0];
            Field qualifierField = campaign.getDeclaredField(columnQualifierSelector);
            return (List<String>) col.stream()
                    .map(o -> {
                        try {
                            Field declaredField1 = o.getClass().getDeclaredField(qualifierField.getName());
                            declaredField1.setAccessible(true);
                            return declaredField1.get(o);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .collect(Collectors.toList());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return Collections.EMPTY_LIST;
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Result writeValueAsResult(HBRecord<R> record) {
        validateHBDynamicClass((Class<T>) record.getClass());
        Result result = super.writeValueAsResult(record);
        byte[] row = composeRowKey(record);
        List<Cell> cells = result.listCells();
        List<Cell> cellList = new ArrayList<>();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> fe : convertRecordToMap(record).entrySet()) {
            byte[] family = fe.getKey();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> e : fe.getValue().entrySet()) {
                byte[] columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null)
                    continue;
                for (Map.Entry<Long, byte[]> columnVersion : valuesVersioned.entrySet()) {
                    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
                    cellBuilder.setType(Cell.Type.Put).setRow(row).setFamily(family).setQualifier(columnName).setTimestamp(columnVersion.getKey()).setValue(columnVersion.getValue());
                    Cell cell = cellBuilder.build();
                    cellList.add(cell);
                }
            }
        }
        cellList.addAll(cells);
        return Result.create(cellList);
    }

    @SuppressWarnings("unchecked")
    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> convertRecordToMap(HBRecord<R> record) {
        Class<T> clazz = (Class<T>) record.getClass();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        Collection<Field> dynamicfields = getHBDynamicColumnFields0(clazz).values();
        for (Field dynamicField : dynamicfields) {
            WrappedHBDynamicColumn hbColumn = new WrappedHBDynamicColumn(dynamicField);
            if (hbColumn.isPresent()) {
                val hbDynamicColumnNames = getHBDynamicColumnNames(dynamicField, hbColumn.columnQualifierSelector(), record);
                for (val columnName : hbDynamicColumnNames) {
                    byte[] familyName = hbColumn.familyBytes();
                    byte[] columnNameBytes = hbColumn.columnBytes(columnName);
                    if (!map.containsKey(familyName)) {
                        map.put(familyName, new TreeMap<>(Bytes.BYTES_COMPARATOR));
                    }
                    Map<byte[], NavigableMap<Long, byte[]>> columns = map.get(familyName);
                    final byte[] fieldValueBytes = getListFieldValueAsBytes(record, dynamicField, hbColumn.columnQualifierSelector(), columnName, Collections.emptyMap());
                    if (fieldValueBytes == null || fieldValueBytes.length == 0) {
                        continue;
                    }
                    NavigableMap<Long, byte[]> singleValue = new TreeMap<>();
                    singleValue.put(HConstants.LATEST_TIMESTAMP, fieldValueBytes);
                    columns.put(columnNameBytes, singleValue);
                    numOfFieldsToWrite++;
                }
            }
        }
        if (numOfFieldsToWrite == 0) {
            throw new AllHBColumnFieldsNullException();
        }
        return map;
    }

    <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void validateHBDynamicClass(Class<T> clazz) {
        Constructor<?> constructor;
        try {
            constructor = clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new NoEmptyConstructorException(clazz, e);
        }
        if (!Modifier.isPublic(constructor.getModifiers())) {
            throw new EmptyConstructorInaccessibleException(String.format("Empty constructor of class %s is inaccessible. It needs to be public.", clazz.getName()));
        }
        int numOfHBColumns = 0, numOfHBRowKeys = 0;
        Map<String, Field> hbDynamicColumnFields = getHBDynamicColumnFields0(clazz);
        for (Field field : hbDynamicColumnFields.values()) {
            WrappedHBDynamicColumn hbDynamicColumn = new WrappedHBDynamicColumn(field);
            if (hbDynamicColumn.isPresent()) {
                validateHBDynamicColumnField(field);
                numOfHBColumns++;
            }
        }
        if (numOfHBColumns == 0) {
            throw new MissingHBColumnFieldsException(clazz);
        }
    }


    private void validateHBDynamicColumnField(Field field) {
        WrappedHBDynamicColumn hbColumn = new WrappedHBDynamicColumn(field);
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers)) {
            throw new MappedColumnCantBeTransientException(field, hbColumn.getName());
        }
        if (Modifier.isStatic(modifiers)) {
            throw new MappedColumnCantBeStaticException(field, hbColumn.getName());
        }
        Type fieldType = getFieldType(field, false);
        if (fieldType instanceof Class) {
            Class<?> fieldClazz = (Class<?>) fieldType;
            if (fieldClazz.isPrimitive()) {
                throw new MappedColumnCantBePrimitiveException(String.format("Field %s in class %s is a primitive of type %s (Primitive data types are not supported as they're not nullable)", field.getName(), field.getDeclaringClass().getName(), fieldClazz.getName()));
            }
        }
        if (!codec.canDeserialize(fieldType)) {
            throw new UnsupportedFieldTypeException(String.format("Field %s in class %s is of unsupported type (%s)", field.getName(), field.getDeclaringClass().getName(), fieldType));
        }
    }

    Type getFieldType(Field field, boolean isMultiVersioned) {
        if (isMultiVersioned) {
            return ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
        } else {
            return field.getGenericType();
        }
    }

    private <R extends Serializable & Comparable<R>> byte[] getListFieldValueAsBytes(HBRecord<R> record, Field field, String fieldSelector, String elementSelector, Map<String, String> codecFlags) {
        Serializable fieldValue;
        try {
            field.setAccessible(true);
            fieldValue = (Serializable) field.get(record);
            if (!Collection.class.isAssignableFrom(fieldValue.getClass())) {
                throw new RuntimeException("HBDynamicColumn Field must be a collection, but was " + fieldValue.getClass().getSimpleName());
            }
            Collection col = (Collection) fieldValue;
            Serializable collect = (Serializable) col.stream()
                    .filter(o -> {
                        try {
                            Field declaredField = o.getClass().getDeclaredField(fieldSelector);
                            declaredField.setAccessible(true);
                            String o1 = (String) declaredField.get(o);
                            return o1.equals(elementSelector);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            e.printStackTrace();
                        }
                        return false;
                    }).findFirst().get();
            return valueToByteArray(collect, codecFlags);
        } catch (IllegalAccessException e) {
            throw new BadHBaseLibStateException(e);
        }
    }

    byte[] valueToByteArray(Serializable value, Map<String, String> codecFlags) {
        try {
            return codec.serialize(value, codecFlags);
        } catch (SerializationException e) {
            throw new CodecException("Couldn't serialize", e);
        }
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> byte[] composeRowKey
            (HBRecord<R> record) {
        R rowKey;
        try {
            rowKey = record.composeRowKey();
        } catch (Exception ex) {
            throw new RowKeyCantBeComposedException(ex);
        }
        if (rowKey == null || rowKey.toString().isEmpty()) {
            throw new RowKeyCantBeEmptyException();
        }
        return valueToByteArray(rowKey, EMPTY_MAP);
    }
}
