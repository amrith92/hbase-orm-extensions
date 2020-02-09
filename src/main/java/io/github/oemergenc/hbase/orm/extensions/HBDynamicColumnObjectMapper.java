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
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException;
import io.github.oemergenc.hbase.orm.extensions.exception.InvalidColumnQualifierFieldException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.StringUtils;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;

@Slf4j
public class HBDynamicColumnObjectMapper extends HBObjectMapper {

    private static final BestSuitCodec CODEC = new BestSuitCodec();
    private final Codec codec;

    public HBDynamicColumnObjectMapper(Codec codec) {
        super(codec);
        this.codec = codec;
    }

    public HBDynamicColumnObjectMapper() {
        this(CODEC);
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Put writeValueAsPut(HBRecord<R> record) {
        validateHBDynamicClass(record.getClass());
        Put put = super.writeValueAsPut(record);
        for (val fe : convertRecordToMap(record).entrySet()) {
            val family = fe.getKey();
            for (val e : fe.getValue().entrySet()) {
                val columnName = e.getKey();
                val columnValuesVersioned = e.getValue();
                if (columnValuesVersioned == null)
                    continue;
                for (val versionAndValue : columnValuesVersioned.entrySet()) {
                    put.addColumn(family, columnName, versionAndValue.getKey(), versionAndValue.getValue());
                }
            }
        }
        return put;
    }

    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> T readValue(Result result, Class<T> clazz) {
        validateHBDynamicClass(clazz);
        T t = super.readValue(result, clazz);
        if (t != null) {
            convertMapToRecord(result.getMap(), clazz, t);
        }
        return t;
    }

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> void convertMapToRecord(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map, Class<T> clazz, T record) {
        Collection<Field> fields = getHBDynamicColumnFields0(clazz).values();
        for (Field dynamicField : fields) {
            val genericTypeOfList = (ParameterizedType) dynamicField.getGenericType();
            val genericObjectType = (Class<?>) genericTypeOfList.getActualTypeArguments()[0];
            val hbDynamicColumn = new WrappedHBDynamicColumn(dynamicField);
            if (hbDynamicColumn.isPresent()) {
                val dynamicListMembers = new ArrayList<>();
                val familyMap = map.get(hbDynamicColumn.familyBytes());
                if (familyMap == null || familyMap.isEmpty()) {
                    continue;
                }
                val dynamicColumnBytesList = familyMap
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
                for (val dynamicColumnBytes : dynamicColumnBytesList) {
                    val columnVersionsMap = familyMap.get(dynamicColumnBytes);
                    val lastEntry = columnVersionsMap.lastEntry();
                    try {
                        val deserialize = codec.deserialize(lastEntry.getValue(), genericObjectType, EMPTY_MAP);
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

    List<String> getHBDynamicColumnNames(Field field, String columnQualifierField, HBRecord record) {
        try {
            val declaredField = record.getClass().getDeclaredField(field.getName());
            declaredField.setAccessible(true);
            val qualifierObject = declaredField.get(record);
            if (!Collection.class.isAssignableFrom(qualifierObject.getClass())) {
                throw new RuntimeException("HBDynamicColumn Field must be a collection, but was " + qualifierObject.getClass().getSimpleName());
            }
            val listOfPojos = (List<?>) qualifierObject;
            val listOfPojosType = (ParameterizedType) declaredField.getGenericType();
            val pojoClazz = (Class<?>) listOfPojosType.getActualTypeArguments()[0];
            val qualifierField = pojoClazz.getDeclaredField(columnQualifierField);
            return getValidDynamicColumnValues(listOfPojos, qualifierField);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private List<String> getValidDynamicColumnValues(List<?> dynamicColumnNameList, Field qualifierField) {
        val validQualifierValues = new ArrayList<String>();
        for (Object pojo : dynamicColumnNameList) {
            try {
                val concreteQualifierField = pojo.getClass().getDeclaredField(qualifierField.getName());
                concreteQualifierField.setAccessible(true);
                Object qualifierFieldValue = concreteQualifierField.get(pojo);
                if (qualifierFieldValue != null
                        && String.class.isAssignableFrom(qualifierFieldValue.getClass())
                        && StringUtils.isNotBlank((String) qualifierFieldValue)) {
                    validQualifierValues.add((String) qualifierFieldValue);
                } else {
                    throw new InvalidColumnQualifierFieldException(qualifierField.getName());
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvalidColumnQualifierFieldException ex) {
                log.error("There was an invalid qualifier field value, which will be ignored", ex);
                ex.printStackTrace();
            }
        }
        return validQualifierValues;
    }

    @Override
    public <R extends Serializable & Comparable<R>, T extends HBRecord<R>> Result writeValueAsResult(HBRecord<R> record) {
        validateHBDynamicClass(record.getClass());
        val result = super.writeValueAsResult(record);
        val row = composeRowKey(record);
        val cells = result.listCells();
        List<Cell> cellList = new ArrayList<>();
        for (val fe : convertRecordToMap(record).entrySet()) {
            val family = fe.getKey();
            for (val e : fe.getValue().entrySet()) {
                val columnName = e.getKey();
                NavigableMap<Long, byte[]> valuesVersioned = e.getValue();
                if (valuesVersioned == null)
                    continue;
                for (val columnVersion : valuesVersioned.entrySet()) {
                    val cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
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
        val map = new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR);
        int numOfFieldsToWrite = 0;
        val dynamicfields = getHBDynamicColumnFields0(clazz).values();
        for (Field dynamicField : dynamicfields) {
            val hbColumn = new WrappedHBDynamicColumn(dynamicField);
            if (hbColumn.isPresent()) {
                val hbDynamicColumnNames = getHBDynamicColumnNames(dynamicField, hbColumn.columnQualifierField(), record);
                for (val columnName : hbDynamicColumnNames) {
                    val familyName = hbColumn.familyBytes();
                    val columnNameBytes = hbColumn.columnBytes(columnName);
                    if (!map.containsKey(familyName)) {
                        map.put(familyName, new TreeMap<>(Bytes.BYTES_COMPARATOR));
                    }
                    val columns = map.get(familyName);
                    final byte[] fieldValueBytes = getListFieldValueAsBytes(record, dynamicField, hbColumn.columnQualifierField(), columnName, Collections.emptyMap());
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
        int numOfHBColumns = 0;
        Map<String, Field> hbDynamicColumnFields = getHBDynamicColumnFields0(clazz);
        val hbDynamicColumnIdentifiers = new HashSet<String>();
        for (Field field : hbDynamicColumnFields.values()) {
            WrappedHBDynamicColumn hbDynamicColumn = new WrappedHBDynamicColumn(field);
            if (hbDynamicColumn.isPresent()) {
                if (!hbDynamicColumnIdentifiers.contains(hbDynamicColumn.toString())) {
                    hbDynamicColumnIdentifiers.add(hbDynamicColumn.toString());
                } else {
                    throw new DuplicateColumnIdentifierException(hbDynamicColumn.toString());
                }
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
            Collection col = (Collection) fieldValue;
            Serializable collect = (Serializable) col.stream()
                    .filter(o -> {
                        try {
                            Field declaredField = o.getClass().getDeclaredField(fieldSelector);
                            declaredField.setAccessible(true);
                            String o1 = (String) declaredField.get(o);
                            if (o1 != null)
                                return o1.equals(elementSelector);
                            return false;
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

    private <R extends Serializable & Comparable<R>, T extends HBRecord<R>> byte[] composeRowKey(HBRecord<R> record) {
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
