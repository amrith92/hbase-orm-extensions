package io.github.oemergenc.hbase.orm.extensions;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.exceptions.FieldNotMappedToHBaseColumnException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.io.netty.util.internal.StringUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

/**
 * A wrapper class for {@link HBDynamicColumn}
 */
class WrappedHBDynamicColumn {
    private final String family;
    private final DynamicQualifier columnQualifierField;
    private final Class<? extends Annotation> annotationClass;
    private final Field field;
    private final String alias;
    private final String seperator;
    private final boolean isPresent;

    WrappedHBDynamicColumn(Field field) {
        this(field, false);
    }

    @SuppressWarnings("unchecked")
    WrappedHBDynamicColumn(Field field, boolean throwExceptionIfNonHBColumn) {
        this.field = field;
        HBDynamicColumn hbColumn = field.getAnnotation(HBDynamicColumn.class);
        if (hbColumn != null) {
            if (!Collection.class.isAssignableFrom(field.getType())) {
                throw new IllegalArgumentException("HBDynamicColumn field must be a collection, but was " + field.getType());
            }
            columnQualifierField = hbColumn.qualifier();
            validateQualifierParts(columnQualifierField);
            validateQualifierField(field);
            validateAlias(hbColumn.alias());
            isPresent = true;
            family = hbColumn.family();
            alias = hbColumn.alias();
            seperator = hbColumn.separator();
            annotationClass = HBDynamicColumn.class;
        } else {
            if (throwExceptionIfNonHBColumn) {
                throw new FieldNotMappedToHBaseColumnException(field.getDeclaringClass(), field.getName());
            }
            isPresent = false;
            family = null;
            columnQualifierField = null;
            annotationClass = null;
            alias = null;
            seperator = null;
        }
    }

    private void validateQualifierParts(DynamicQualifier columnQualifierField) {
        for (String part : columnQualifierField.parts()) {
            if (StringUtil.isNullOrEmpty(part)) {
                throw new IllegalArgumentException("parts of DynamicQualifier cannot be empty or null");
            }
        }
    }

    private void validateAlias(String alias) {
        if (StringUtil.isNullOrEmpty(alias)) {
            throw new IllegalArgumentException("alias of HBDynamicColumn cannot be empty or null");
        }
    }

    private void validateQualifierField(Field field) {

        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        Type actualTypeArgument = listType.getActualTypeArguments()[0];
        if (actualTypeArgument instanceof Class) {
            Class<?> qualifierObjectClassType = (Class<?>) actualTypeArgument;
            try {
                String[] parts = columnQualifierField.parts();
                for (String part : parts) {
                    Field declaredField = qualifierObjectClassType.getDeclaredField(part);
                    if (!declaredField.getType().equals(String.class)) {
                        throw new IllegalArgumentException("The generic Type of HBDynamicColumn must have a field with name " + part + " of type string, but was " + declaredField.getType());
                    }
                }
            } catch (NoSuchFieldException e) {
                throw new IllegalArgumentException("Generic Type of HBDynamicColumn must have a field with name " + columnQualifierField + " but was not found");
            }
        } else {
            throw new IllegalArgumentException("Generic Type of HBDynamicColumn must be a ParameterizedType");
        }
    }

    public String family() {
        return family;
    }

    public byte[] familyBytes() {
        return Bytes.toBytes(family);
    }

    public String[] columnQualifierParts() {
        return columnQualifierField.parts();
    }

    public byte[] columnBytes(String columName) {
        return Bytes.toBytes(getPrefix() + columName);
    }

    public String getPrefix() {
        return alias + seperator;
    }

    public String getName() {
        return annotationClass.getName();
    }

    @Override
    public String toString() {
        return String.format("%s:%s", family, getPrefix());
    }

    public boolean isPresent() {
        return isPresent;
    }
}
