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
    private final String columnQualifierField;
    private final Class<? extends Annotation> annotationClass;
    private final Field field;
    private final String alias;
    private final String seperator;
    private final String[] parts;
    private final String partsSeperator;
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
            validateQualifierParts(hbColumn.qualifier());
            validateQualifierField(field, hbColumn.qualifier());
            validateAlias(hbColumn.alias());
            validateSeparator(hbColumn.qualifier().separator());
            String[] parts = hbColumn.qualifier().parts();
            partsSeperator = hbColumn.qualifier().separator();
            this.parts = parts;
            columnQualifierField = composeQualifierField(parts, partsSeperator);
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
            parts = null;
            partsSeperator = null;
            alias = null;
            seperator = null;
        }
    }

    private String composeQualifierField(String[] parts, String separator) {
        return String.join(separator, parts);
    }

    private void validateQualifierParts(DynamicQualifier columnQualifierField) {
        if (columnQualifierField.parts() == null || columnQualifierField.parts().length == 0) {
            throw new IllegalArgumentException("parts array of DynamicQualifier cannot be empty or null");
        }
        for (String part : columnQualifierField.parts()) {
            if (StringUtil.isNullOrEmpty(part)) {
                throw new IllegalArgumentException("a part of DynamicQualifier cannot be empty or null");
            }
        }
    }

    private void validateAlias(String alias) {
        if (StringUtil.isNullOrEmpty(alias)) {
            throw new IllegalArgumentException("alias of HBDynamicColumn cannot be empty or null");
        }
    }

    private void validateSeparator(String sep) {
        if (StringUtil.isNullOrEmpty(sep)) {
            throw new IllegalArgumentException("sep of DynamicQualifier cannot be empty or null");
        }
    }

    private void validateQualifierField(Field field, DynamicQualifier qualifier) {

        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        Type actualTypeArgument = listType.getActualTypeArguments()[0];
        if (actualTypeArgument instanceof Class) {
            Class<?> qualifierObjectClassType = (Class<?>) actualTypeArgument;
            String[] parts = qualifier.parts();
            for (String part : parts) {
                try {
                    Field declaredField = qualifierObjectClassType.getDeclaredField(part);
                    if (!declaredField.getType().equals(String.class)) {
                        throw new IllegalArgumentException("The generic Type of HBDynamicColumn must have a field with name " + part + " of type string, but was " + declaredField.getType());
                    }
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException("Generic Type of HBDynamicColumn must have a field with name " + part + " but was not found");
                }
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

    public String columnQualifierField() {
        return columnQualifierField;
    }

    public byte[] columnBytes(String columName) {
        return Bytes.toBytes(getPrefix() + columName);
    }

    public String getPrefix() {
        return alias + seperator;
    }

    public String[] getParts() {
        return parts;
    }

    public String getPartsSeperator() {
        return partsSeperator;
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
