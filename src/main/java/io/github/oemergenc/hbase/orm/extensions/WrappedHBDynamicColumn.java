package io.github.oemergenc.hbase.orm.extensions;


import com.flipkart.hbaseobjectmapper.exceptions.FieldNotMappedToHBaseColumnException;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;


/**
 * A wrapper class for {@link HBDynamicColumn}
 */
class WrappedHBDynamicColumn {
    private final String family, columnQualifierSelector;
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
            isPresent = true;
            family = hbColumn.family();
            columnQualifierSelector = hbColumn.columnQualifier();
            alias = (!hbColumn.alias().equals("") ? hbColumn.alias() : hbColumn.columnQualifier());
            seperator = hbColumn.separator();
            annotationClass = HBDynamicColumn.class;
        } else {
            if (throwExceptionIfNonHBColumn) {
                throw new FieldNotMappedToHBaseColumnException(field.getDeclaringClass(), field.getName());
            }
            isPresent = false;
            family = null;
            columnQualifierSelector = null;
            annotationClass = null;
            alias = null;
            seperator = null;
        }
    }

    public String family() {
        return family;
    }

    public byte[] familyBytes() {
        return Bytes.toBytes(family);
    }

    public String columnQualifierSelector() {
        return columnQualifierSelector;
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
        return String.format("%s:%s", family, columnQualifierSelector);
    }

    public boolean isPresent() {
        return isPresent;
    }
}
