package io.github.oemergenc.hbase.orm.extensions.exception;

public class InvalidDynamicListEntryException extends IllegalArgumentException {

    public InvalidDynamicListEntryException(Object dynamicQualifier) {
        super(String.format("An entry of the dynamic hbase column list was null, which is not allowed. DynamicQualifier: %s", dynamicQualifier));
    }
}
