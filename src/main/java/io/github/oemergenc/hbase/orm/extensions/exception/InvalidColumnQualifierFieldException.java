package io.github.oemergenc.hbase.orm.extensions.exception;

public class InvalidColumnQualifierFieldException extends IllegalArgumentException {

    public InvalidColumnQualifierFieldException(String part, Object dynamicListEntry) {
        super(String.format("A part of the dynamicQualifier was empty or null, which is not allowed. This entry will be ignored part: %s, object: %s", part, dynamicListEntry));
    }
}
