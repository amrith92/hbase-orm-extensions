package io.github.oemergenc.hbase.orm.extensions.exception;

public class InvalidColumnQualifierFieldException extends IllegalArgumentException {

    public InvalidColumnQualifierFieldException(String columnQualifierField) {
        super(String.format("The value of the qualifierField was empty or null, which is not allowed. This entry will be ignored qualifierField: %s", columnQualifierField));
    }
}
