package io.github.oemergenc.hbase.orm.extensions.exception;

public class InvalidNoAliasColumnQualifierFieldException extends IllegalArgumentException {

    public InvalidNoAliasColumnQualifierFieldException() {
        super(String.format("The definition of a dynamic column without an alias is invalid. Dynamic columns families without an alias can only have exactly one column"));
    }
}
