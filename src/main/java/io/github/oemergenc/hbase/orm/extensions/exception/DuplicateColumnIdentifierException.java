package io.github.oemergenc.hbase.orm.extensions.exception;

public class DuplicateColumnIdentifierException extends IllegalArgumentException {

    public DuplicateColumnIdentifierException(String columnIdentifier) {
        super(String.format("Duplicate column identifier [%s]. Make sure to use different qualifiers on each HBDynamicColumn annotation", columnIdentifier));
    }
}
