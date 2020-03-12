package io.github.oemergenc.hbase.orm.extensions.exception;

import java.util.List;

public class DuplicateColumnIdentifierException extends IllegalArgumentException {

    public DuplicateColumnIdentifierException(List<String> duplicates) {
        super(String.format("Duplicate column identifier. Make sure to use different qualifiers on each HBDynamicColumn annotation. Duplicates: %s", duplicates));
    }
}
