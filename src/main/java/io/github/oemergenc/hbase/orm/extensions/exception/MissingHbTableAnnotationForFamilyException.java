package io.github.oemergenc.hbase.orm.extensions.exception;

public class MissingHbTableAnnotationForFamilyException extends IllegalArgumentException {

    public MissingHbTableAnnotationForFamilyException(Object family) {
        super(String.format("The family %s is not defined on the HBTable definition. Consider adding a new Family on the HBTable class", family));
    }
}
