package com.flipkart.hbaseobjectmapper;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Represents a dynamic column qualifier in HBase
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface DynamicQualifier {

    String[] parts();

    String separator() default ":";
}
