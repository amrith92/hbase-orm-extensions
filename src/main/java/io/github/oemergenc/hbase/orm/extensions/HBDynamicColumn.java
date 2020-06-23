package io.github.oemergenc.hbase.orm.extensions;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Maps an entity field to an HBase column
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface HBDynamicColumn {

    /**
     * Required: Name of HBase column family
     *
     * @return Name of HBase column family
     */
    String family();

    /**
     * Optional: Name of the field which values will be use for the  column.
     *
     * @return Name of field which value will be used for the column name
     */
    DynamicQualifier qualifier();

    /**
     * alias to be used as prefix in the column name. If not explicitly set no prefix will be set at all and the
     * dynamic column family cannot contain any other dynamic field. In this case defining another dynamic
     * column will throw an exception.
     *
     * @return alias as prefix of the resulting column name
     */
    String alias() default "";

    /**
     * Optional separator between the alias and the qualifierField value. Becomes obsolete if no alias is set explicitly
     *
     * @return separator
     */
    String separator() default "#";
}
