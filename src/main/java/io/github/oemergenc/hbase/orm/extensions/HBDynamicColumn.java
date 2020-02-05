package io.github.oemergenc.hbase.orm.extensions;

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
     * Name of HBase column family
     *
     * @return Name of HBase column family
     */
    String family();

    /**
     * Name of the field which values will be use for the  column
     *
     * @return Name of field which value will be used for the column name
     */
    String qualifierField();

    /**
     * Optional alias to be as prefix in the column name,if omitted the value of qualifierField will be used
     *
     * @return alias as prefix of the resulting column name
     */
    String alias() default "";

    /**
     * Optional separator between the alias and the qualifierField value
     *
     * @return separator
     */
    String separator() default "#";
}
