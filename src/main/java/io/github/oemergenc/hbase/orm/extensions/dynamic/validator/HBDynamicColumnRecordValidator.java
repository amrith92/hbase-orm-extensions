package io.github.oemergenc.hbase.orm.extensions.dynamic.validator;

import com.flipkart.hbaseobjectmapper.DynamicQualifier;
import com.flipkart.hbaseobjectmapper.Family;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBTable;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException;
import io.github.oemergenc.hbase.orm.extensions.exception.InvalidNoAliasColumnQualifierFieldException;
import io.github.oemergenc.hbase.orm.extensions.exception.MissingHbTableAnnotationForFamilyException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.vidageek.mirror.dsl.Mirror;
import net.vidageek.mirror.list.dsl.MirrorList;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.hbase.thirdparty.io.netty.util.internal.StringUtil;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class HBDynamicColumnRecordValidator {
    private static Mirror MIRROR = new Mirror();

    static public <T extends HBRecord<?>> void validate(Class<T> hbRecordClazz) {
        val hbTableFamilies = MIRROR.on(hbRecordClazz).reflect()
                .annotation(HBTable.class).atClass().families();
        List<String> tableFamilyNames = Arrays.stream(hbTableFamilies)
                .map(Family::name).collect(Collectors.toList());
        val hbDynamicColumnFields = MIRROR.on(hbRecordClazz).reflectAll().fields()
                .matching(element -> element.getAnnotation(HBDynamicColumn.class) != null);

        List<String> columnPrefixList = new ArrayList<>();
        Set<String> validNoAliasDynamicFields = validateNoAliasDynamicFields(hbRecordClazz, hbDynamicColumnFields);
        for (Field dynamicField : hbDynamicColumnFields) {
            HBDynamicColumn hbDynamicColumn = MIRROR.on(hbRecordClazz).reflect()
                    .annotation(HBDynamicColumn.class).atField(dynamicField.getName());
            String family = hbDynamicColumn.family();
            String alias = hbDynamicColumn.alias();
            String separator = hbDynamicColumn.separator();
            DynamicQualifier qualifier = hbDynamicColumn.qualifier();
            if (!Collection.class.isAssignableFrom(dynamicField.getType())) {
                throw new IllegalArgumentException("HBDynamicColumn field must be a collection, but was " + dynamicField.getType());
            }
            validateQualifierField(dynamicField, qualifier);
            validateQualifierParts(Arrays.asList(qualifier.parts()));
            validateFamily(family, tableFamilyNames);
            if (!validNoAliasDynamicFields.contains(family)) {
                validateAlias(alias);
                validateSeparator(separator);
            }
            columnPrefixList.add(family.concat(separator).concat(alias));
        }
        validateNoDuplicatePrefix(columnPrefixList);
    }

    private static <T extends HBRecord<?>> Set<String> validateNoAliasDynamicFields(Class<T> hbRecordClazz,
                                                                                    MirrorList<Field> hbDynamicColumnFields) {
        val hbDynamicColumns = hbDynamicColumnFields.stream()
                .map(dynamicField -> MIRROR.on(hbRecordClazz).reflect()
                        .annotation(HBDynamicColumn.class).atField(dynamicField.getName()))
                .collect(Collectors.toList());

        val noAliasColumns = hbDynamicColumns
                .stream()
                .filter(column -> StringUtils.isBlank(column.alias()))
                .map(HBDynamicColumn::family)
                .collect(Collectors.toList());

        Map<String, ? extends List<? extends HBDynamicColumn>> noAliasColumnsSummedByFamily = hbDynamicColumns
                .stream()
                .filter(column -> noAliasColumns.contains(column.family()))
                .collect(Collectors.groupingBy(HBDynamicColumn::family));

        val dynamicColumnsWithMoreThanOneNoAliasField = noAliasColumnsSummedByFamily
                .entrySet()
                .stream()
                .filter(stringListEntry -> stringListEntry.getValue().size() > 1)
                .collect(Collectors.toList());

        if (!dynamicColumnsWithMoreThanOneNoAliasField.isEmpty()) {
            dynamicColumnsWithMoreThanOneNoAliasField
                    .forEach(key -> log.error("The definition of the dynamic column for family {} is invalid. Make sure a no alias dynamic column family is not used more than once", key));
            throw new InvalidNoAliasColumnQualifierFieldException();
        }
        return noAliasColumnsSummedByFamily.keySet();
    }

    static public void validateNoDuplicatePrefix(List<String> columnPrefixList) {
        val duplicates = columnPrefixList.stream().collect(Collectors.groupingBy(Function.identity()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (!duplicates.isEmpty()) {
            throw new DuplicateColumnIdentifierException(duplicates);
        }
    }

    static public void validateQualifierParts(List<String> parts) {
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("parts array of DynamicQualifier cannot be empty or null");
        }
        for (String part : parts) {
            if (StringUtil.isNullOrEmpty(part)) {
                throw new IllegalArgumentException("a part of DynamicQualifier cannot be empty or null");
            }
        }
    }

    static public void validateFamily(String family, List<String> tableFamilyNames) {
        validateFamilyName(family);
        if (!tableFamilyNames.contains(family)) {
            throw new MissingHbTableAnnotationForFamilyException(family);
        }
    }

    public static void validateFamilyName(String family) {
        if (StringUtil.isNullOrEmpty(family)) {
            throw new IllegalArgumentException("family of HBDynamicColumn cannot be empty or null");
        }
    }

    static public void validateAlias(String alias) {
        if (StringUtil.isNullOrEmpty(alias)) {
            throw new IllegalArgumentException("alias of HBDynamicColumn cannot be empty or null");
        }
    }

    static public void validateSeparator(String sep) {
        if (StringUtil.isNullOrEmpty(sep)) {
            throw new IllegalArgumentException("separator of DynamicQualifier cannot be empty or null");
        }
    }

    static public void validateQualifierField(Field field, DynamicQualifier qualifier) {

        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        Type actualTypeArgument = listType.getActualTypeArguments()[0];
        if (actualTypeArgument instanceof Class) {
            Class<?> qualifierObjectClassType = (Class<?>) actualTypeArgument;
            String[] parts = qualifier.parts();
            for (String part : parts) {
                try {
                    Field declaredField = qualifierObjectClassType.getDeclaredField(part);
                    if (!declaredField.getType().equals(String.class)) {
                        throw new IllegalArgumentException("The generic Type of HBDynamicColumn must have a field with name " + part + " of type string, but was " + declaredField.getType());
                    }
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException("Generic Type of HBDynamicColumn must have a field with name " + part + " but was not found");
                }
            }
        } else {
            throw new IllegalArgumentException("Generic Type of HBDynamicColumn must be a ParameterizedType");
        }
    }
}
