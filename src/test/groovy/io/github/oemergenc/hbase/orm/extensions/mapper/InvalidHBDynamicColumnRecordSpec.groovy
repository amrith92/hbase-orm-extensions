package io.github.oemergenc.hbase.orm.extensions.mapper

import com.flipkart.hbaseobjectmapper.DynamicQualifier
import com.flipkart.hbaseobjectmapper.HBRecord
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException
import spock.lang.Specification
import spock.lang.Unroll

class InvalidHBDynamicColumnRecordSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Valid family and qualifier works"() {
        when:
        mapper.validate(ValidHBDynamicColumnClass.class)

        then:
        noExceptionThrown()
    }

    @Unroll
    def "wrong field type throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                          | _
        WrongPrimitiveHBDynamicColumnClass.class       | _
        WrongComplexHBDynamicColumnClass.class         | _
        WrongListComplexTypeHBDynamicColumnClass.class | _
    }

    @Unroll
    def "duplicate hbdynamic columns in a class thorws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(DuplicateColumnIdentifierException)

        where:
        clazz                               | _
        DuplicateHBDynamicColumnClass.class | _
    }

    @Unroll
    def "invalid qualifier field throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                    | _
        EmptyQualifierHBDynamicColumnClass.class | _
        BlankQualifierHBDynamicColumnClass.class | _
    }

    @Unroll
    def "wrong list field type throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                          | _
        WrongListPrimitiveHBDynamicColumnClass.class   | _
        WrongListComplexTypeHBDynamicColumnClass.class | _
    }

    @Unroll
    def "invalid multiple part columns throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                           | _
        InvalidMultiplePartsHBDynamicColumnClass1.class | _
        InvalidMultiplePartsHBDynamicColumnClass2.class | _
        EmptyPartsHBDynamicColumnClass2.class           | _
    }

    class SimpleQualifierClass {
        String q;
    }

    class TwoPartsQualifierClass {
        String q;
        String q1;
    }

    class ThreePartsQualifierClass {
        String q;
        String q1;
        String q2;
    }

    class HBRecordTestBase implements HBRecord<String> {
        def static ID = "theId"

        @Override
        String composeRowKey() {
            return ID
        }

        @Override
        void parseRowKey(String rowKey) {

        }
    }

    class ValidHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<SimpleQualifierClass> field;
    }

    class DuplicateHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field;
        @HBDynamicColumn(family = "f", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field2;
    }

    class EmptyQualifierHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [" "])
        )
        List<SimpleQualifierClass> field;
    }

    class BlankQualifierHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [""])
        )
        List<SimpleQualifierClass> field;
    }

    class ListHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<String> field;
    }

    class WrongPrimitiveHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        String field;
    }

    class WrongComplexHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        Map<String, String> field;
    }

    class WrongListPrimitiveHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Integer> field;
    }

    class WrongListComplexTypeHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Map<String, String>> field;
    }

    class MultiplePartsHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q", "q1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class MultiplePartsHBDynamicColumnClass2 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q2", "q", "q1"])
        )
        List<ThreePartsQualifierClass> field;
    }

    class InvalidMultiplePartsHBDynamicColumnClass1 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q", "1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class InvalidMultiplePartsHBDynamicColumnClass2 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["", "q1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class EmptyPartsHBDynamicColumnClass2 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [])
        )
        List<TwoPartsQualifierClass> field;
    }

    class NoHBDynamicColumnClass extends HBRecordTestBase {
        def field;
    }
}

