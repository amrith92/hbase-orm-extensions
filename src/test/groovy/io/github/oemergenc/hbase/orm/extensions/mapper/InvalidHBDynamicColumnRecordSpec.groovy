package io.github.oemergenc.hbase.orm.extensions.mapper

import com.flipkart.hbaseobjectmapper.DynamicQualifier
import com.flipkart.hbaseobjectmapper.Family
import com.flipkart.hbaseobjectmapper.HBRecord
import com.flipkart.hbaseobjectmapper.HBTable
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException
import io.github.oemergenc.hbase.orm.extensions.exception.MissingHbTableAnnotationForFamilyException
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
    def "duplicate hbdynamic columns in a class throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(DuplicateColumnIdentifierException)

        where:
        clazz                               | _
        DuplicateHBDynamicColumnClass.class | _
    }

    @Unroll
    def "Missing hbdynamic columns family on table throws exception"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(MissingHbTableAnnotationForFamilyException)

        where:
        clazz                                          | _
        MissingHBDynamicColumnFamilyOnTableClass.class | _
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

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
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

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class ValidHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<SimpleQualifierClass> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class DuplicateHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field;
        @HBDynamicColumn(family = "f", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field2;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class MissingHBDynamicColumnFamilyOnTableClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field;
        @HBDynamicColumn(family = "NotOnTable", alias = "a", qualifier = @DynamicQualifier(parts = ["q"]))
        List<SimpleQualifierClass> field2;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class EmptyQualifierHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [" "])
        )
        List<SimpleQualifierClass> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class BlankQualifierHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [""])
        )
        List<SimpleQualifierClass> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class WrongPrimitiveHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        String field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class WrongComplexHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        Map<String, String> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class WrongListPrimitiveHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Integer> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class WrongListComplexTypeHBDynamicColumnClass extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Map<String, String>> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class InvalidMultiplePartsHBDynamicColumnClass1 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q", "1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class InvalidMultiplePartsHBDynamicColumnClass2 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["", "q1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    @HBTable(name = "HBRecordTestBase", families = [@Family(name = "f")])
    class EmptyPartsHBDynamicColumnClass2 extends HBRecordTestBase {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [])
        )
        List<TwoPartsQualifierClass> field;
    }
}

