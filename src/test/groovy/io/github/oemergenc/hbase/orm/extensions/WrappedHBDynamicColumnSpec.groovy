package io.github.oemergenc.hbase.orm.extensions

import com.flipkart.hbaseobjectmapper.DynamicQualifier
import spock.lang.Specification
import spock.lang.Unroll

class WrappedHBDynamicColumnSpec extends Specification {

    def "Valid family and qualifier works"() {
        given:
        def clazz = new ValidHBDynamicColumnClass()
        def field = clazz.getClass().getDeclaredField("field")

        when:
        def column = new WrappedHBDynamicColumn(field)

        then:
        noExceptionThrown()
        column.isPresent == true
        column.family == "f"
        column.columnQualifierField == "q"
        column.alias == "a"
        column.seperator == "#"
        column.prefix == "a#"
    }

    @Unroll
    def "Multiple parts qualifier works"() {
        given:
        def field = clazz.getClass().getDeclaredField("field")

        when:
        def column = new WrappedHBDynamicColumn(field)

        then:
        noExceptionThrown()
        column.columnQualifierField == expectedQualifier

        where:
        clazz                                    | expectedQualifier
        new MultiplePartsHBDynamicColumnClass()  | 'q:q1'
        new MultiplePartsHBDynamicColumnClass2() | 'q2:q:q1'
    }

    def "no HBDynamicColumn annotation does not throw exception"() {
        given:
        def clazz = new NoHBDynamicColumnClass()
        def field = clazz.getClass().getDeclaredField("field")

        when:
        def column = new WrappedHBDynamicColumn(field)

        then:
        noExceptionThrown()
        column.isPresent == false
        column.family == null
        column.columnQualifierField == null
        column.alias == null
        column.seperator == null
    }

    @Unroll
    def "wrong field type throws exception"() {
        given:
        def field = clazz.getClass().getDeclaredField("field")

        when:
        new WrappedHBDynamicColumn(field)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                          | _
        new WrongPrimitiveHBDynamicColumnClass()       | _
        new WrongComplexHBDynamicColumnClass()         | _
        new WrongListComplexTypeHBDynamicColumnClass() | _
    }

    @Unroll
    def "invalid qualifier field throws exception"() {
        given:
        def field = clazz.getClass().getDeclaredField("field")

        when:
        new WrappedHBDynamicColumn(field)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                    | _
        new EmptyQualifierHBDynamicColumnClass() | _
        new BlankQualifierHBDynamicColumnClass() | _
    }

    @Unroll
    def "wrong list field type throws exception"() {
        given:
        def field = clazz.getClass().getDeclaredField("field")

        when:
        new WrappedHBDynamicColumn(field)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                          | _
        new WrongListPrimitiveHBDynamicColumnClass()   | _
        new WrongListComplexTypeHBDynamicColumnClass() | _
    }

    @Unroll
    def "invalid multiple part columns throws exception"() {
        given:
        def field = clazz.getClass().getDeclaredField("field")

        when:
        new WrappedHBDynamicColumn(field)

        then:
        thrown(IllegalArgumentException)

        where:
        clazz                                           | _
        new InvalidMultiplePartsHBDynamicColumnClass1() | _
        new InvalidMultiplePartsHBDynamicColumnClass2() | _
        new EmptyPartsHBDynamicColumnClass2()           | _
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

    class ValidHBDynamicColumnClass {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<SimpleQualifierClass> field;
    }

    class EmptyQualifierHBDynamicColumnClass {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [" "])
        )
        List<SimpleQualifierClass> field;
    }

    class BlankQualifierHBDynamicColumnClass {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [""])
        )
        List<SimpleQualifierClass> field;
    }

    class ListHBDynamicColumnClass {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<String> field;
    }

    class WrongPrimitiveHBDynamicColumnClass {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        String field;
    }

    class WrongComplexHBDynamicColumnClass {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        Map<String, String> field;
    }

    class WrongListPrimitiveHBDynamicColumnClass {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Integer> field;
    }

    class WrongListComplexTypeHBDynamicColumnClass {
        @HBDynamicColumn(family = "",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q"])
        )
        List<Map<String, String>> field;
    }

    class MultiplePartsHBDynamicColumnClass {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q", "q1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class MultiplePartsHBDynamicColumnClass2 {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q2", "q", "q1"])
        )
        List<ThreePartsQualifierClass> field;
    }

    class InvalidMultiplePartsHBDynamicColumnClass1 {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["q", "1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class InvalidMultiplePartsHBDynamicColumnClass2 {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = ["", "q1"])
        )
        List<TwoPartsQualifierClass> field;
    }

    class EmptyPartsHBDynamicColumnClass2 {
        @HBDynamicColumn(family = "f",
                alias = "a",
                qualifier = @DynamicQualifier(parts = [])
        )
        List<TwoPartsQualifierClass> field;
    }

    class NoHBDynamicColumnClass {
        def field;
    }
}

