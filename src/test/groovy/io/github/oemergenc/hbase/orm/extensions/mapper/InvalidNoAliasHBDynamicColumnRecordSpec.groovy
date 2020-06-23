package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.invalid.InvalidNoAliasHBDynamicColumnRecord
import io.github.oemergenc.hbase.orm.extensions.domain.invalid.InvalidNoAliasMixedHBDynamicColumnRecord
import io.github.oemergenc.hbase.orm.extensions.domain.records.NoAliasHBDynamicColumnRecord
import io.github.oemergenc.hbase.orm.extensions.exception.InvalidNoAliasColumnQualifierFieldException
import spock.lang.Specification
import spock.lang.Unroll

class InvalidNoAliasHBDynamicColumnRecordSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    @Unroll
    def "should allow one dynamic entry without alias on a single dynamic column"() {
        when:
        mapper.validate(clazz)

        then:
        noExceptionThrown()

        where:
        clazz                              | _
        NoAliasHBDynamicColumnRecord.class | _
    }

    @Unroll
    def "should only allow one dynamic entry without alias on a single dynamic column"() {
        when:
        mapper.validate(clazz)

        then:
        thrown(InvalidNoAliasColumnQualifierFieldException)

        where:
        clazz                                          | _
        InvalidNoAliasHBDynamicColumnRecord.class      | _
        InvalidNoAliasMixedHBDynamicColumnRecord.class | _
    }
}

