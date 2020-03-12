package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.InvalidUserDao
import io.github.oemergenc.hbase.orm.extensions.dao.ValidUserDao
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException
import spock.lang.Unroll

import static io.github.oemergenc.hbase.orm.extensions.data.UserContent.*
import static java.util.UUID.randomUUID

class UserComponentTest extends AbstractComponentSpec {
    def validUserDao = new ValidUserDao(bigTableHelper.connect())

    def "Invalid record throws exception"() {
        when:
        new InvalidUserDao(bigTableHelper.connect())

        then:
        thrown(DuplicateColumnIdentifierException)
    }

    @Unroll
    def "empty or null dynamic columns value do not break persistence"() {
        given:
        def userId = randomUUID() as String
        def validUserRecord = validrecord(userId: userId,
                workAddresses: workAdress,
                homeAddresses: homeAdress,
        )

        when:
        validUserDao.persist(validUserRecord)

        and:
        def record = validUserDao.get(userId)

        then:
        noExceptionThrown()
        record
        record.workAddresses.collect { it.workAddress } == expectedWork
        record.homeAddresses.collect { it.homeAddress } == expectedHome

        where:
        workAdress                            | homeAdress                                | expectedWork    | expectedHome
        [workAddress(address: "workAddress")] | [homeAddress(address: "my-home-address")] | ["workAddress"] | ["my-home-address"]
        [workAddress(address: "workAddress")] | []                                        | ["workAddress"] | []
        []                                    | [homeAddress(address: "my-home-address")] | []              | ["my-home-address"]
        null                                  | [homeAddress(address: "my-home-address")] | []              | ["my-home-address"]
        [workAddress(address: "workAddress")] | null                                      | ["workAddress"] | []
        [workAddress(address: "workAddress")] | []                                        | ["workAddress"] | []
    }
}
