package io.github.oemergenc.hbase.orm.extensions.componenttests

import io.github.oemergenc.hbase.orm.extensions.dao.InvalidUserDao
import io.github.oemergenc.hbase.orm.extensions.dao.ValidUserDao
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException

import static io.github.oemergenc.hbase.orm.extensions.data.UserContent.address
import static io.github.oemergenc.hbase.orm.extensions.data.UserContent.invalidrecord

class UserComponentTest extends AbstractComponentSpec {
    def validUserDao = new ValidUserDao(bigTableHelper.connect())
    def invalidUserDao = new InvalidUserDao(bigTableHelper.connect())

    def "Invalid record throws exception"() {
        given:
        def userId = "theExceptionUser"
        def workAddress = address(address: "workAddress")
        def homeAddress = address(address: "my-home-address")
        def invalidUserRecord = invalidrecord(userId: userId,
                workAddresses: [workAddress],
                homeAddresses: [homeAddress],
        )

        when:
        invalidUserDao.persist(invalidUserRecord)

        then:
        thrown(DuplicateColumnIdentifierException)
    }
}
