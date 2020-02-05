package io.github.oemergenc.hbase.orm.extensions.data

import io.github.oemergenc.hbase.orm.extensions.domain.*

class UserContent {
    def static workAddress(Map params = [:]) {
        Map values = [
                address: "work-address",
        ] << params
        def address = new WorkAddress(values.address)
        address
    }

    def static homeAddress(Map params = [:]) {
        Map values = [
                address: "home-address",
        ] << params
        def address = new HomeAddress(values.address)
        address
    }

    def static address(Map params = [:]) {
        Map values = [
                address: "home-address",
        ] << params
        def address = new Address(values.address)
        address
    }

    def static validrecord(Map params = [:]) {
        Map values = [
                userId       : "the-user-id",
                workAddresses: [workAddress(params)],
                homeAddresses: [homeAddress(params)],
        ] << params
        new ValidUserRecord(values.userId, values.workAddresses, values.homeAddresses)
    }

    def static invalidrecord(Map params = [:]) {
        Map values = [
                userId       : "the-user-id",
                workAddresses: [address(params)],
                homeAddresses: [address(params)],
        ] << params
        new InvalidUserRecord(values.userId, values.workAddresses, values.homeAddresses)
    }
}
