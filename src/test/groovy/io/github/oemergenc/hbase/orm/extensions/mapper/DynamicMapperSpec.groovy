package io.github.oemergenc.hbase.orm.extensions.mapper

import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumnObjectMapper
import io.github.oemergenc.hbase.orm.extensions.domain.Campaign
import io.github.oemergenc.hbase.orm.extensions.domain.CampaignRecord
import io.github.oemergenc.hbase.orm.extensions.domain.ValidUserRecord
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.ContentRecipeJsonDto
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeTile
import io.github.oemergenc.hbase.orm.extensions.exception.DuplicateColumnIdentifierException
import spock.lang.Specification
import spock.lang.Unroll

import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.campaign
import static io.github.oemergenc.hbase.orm.extensions.data.CampaignContent.record
import static io.github.oemergenc.hbase.orm.extensions.data.UserContent.*
import static java.util.UUID.randomUUID

class DynamicMapperSpec extends Specification {
    def mapper = new HBDynamicColumnObjectMapper()

    def "Converting model class works"() {
        given:
        def campain = new Campaign("camId1", "custId2")
        def campain2 = new Campaign("camId2", "custId2")
        def campaignRecord = new CampaignRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setCampaigns([campain, campain2])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        and:
        def campaignRecordResult = mapper.readValue(result, CampaignRecord.class)

        then:
        result
        campaignRecordResult.customerId == "custId2"
        campaignRecordResult.campaigns.size() == 2
        campaignRecordResult.campaigns.collect { it.campaignId }.containsAll(["camId2", "camId1"])
        campaignRecordResult.campaigns.collect { it.customerId }.containsAll(["custId2"])
    }

    def "Invalid entries do no break persisting valid objects"() {
        given:
        def campaignInvalid = campaign(campaignId: null, customerId: "the-customer-id")
        def campaignValid = campaign(campaignId: "valid-campaign", customerId: "the-customer-id")
        def campaignRecord = record(campaigns: [campaignInvalid, campaignValid])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        and:
        def campaignRecordResult = mapper.readValue(result, CampaignRecord.class)

        then:
        result
        campaignRecordResult.customerId == "the-customer-id"
        campaignRecordResult.campaigns.size() == 1
        campaignRecordResult.campaigns.collect { it.campaignId }.containsAll(["valid-campaign"])
        campaignRecordResult.campaigns.collect { it.customerId }.containsAll(["the-customer-id"])
    }

    def "Multiple columns works"() {
        given:
        def userId = "theUser"
        def workAddress = workAddress(address: "workAddress")
        def homeAddress = homeAddress(address: "my-home-address")
        def validUserRecord = validrecord(userId: userId,
                workAddresses: [workAddress],
                homeAddresses: [homeAddress],
        )

        when:
        def result = mapper.writeValueAsResult(validUserRecord)

        and:
        def recordResult = mapper.readValue(result, ValidUserRecord.class)

        then:
        recordResult.userId == userId
        recordResult.workAddresses.size() == 1
        recordResult.homeAddresses.size() == 1
        recordResult.workAddresses.collect { it.workAddress }.containsAll(["workAddress"])
        recordResult.homeAddresses.collect { it.homeAddress }.containsAll(["my-home-address"])
    }

    def "Invalid record throws exception"() {
        given:
        def userId = "theExceptionUser"
        def workAddress = address(address: "workAddress")
        def homeAddress = address(address: "my-home-address")
        def validUserRecord = invalidrecord(userId: userId,
                workAddresses: [workAddress],
                homeAddresses: [homeAddress],
        )

        when:
        mapper.writeValueAsResult(validUserRecord)

        then:
        thrown(DuplicateColumnIdentifierException)
    }

    def "Converting multi model class works"() {
        given:
        def tile = new RecipeTile("customerId", "campaignId1234", "1", "recipeId", "<html/>", "www.detail.url", "trending")
        def jsonDto = new ContentRecipeJsonDto("customerId", "23-01-2019", [], "trending")
        def campaignRecord = new RecipeCampaignActionRecord()
        campaignRecord.setCustomerPid("custId2")
        campaignRecord.setRecipeTiles([tile])
        campaignRecord.setJsonRecipes([jsonDto])

        when:
        def result = mapper.writeValueAsResult(campaignRecord)

        then:
        def cells = result.getColumnCells("campaigns".bytes, 'cp#campaignId1234$1'.bytes)
        cells.size() == 1
        def dayCells = result.getColumnCells("days".bytes, 'd#23-01-2019'.bytes)
        dayCells.size() == 1

        when:
        def campaignRecordResult = mapper.readValue(result, RecipeCampaignActionRecord.class)

        then:
        campaignRecordResult.recipeTiles.size() == 1
        campaignRecordResult.recipeTiles[0].customerId == 'customerId'
        campaignRecordResult.recipeTiles[0].campaignId == 'campaignId1234'
        campaignRecordResult.recipeTiles[0].position == '1'
        campaignRecordResult.recipeTiles[0].recipeId == 'recipeId'
        campaignRecordResult.recipeTiles[0].html == '<html/>'
        campaignRecordResult.recipeTiles[0].recoType == 'trending'
        campaignRecordResult.recipeTiles[0].detailUrl == 'www.detail.url'

        and:
        campaignRecordResult.jsonRecipes.size() == 1
        campaignRecordResult.jsonRecipes[0].recoType == "trending"
        campaignRecordResult.jsonRecipes[0].customerId == "customerId"
        campaignRecordResult.jsonRecipes[0].dayId == "23-01-2019"
        campaignRecordResult.jsonRecipes[0].recipes.isEmpty()
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
        def result = mapper.writeValueAsResult(validUserRecord)

        and:
        def record = mapper.readValue(result, ValidUserRecord.class)

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
