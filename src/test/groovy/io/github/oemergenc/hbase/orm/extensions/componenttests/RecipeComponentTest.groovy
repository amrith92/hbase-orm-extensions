package io.github.oemergenc.hbase.orm.extensions.componenttests


import io.github.oemergenc.hbase.orm.extensions.dao.RecipeCampaignActionsDao
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeTile

class RecipeComponentTest extends AbstractComponentSpec {
    def recipeCampaignDao = new RecipeCampaignActionsDao(bigTableHelper.connect())

    def "Store and reading dynamic column works"() {
        given:
        def tile = new RecipeTile("customerId", "campaignId", 1, "recipeId", "<html/>", "www.detail.url", "trending")
        def campaignRecord = new RecipeCampaignActionRecord()
        campaignRecord.setCustomerId("custId2")
        campaignRecord.setRecipeTiles([tile])

        when:
        recipeCampaignDao.persist([campaignRecord])

        and:
        def record = recipeCampaignDao.get("cp#campaignId:1")

        then:
        record.customerId == "custId2"
    }
}
