package io.github.oemergenc.hbase.orm.extensions.componenttests


import io.github.oemergenc.hbase.orm.extensions.dao.RecipeCampaignActionsDao
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.ContentRecipeJsonDto
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeCampaignActionRecord
import io.github.oemergenc.hbase.orm.extensions.domain.recipe.RecipeTile

class RecipeComponentTest extends AbstractComponentSpec {
    def recipeCampaignDao = new RecipeCampaignActionsDao(bigTableHelper.connect())

    def "Store and reading dynamic column works"() {
        given:
        def tile = new RecipeTile("customerId", "campaignId1234", "1", "recipeId", "<html/>", "www.detail.url", "trending")
        def campaignRecord = new RecipeCampaignActionRecord()
        campaignRecord.setCustomerPid("custId2")
        campaignRecord.setRecipeTiles([tile])

        when:
        recipeCampaignDao.persist([campaignRecord])

        and:
        def record = recipeCampaignDao.get("custId2")

        then:
        record.customerPid == "custId2"
    }

    def "Converting multi model class works"() {
        given:
        def tile = new RecipeTile("customerId", "campaignId1234", "1", "recipeId", "<html/>", "www.detail.url", "trending")
        def tile2 = new RecipeTile("customerId", "campaignId6527", "2", "recipeId", "<html/>", "www.detail.url", "trending")
        def jsonDto = new ContentRecipeJsonDto("customerId", "23-01-2019", [], "trending")
        def jsonDto2 = new ContentRecipeJsonDto("customerId", "24-01-2019", [], "trending")
        def campaignRecord = new RecipeCampaignActionRecord()
        campaignRecord.setCustomerPid("custId2")
        campaignRecord.setRecipeTiles([tile, tile2])
        campaignRecord.setJsonRecipes([jsonDto, jsonDto2])

        when:
        recipeCampaignDao.persist([campaignRecord])

        then:
        recipeCampaignDao.getCampaignActionByPosition("custId2", "campaignId6527", 2)
        recipeCampaignDao.getCampaignActionByPosition("custId2", "campaignId1234", 1)
        recipeCampaignDao.getCampaignActionByDay("custId2", "23-01-2019")
        recipeCampaignDao.getCampaignActionByDay("custId2", "24-01-2019")

        when:
        def recipeCampaignActionRecord = recipeCampaignDao.get("custId2")

        then:
        recipeCampaignActionRecord.recipeTiles.size() == 2
        recipeCampaignActionRecord.recipeTiles[0].customerId == 'customerId'
        recipeCampaignActionRecord.recipeTiles[0].campaignId == 'campaignId1234'
        recipeCampaignActionRecord.recipeTiles[0].position == '1'
        recipeCampaignActionRecord.recipeTiles[0].recipeId == 'recipeId'
        recipeCampaignActionRecord.recipeTiles[0].html == '<html/>'
        recipeCampaignActionRecord.recipeTiles[0].recoType == 'trending'
        recipeCampaignActionRecord.recipeTiles[0].detailUrl == 'www.detail.url'
        recipeCampaignActionRecord.recipeTiles[1].customerId == 'customerId'
        recipeCampaignActionRecord.recipeTiles[1].campaignId == 'campaignId6527'
        recipeCampaignActionRecord.recipeTiles[1].position == '2'
        recipeCampaignActionRecord.recipeTiles[1].recipeId == 'recipeId'
        recipeCampaignActionRecord.recipeTiles[1].html == '<html/>'
        recipeCampaignActionRecord.recipeTiles[1].recoType == 'trending'
        recipeCampaignActionRecord.recipeTiles[1].detailUrl == 'www.detail.url'

        and:
        recipeCampaignActionRecord.jsonRecipes.size() == 2
        recipeCampaignActionRecord.jsonRecipes[0].recoType == "trending"
        recipeCampaignActionRecord.jsonRecipes[0].customerId == "customerId"
        recipeCampaignActionRecord.jsonRecipes[0].dayId == "23-01-2019"
        recipeCampaignActionRecord.jsonRecipes[0].recipes.isEmpty()
        recipeCampaignActionRecord.jsonRecipes[1].recoType == "trending"
        recipeCampaignActionRecord.jsonRecipes[1].customerId == "customerId"
        recipeCampaignActionRecord.jsonRecipes[1].dayId == "24-01-2019"
        recipeCampaignActionRecord.jsonRecipes[1].recipes.isEmpty()
    }
}
