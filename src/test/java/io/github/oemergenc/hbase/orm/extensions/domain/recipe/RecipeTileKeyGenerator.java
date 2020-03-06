package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import io.github.oemergenc.hbase.orm.extensions.DynamicColumnKeyGenerator;

public class RecipeTileKeyGenerator implements DynamicColumnKeyGenerator<RecipeTile> {
    @Override
    public String generateKey(RecipeTile recipeTile) {
        return recipeTile.getCampaignId() + ":" + recipeTile.getPosition();
    }
}
