package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.Serializable;

@Data
@RequiredArgsConstructor
public class RecipeTile implements Serializable {

    private final String customerId;
    private final String campaignId;
    private final Integer position;
    private final String recipeId;
    private final String html;
    @With
    private final String detailUrl;
    @With
    private final String recoType;
    private final String campaignPosition;
}
