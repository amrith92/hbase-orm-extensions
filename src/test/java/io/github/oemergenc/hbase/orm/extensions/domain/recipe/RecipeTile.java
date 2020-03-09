package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecipeTile implements Serializable {

    private String customerId;
    private String campaignId;
    private String position;
    private String recipeId;
    private String html;
    @With
    private String detailUrl;
    @With
    private String recoType;
}
