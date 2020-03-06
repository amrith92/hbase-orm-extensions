package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Value;

import java.io.Serializable;
import java.util.List;

@Value
public class ContentRecipeJsonDto implements Serializable {
    String customerId;
    String dayId;
    List<JsonNode> recipes;
    String recoType;
}
