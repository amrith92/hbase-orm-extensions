package io.github.oemergenc.hbase.orm.extensions.domain.recipe;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ContentRecipeJsonDto implements Serializable {
    String customerId;
    String dayId;
    List<JsonNode> recipes;
    String recoType;
}
