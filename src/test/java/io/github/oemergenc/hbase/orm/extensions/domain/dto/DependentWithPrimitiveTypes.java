package io.github.oemergenc.hbase.orm.extensions.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DependentWithPrimitiveTypes implements Serializable {

    private String dynamicPart1;
    private String dynamicPart2;
    private String position;
    private String recipeId;
    private String html;
}
