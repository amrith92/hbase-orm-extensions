package io.github.oemergenc.hbase.orm.extensions.domain.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DependentWithListType implements Serializable {
    String dynamicPart1;
    String otherId;
    List<JsonNode> nodes;
    String aString;
}
