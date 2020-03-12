package io.github.oemergenc.hbase.orm.extensions.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DependentWithMap implements Serializable {
    private String dynamicId;
    private Map<String, String> products;
}
