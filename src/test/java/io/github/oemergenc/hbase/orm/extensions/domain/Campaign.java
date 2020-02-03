package io.github.oemergenc.hbase.orm.extensions.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@RequiredArgsConstructor
@NoArgsConstructor
public class Campaign implements Serializable {
    @NonNull
    private String campaignId;
    @NonNull
    private String customerId;
    private Map<String, String> products;
}
