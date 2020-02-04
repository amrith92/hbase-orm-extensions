package io.github.oemergenc.hbase.orm.extensions.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
public class Campaign implements Serializable {
    private String campaignId;
    private String customerId;
    private Map<String, String> products;

    public Campaign(String campaignId, String customerId) {
        this.campaignId = campaignId;
        this.customerId = customerId;
    }
}
