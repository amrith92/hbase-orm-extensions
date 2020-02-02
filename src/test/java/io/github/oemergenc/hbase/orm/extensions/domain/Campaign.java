package io.github.oemergenc.hbase.orm.extensions.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Campaign implements Serializable {
    private String campaignId;
    private String customerId;
}
