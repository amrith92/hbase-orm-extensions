package io.github.oemergenc.hbase.orm.extensions.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HomeAddress implements Serializable {
    String homeAddress;
}
