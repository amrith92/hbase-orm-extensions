package io.github.oemergenc.hbase.orm.extensions.dynamic.processor.alias;

import com.flipkart.hbaseobjectmapper.codec.Codec;
import io.github.oemergenc.hbase.orm.extensions.HBDynamicColumn;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.StringUtils;

public class DynamicAliasFactory {

    public static AliasHandler getHandler(HBDynamicColumn column, Codec codec) {
        String alias = column.alias();
        String separator = column.separator();
        if (StringUtils.isNotBlank(alias)) {
            return new DynamicAliasHandler(alias, separator, codec);
        } else {
            return new DynamicNoAliasHandler();
        }
    }
}
