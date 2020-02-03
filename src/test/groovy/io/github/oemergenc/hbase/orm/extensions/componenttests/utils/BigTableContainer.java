package io.github.oemergenc.hbase.orm.extensions.componenttests.utils;

import org.testcontainers.containers.GenericContainer;

public class BigTableContainer extends GenericContainer<BigTableContainer> {

    public static final Integer BIG_TABLE_PORT = 8080;

    public BigTableContainer() {
        super("google/cloud-sdk:latest:latest");
        withExposedPorts(BIG_TABLE_PORT);
        withCommand("gcloud beta emulators bigtable start --host-port=0.0.0.0:" + BIG_TABLE_PORT);
    }
}
