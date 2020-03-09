# HBase ORM Extensions
![Build master branch](https://github.com/oemergenc/hbase-orm-extensions/workflows/Build%20master%20branch/badge.svg?branch=master)
## Introduction
Some extensions for the nice hbase-orm library, you can find the library here: [hbase-orm](https://flipkart-incubator.github.io/hbase-orm/)

This was primarily developed to be used with big table.

## Usage
This library provides a simple way to horizontally scale your hbase/bigtable row by simply annotating a corresponding member variable of your pojo class. Example:

Add the following dependencies to your project:

Gradle:
```
implementation group: 'io.github.oemergenc', name: 'hbase-orm-extensions', version: '0.0.7'
```
Maven:
```
<dependency>
  <groupId>io.github.oemergenc</groupId>
  <artifactId>hbase-orm-extensions</artifactId>
</dependency>
```

Annotate your class according to the following example:
```
public class Dependents implements Serializable {
    private String dependId;
    private String anotherId;
}

@HBTable(name = "citizens", families = {@Family(name = "dependents"), @Family(name = "optional", versions = 10)})
public class Citizen implements HBRecord<String> {
    private static final String ROWKEY_DELIMITER = "#";
    @HBRowKey
    private String citizenId;
    @HBDynamicColumn(family = "dependents", qualifier = @DynamicQualifier(parts = {"dependId", "anotherId"}))
    private List<Dependents> dependents; 
...
}
```

This will automatically store all `Dependents` in the list member variable `dependents` in the hbase table `citizens` using the following column qualifier scheme `dependents:dependId#dependId$anotherId`: 
```
- dependents:dependId#9876$bcdhs123
- dependents:dependId#9529$nkj9280
```

## License

Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
