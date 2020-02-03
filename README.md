# HBase ORM Extensions

## Introduction
Some extensions for the nice hbase-orm library, you can find the library here: [hbase-orm](https://flipkart-incubator.github.io/hbase-orm/)

This was primarily developed to be used with big table.

## Usage
This library provides a simple way to horizontally scale your hbase/bigtable row by simply annotating a corresponding member variable of your pojo class. Example:

```
public class Dependents implements Serializable {
    private Integer dependId;
}

@HBTable(name = "citizens", families = {@Family(name = "main"), @Family(name = "optional", versions = 10)})
public class Citizen implements HBRecord<String> {
    private static final String ROWKEY_DELIMITER = "#";
    @HBRowKey
    private String citizenId;
    @HBColumn(family = "dependents", qualifierField = "dependId")
    private List<Dependents> dependents; 
...
}
```

This will automatically store all `Dependents` in the list member variable `dependents` in the hbase table `citizens` using the following column qualifiers: 
```
- dependents:dependId#1234
- dependents:dependId#9876
- dependents:dependId#952
```

## License

Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
