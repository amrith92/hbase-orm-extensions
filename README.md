# HBase ORM Extensions

## Introduction
Some extensions for the nice hbase-orm library, you can find the library here: [hbase-orm](https://flipkart-incubator.github.io/hbase-orm/)

This was primarily developed to be used with big table.

## Usage
This library provides a simple way to horizontally scale your hbase/bigtable row by simply annotating a corresponding member variable of your pojo class. Example:

```
@HBTable(name = "campaigns", families = {@Family(name = "campaign")})
public class CampaignRecord implements HBRecord<String> {
    @HBRowKey
    @HBColumn(family = "campaign", column = "customerId")
    private String customerId;

    @HBDynamicColumn(family = "campaign", qualifierField = "campaignId", alias = "id")
    private List<Campaign> campaigns;
...
}
```

This will automatically store all `Campaigns` in the list member variable `campaigns` in the hbase table `campaign` using the following column qualifiers: `campaign:id#123123`

## License

Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
