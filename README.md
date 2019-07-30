# HuaweiCloud DIS Flink Connector

HuaweiCloud DIS Flink Connector is a connector provided by the Data Access Service (DIS). It can be used to create a DStream to connect to Flink.

Quick Links:
- issue
- [DIS Homepage](https://www.huaweicloud.com/en-us/product/dis.html), or Chinese language site [数据接入服务](https://www.huaweicloud.com/product/dis.html)

## Getting Started
### Requirements
- JDK 1.8+
- Scala-sdk-2.11

### Install the JDK
The recommended way to use the DIS SDK for DIS Flink Connector in your project is to consume it from Maven.

#### Specify the SDK Maven dependency
    <dependency>
        <groupId>com.huaweicloud.dis</groupId>
        <artifactId>huaweicloud-dis-flink-connector_2.11</artifactId>
        <version>1.0.0</version>
        <scope>compile</scope>
    </dependency>


## Building From Source
Once you check out the code from GitHub, you can build it using Maven:

    mvn clean install

## License
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)