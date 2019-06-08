## Crud4Dynamo
[![Build Status](https://travis-ci.org/aws-samples/crud4dynamo.svg?branch=master)](https://travis-ci.org/aws-samples/crud4dynamo)

Crud4Dynamo is a light-weight library built on-top of [AWS DynamoDB SDK](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.html) and provides the interfaces for common access patterns when dealing with DynamoDB.

See [Introduction section](https://github.com/aws-samples/crud4dynamo/wiki/Introduction) in the [wiki](https://github.com/aws-samples/crud4dynamo/wiki) for quick start.

## Requirement
* JDK >= 1.8

## Features
* Basic CRUD methods for Simple Key and Composite Key Models
* Paging support for read operations, e.g, query and scan
* Customizable CRUD supported by annotations
* Annotation based transaction support
* Simple caching for read operations

## Getting Help
See [wiki](https://github.com/aws-samples/crud4dynamo/wiki) for basic usages, examples and full documentation.

## Adding Crud4Dynamo to your build
Build artifact is published to [JFrog Bintray](https://bintray.com/alanzplus/crud4dynamo/crud4dynamo)

### Maven

```xml
<dependency>
  <groupId>com.amazon</groupId>
  <artifactId>crud4dynamo</artifactId>
  <version>1.0</version>
  <type>pom</type>
</dependency>
```

### Gradle

```groovy
repositories {
    maven {
        url 'https://dl.bintray.com/alanzplus/crud4dynamo'
    }
}

dependencies {
    implementation group: 'com.amazon', name: 'crud4dynamo', version: '1.0'
}
```

## License
This library is licensed under the Apache 2.0 License.
