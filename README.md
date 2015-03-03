# Sql input plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **property1**: description (string, required)
- **property2**: description (integer, default: default-value)

## Example

```yaml
in:
  type: sql
  property1: example1
  property2: example2
```


## Build

```
$ ./gradlew gem
```
