---
title: beanstalkd
type: input
status: experimental
categories: ["Services"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/input/beanstalkd.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::caution EXPERIMENTAL
This component is experimental and therefore subject to change or removal outside of major version releases.
:::
Reads messages from Beanstalkd queue.

Introduced in version 3.46.0.

```yml
# Config fields, showing default values
input:
  label: ""
  beanstalkd:
    tcp_address: ""
```

## Fields

### `tcp_address`

Beanstalkd address to connect to.


Type: `string`  

```yml
# Examples

tcp_address: 127.0.0.1:11300
```

