# beats-outputs-http

Ships logs to a HTTP endpoint without any fuzz.

## Setup

Include the line `_ "github.com/martiningesen/beats-outputs-http"` in your main.go file like this:

```
package main

import (
  "os"

  _ "github.com/martiningesen/beats-outputs-http"
  "github.com/elastic/beats/filebeat/cmd"
)

func main() {
  if err := cmd.RootCmd.Execute(); err != nil {
    os.Exit(1)
  }
}

```

## Config

Add this to your config yaml:

```
output.http:
  hosts: ["localhost:1337"]
```


## Credits
Based on [https://github.com/raboof/beats-output-http](https://github.com/raboof/beats-output-http)
