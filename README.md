# Rotary

A Clojure client for Amazon's [DynamoDB][1] database.

[1]: http://aws.amazon.com/dynamodb/

## Installation

Add the following dependency to your Clojure project:

    [rotary "0.4.1"]

## Simple Example

    (def aws-credential {:access-key "myAccessKey", :secret-key "mySecretKey"})
    (query aws-credential "MyTable" "somePrimaryKey")
    (query aws-credential "AnotherTable" 22 `(> 13392) {:limit 100 :count true})

## Documentation

* [API Docs](http://weavejester.github.com/rotary)

## License

Copyright © 2013 James Reeves

Distributed under the Eclipse Public License, the same as Clojure.
