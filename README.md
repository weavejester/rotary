# Rotary

A Clojure client for Amazon's [DynamoDB][1] database.

[1]: http://aws.amazon.com/dynamodb/

## Installation

Add the following dependency to your Clojure project:

    [rotary "0.2.5"]

## Simple Example

    (def aws-credential {:access-key "myAccessKey", :secret-key "mySecretKey"})
    (get-item aws-credential "MyTable" "somePrimaryKey")
    (query aws-credential "AnotherTable" 22 `(> 13392) :limit 100 :count true)
    (update-item aws-credential "MyFavoriteTable" [36 263] {"awesomeness" [:add 20] "updated" [:put 1339529420]})
    (put-item aws-credential "RandomTable" {"myHashKey" 777 "theRangeKey" 3843 "someOtherAttribute" 33} :return-values "ALL_OLD")

## Documentation

* [API Docs](http://weavejester.github.com/rotary)

## License

Copyright (C) 2012 James Reeves

Distributed under the Eclipse Public License, the same as Clojure.
