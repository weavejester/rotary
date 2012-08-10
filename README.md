# Rotary

A Clojure client for Amazon's [DynamoDB][1] database.

[1]: http://aws.amazon.com/dynamodb/

## Installation

Add the following dependency to your Clojure project:

    [rotary "0.2.5"]

## Simple Example

    (def aws-credential {:access-key "myAccessKey", :secret-key "mySecretKey"})
    (query aws-credential "MyTable" "somePrimaryKey")
    (query aws-credential "AnotherTable" 22 `(> 13392) {:limit 100 :count true})

## Documentation

* [API Docs](http://weavejester.github.com/rotary)

## Testing

To run the unit tests: 

	$ lein midje 'unit.*'

To run the integration tests, set the environment variables 
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your AWS credentials, 
then run

	$ lein midje 'integration.*'

This will take a bit of time, since the tests have to create tables and wait
for Amazon to activate them.

## License

Copyright (C) 2012 James Reeves

Distributed under the Eclipse Public License, the same as Clojure.
