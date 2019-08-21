# go-esworker
`go-esworker` is an async worker that documents can bulk insert, update, delete to the elasticsearch using Golang.
It is support to an infrastructure on AWS, GCP, Elastic Cloud, and so on.
<p align="left"> 
   <a href="https://hits.seeyoufarm.com"/><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Fgjbae1212%2Fgo-esworker"/></a>
   <a href="https://circleci.com/gh/gjbae1212/go-esworker"><img src="https://circleci.com/gh/gjbae1212/go-esworker.svg?style=svg"></a>   
   <a href="https://goreportcard.com/report/github.com/gjbae1212/go-esworker"><img src="https://goreportcard.com/badge/github.com/gjbae1212/go-esworker"/></a>         
   <a href="https://godoc.org/github.com/gjbae1212/go-esworker"><img src="https://godoc.org/github.com/gjbae1212/go-esworker?status.svg"/></a>   
   <a href="/LICENSE"><img src="https://img.shields.io/badge/license-MIT-GREEN.svg" alt="license"/></a>
</p>

## Installation
```bash
go get -u github.com/gjbae1212/go-esworker
```


## Usage
```go
import (
	"context"
	"log"

	"github.com/gjbae1212/go-esworker"
)

func main() {

	// Create dispatcher
	dispatcher, err := esworker.NewDispatcher(
    		esworker.WithESVersionOption(esworker.V6),
    		esworker.WithAddressesOption([]string{"http://localhost:9200"}),
    		esworker.WithUsernameOption("user"),
    		esworker.WithPasswordOption("password"),
    		esworker.WithErrorHandler(func(err error) {
    			log.Println(err)
    		}),
    	)
    	if err != nil {
    		log.Panic(err)
    	}

	// Start dispatcher
	if err := dispatcher.Start(); err != nil {
		log.Panic(err)
	}

	// Process operations in bulk.
	ctx := context.Background()
	// create doc
	dispatcher.AddAction(ctx, &esworker.StandardAction{
		op:    esworker.ES_CREATE,
		index: "allan",
		id:    "1",
		doc:   map[string]interface{}{"field1": 10},
	})

	// update doc
	dispatcher.AddAction(ctx, &esworker.StandardAction{
		op:    esworker.ES_UPDATE,
		index: "allan",
		id:    "1",
		doc:   map[string]interface{}{"field1": 20},
	})

	// delete doc
	dispatcher.AddAction(ctx, &esworker.StandardAction{
		op:    esworker.ES_DELETE,
		index: "allan",
		id:    "1",
	})
}

```


## Dispatcher Parameters
It should pass parameters for dependency injection when you are creating a `go-esworker` dispatcher.  
A list to support the parameters below.  

| method name | description | value | state |
|-------------|-------------|-------|-------|
| **WithESVersionOption** | ElasticSearch Version | esworker.V5, esworker.V6, esworker.V7 | default `V6` |
| **WithAddressesOption** | ElasticSearch Address | | default `http://localhost:9200` |
| **WithUsernameOption** | ElasticSearch Username for HTTP basic authentication| | optional |
| **WithPasswordOption** | ElasticSearch Password for HTTP basic authentication | | optional |
| **WithCloudIdOption**  | ID for Elastic Cloud | | optional |
| **WithApiKeyOption**  | Base64-Encoded value for authorization(api-key) | | optional(if set, overrides username and password) |
| **WithTransportOption** | Http transport | | default `http default transport` |
| **WithLoggerOption** | Logger | | optional |
| **WithGlobalQueueSizeOption** | Global queue max size | | default `5000` |
| **WithWorkerSizeOption** | Worker size | | default `5` |
| **WithWorkerQueueSizeOption** | Worker max queue size | | default `5` |
| **WithWorkerWaitInterval** | Deal with data in worker queue after every interval time | | default `2 * time.Second` |
| **WithErrorHandler** | A function that deals with an error when an error is raised | | optional |  


## Action Interface
To deal with operation as insert and update and delete to, you would use to the `StandardAction` struct or a struct which is implementing `esworker.Action` interface.
```go
// generate and start dispatcher 
dispatcher, _ := esworker.NewDispatcher()
dispatcher.Start()

// Ex) Standard Action Example
act := &esworker.StandardAction{
	Op: ES_CREATE
	Index: "sample",
	DocType: "_doc",
	Id: "test-id",
	Doc: map[string]interface{}{"field": 1},
}
dispatcher.AddAction(context.Background(), act)


// Ex) Custom Action Example
sampleAction struct {}

func (act *sampleAction) GetOperation() esworker.ESOperation {
	// return esworker.ES_CREATE
	// return esworker.ES_INDEX
	// return esworker.ES_UPDATE
	// return esworker.ES_DELETE    
}

func (act *sampleAction) GetIndex() string {
	// return "your index name"
}

func (act *sampleAction) GetDocType() string {
	//return ""
	//return "doc type"	
}

func (act *sampleAction) GetID() string {
	//return ""
	//return "doc id"		
}

func (act *sampleAction) GetDoc() map[string]interface{} {
	//return map[string]interface{}{}
}
dispatcher.AddAction(context.Background(), &sampleAction{})

``` 
If you will make to a custom struct which is implementing `esworker.Action` interface, it must implement 5 methods.  

| name          | description |
|---------------|----------------------------------------------|
| **GetOperation**  |  ES_CREATE, ES_INDEX, ES_UPDATE, ES_DELETE |
| **GetIndex**      |  index name |
| **GetDocType**    |  doc type (if it is returned an empty string, default `_doc` or `doc`) |
| **GetID**         |  doc id (if an operation is ES_INDEX, possible `empty string`) |
| **GetDoc**        |  doc data |


## Elastic Cloud
If you use to infrastructure on Elastic Cloud, you could access to ElasticSearch without endpoint and basic authentication.
[(**How to use API-KEY)**](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html)
```go
dispatcher, err := esworker.NewDispatcher(
		esworker.WithESVersionOption(esworker.V7),
		esworker.WithCloudIdOption("your-cloud-id"),
		esworker.WithApiKeyOption("api-key"),
)
dispatcher.Start()
```
 
### LICENSE
This project is following The MIT.
