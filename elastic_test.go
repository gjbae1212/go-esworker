package esworker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stretchr/testify/assert"
)

func TestESResponseBulk_Count(t *testing.T) {
	assert := assert.New(t)

	tests := map[string]struct {
		input   *ESResponseBulk
		success int
		fail    int
	}{
		"empty": {input: &ESResponseBulk{}, success: 0, fail: 0},
		"exist": {input: &ESResponseBulk{
			Items: []ESResponseItem{
				ESResponseItem{Index: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Index: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Update: ESResponseStatus{
					Status: 500,
				}},
				ESResponseItem{Update: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 400,
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 201,
				}},
				ESResponseItem{Delete: ESResponseStatus{
					Status: 201,
				}},
			},
		}, success: 6, fail: 2},
	}

	for _, t := range tests {
		success, fail := t.input.Count()
		assert.Equal(t.success, success)
		assert.Equal(t.fail, fail)
	}
}

func TestESResponseBulk_ResultErrors(t *testing.T) {
	assert := assert.New(t)

	tests := map[string]struct {
		input *ESResponseBulk
		isErr bool
	}{
		"empty": {input: &ESResponseBulk{}, isErr: false},
		"exist": {input: &ESResponseBulk{
			Items: []ESResponseItem{
				ESResponseItem{Index: ESResponseStatus{
					Status: 400,
					Error: ESResponseError{
						Cause: ESResponseCause{
							Type:   "type2",
							Reason: "reason2",
						},
					},
				}},
				ESResponseItem{Index: ESResponseStatus{
					Status: 400,
					Error:  ESResponseError{},
				}},
				ESResponseItem{Update: ESResponseStatus{
					Status: 500,
					Error: ESResponseError{
						Type:   "type",
						Reason: "reason",
						Cause: ESResponseCause{
							Type:   "type2",
							Reason: "reason2",
						},
					},
				}},
				ESResponseItem{Update: ESResponseStatus{
					Status: 400,
					Error: ESResponseError{
						Type:   "type",
						Reason: "reason",
					},
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 200,
				}},
				ESResponseItem{Create: ESResponseStatus{
					Status: 400,
				}},
				ESResponseItem{Delete: ESResponseStatus{
					Status: 201,
				}},
			},
		}, isErr: true},
	}

	for _, t := range tests {
		err := t.input.ResultError()
		assert.Equal(t.isErr, err != nil)
		fmt.Println(err)
	}
}

func TestESOperation_GetString(t *testing.T) {
	assert := assert.New(t)

	tests := map[string]struct {
		input  ESOperation
		output string
	}{
		"index":   {input: ES_INDEX, output: "index"},
		"create":  {input: ES_CREATE, output: "create"},
		"update":  {input: ES_UPDATE, output: "update"},
		"delete":  {input: ES_DELETE, output: "delete"},
		"unknown": {input: ESOperation(-1), output: ""},
	}

	for _, t := range tests {
		assert.Equal(t.output, t.input.GetString())
	}
}

func TestEsproxy(t *testing.T) {
	assert := assert.New(t)

	cfg := testCfg(V6)
	// make proxy on ES
	_, err := createESProxy(nil)
	assert.Error(err)

	proxy, err := createESProxy(cfg)
	assert.NoError(err)

	// get client on ES
	s := proxy.(*esproxy)

	_, err = s.getES5()
	assert.NoError(err)

	_, err = s.getES6()
	assert.NoError(err)

	_, err = s.getES7()
	assert.NoError(err)
}

func TestESProxy_Bulk(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	now := time.Now().UnixNano()

	if os.Getenv("WITHOUT_CONTAINER") != "" {
		return
	}

	// doc custom
	acts := []Action{
		&mockAction{
			op:      ES_CREATE,
			index:   "allan",
			docType: "mycustom",
			id:      fmt.Sprintf("%d", now),
			doc: map[string]interface{}{
				"field1": 100,
				"field2": "create",
			},
		},
		&mockAction{
			op:      ES_INDEX,
			index:   "allan",
			docType: "mycustom",
			id:      fmt.Sprintf("%d", now+2),
			doc: map[string]interface{}{
				"field1": 300,
				"field2": "index",
			},
		},
		&mockAction{
			op:      ES_INDEX,
			index:   "allan",
			docType: "mycustom",
			doc: map[string]interface{}{
				"field1": 400,
				"field2": "index",
			},
		},
		&mockAction{
			op:      ES_UPDATE,
			index:   "allan",
			docType: "mycustom",
			id:      fmt.Sprintf("%d", now+2),
			doc: map[string]interface{}{
				"doc": map[string]interface{}{
					"field1": 500,
					"field2": "modify",
				},
			},
		},
		&mockAction{
			op:      ES_DELETE,
			index:   "allan",
			docType: "mycustom",
			id:      fmt.Sprintf("%d", now),
		},
	}

	// doc default
	acts2 := []Action{
		&mockAction{
			op:      ES_INDEX,
			index:   "allan",
			docType: "mycustom2",
			id:      fmt.Sprintf("%d", now+1),
			doc: map[string]interface{}{
				"field1": 200,
				"field2": "index-_doc",
			},
		},
	}

	acts3 := []Action{
		&mockAction{
			op:    ES_INDEX,
			index: "allan",
			id:    fmt.Sprintf("%d", now+100),
			doc: map[string]interface{}{
				"field1": 200,
				"field2": "index-default",
			},
		},
	}

	errActs1 := []Action{
		&mockAction{
			op:      ES_CREATE,
			index:   "allan",
			docType: "mycustom",
			doc: map[string]interface{}{
				"field1": 100,
				"field2": "create",
			},
		},
	}

	errActs2 := []Action{
		&mockAction{
			op:      ES_UPDATE,
			index:   "allan",
			id:      fmt.Sprintf("%d", now+10),
			docType: "mycustom",
			doc: map[string]interface{}{
				"field1": 100,
				"field2": "create",
			},
		},
	}

	// [INFO] `testcontainers` is third party library to start docker container using docker golang library.

	// mock es5
	reqes5 := testcontainers.ContainerRequest{
		Image:        "elasticsearch:5.6",
		Name:         "es5-mock",
		Env:          map[string]string{"discovery.type": "single-node"},
		ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
		WaitingFor:   wait.ForLog("started"),
	}
	es5Mock, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqes5,
		Started:          true,
	})
	assert.NoError(err)

	proxy, err := createESProxy(testCfg(V5))
	assert.NoError(err)

	_, err = proxy.Bulk(ctx, errActs1)
	assert.Error(err)
	_, err = proxy.Bulk(ctx, errActs2)
	assert.Error(err)

	result, err := proxy.Bulk(ctx, acts)
	assert.NoError(err)
	success, fail := result.Count()
	assert.Equal(5, success)
	assert.Equal(0, fail)

	result, err = proxy.Bulk(ctx, acts2)
	assert.NoError(err)
	success, fail = result.Count()
	assert.Equal(1, success)
	assert.Equal(0, fail)

	result, err = proxy.Bulk(ctx, acts3)
	assert.NoError(err)
	success, fail = result.Count()
	success, fail = result.Count()
	assert.Equal(1, success)
	assert.Equal(0, fail)
	es5Mock.Terminate(ctx)

	// mock es6
	reqes6 := testcontainers.ContainerRequest{
		Image:        "elasticsearch:6.8.0",
		Name:         "es6-mock",
		Env:          map[string]string{"discovery.type": "single-node"},
		ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
		WaitingFor:   wait.ForLog("started"),
	}
	es6Mock, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqes6,
		Started:          true,
	})
	assert.NoError(err)

	proxy, err = createESProxy(testCfg(V6))
	assert.NoError(err)

	_, err = proxy.Bulk(ctx, errActs1)
	assert.Error(err)
	_, err = proxy.Bulk(ctx, errActs2)
	assert.Error(err)

	result, err = proxy.Bulk(ctx, acts)
	assert.NoError(err)
	success, fail = result.Count()
	assert.Equal(5, success)
	assert.Equal(0, fail)

	// not insert
	result, err = proxy.Bulk(ctx, acts2)
	assert.NoError(err)
	success, fail = result.Count()
	assert.Equal(0, success)
	assert.Equal(1, fail)

	// default mycustom
	result, err = proxy.Bulk(ctx, acts3)
	assert.NoError(err)
	success, fail = result.Count()
	success, fail = result.Count()
	assert.Equal(1, success)
	assert.Equal(0, fail)
	es6Mock.Terminate(ctx)

	// mock es7
	reqes7 := testcontainers.ContainerRequest{
		Image:        "elasticsearch:7.3.0",
		Name:         "es7-mock",
		Env:          map[string]string{"discovery.type": "single-node"},
		ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
		WaitingFor:   wait.ForLog("started"),
	}
	es7Mock, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqes7,
		Started:          true,
	})
	assert.NoError(err)

	proxy, err = createESProxy(testCfg(V7))
	assert.NoError(err)

	_, err = proxy.Bulk(ctx, errActs1)
	assert.Error(err)
	_, err = proxy.Bulk(ctx, errActs2)
	assert.Error(err)

	result, err = proxy.Bulk(ctx, acts)
	assert.NoError(err)
	success, fail = result.Count()
	assert.Equal(5, success)
	assert.Equal(0, fail)

	// not insert
	result, err = proxy.Bulk(ctx, acts2)
	assert.NoError(err)
	success, fail = result.Count()
	assert.Equal(0, success)
	assert.Equal(1, fail)

	// default mycustom
	result, err = proxy.Bulk(ctx, acts3)
	assert.NoError(err)
	success, fail = result.Count()
	success, fail = result.Count()
	assert.Equal(1, success)
	assert.Equal(0, fail)
	es7Mock.Terminate(ctx)
}
