package esworker

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type (
	mockAction struct {
		op      ESOperation
		index   string
		docType string
		id      string
		doc     map[string]interface{}
	}
)

func (mockAct *mockAction) GetOperation() ESOperation {
	return mockAct.op
}

func (mockAct *mockAction) GetIndex() string {
	return mockAct.index
}

func (mockAct *mockAction) GetDocType() string {
	return mockAct.docType
}

func (mockAct *mockAction) GetID() string {
	return mockAct.id
}

func (mockAct *mockAction) GetDoc() map[string]interface{} {
	return mockAct.doc
}

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

	now := time.Now().UnixNano()

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
			op:    ES_INDEX,
			index: "allan",
			docType: "mycustom2",
			id:    fmt.Sprintf("%d", now+1),
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

	if os.Getenv("MOCK_ES5") != "" {
		proxy, err := createESProxy(testCfg(V5))
		assert.NoError(err)

		_, err = proxy.Bulk(errActs1)
		assert.Error(err)
		_, err = proxy.Bulk(errActs2)
		assert.Error(err)

		result, err := proxy.Bulk(acts)
		assert.NoError(err)
		success, fail := result.Count()
		assert.Equal(5, success)
		assert.Equal(0, fail)

		result, err = proxy.Bulk(acts2)
		assert.NoError(err)
		success, fail = result.Count()
		assert.Equal(1, success)
		assert.Equal(0, fail)

		result, err = proxy.Bulk(acts3)
		assert.NoError(err)
		success, fail = result.Count()
		success, fail = result.Count()
		assert.Equal(1, success)
		assert.Equal(0, fail)
	}

	if os.Getenv("MOCK_ES6") != "" {
		proxy, err := createESProxy(testCfg(V6))
		assert.NoError(err)

		_, err = proxy.Bulk(errActs1)
		assert.Error(err)
		_, err = proxy.Bulk(errActs2)
		assert.Error(err)

		result, err := proxy.Bulk(acts)
		assert.NoError(err)
		success, fail := result.Count()
		assert.Equal(5, success)
		assert.Equal(0, fail)

		// not insert
		result, err = proxy.Bulk(acts2)
		assert.NoError(err)
		success, fail = result.Count()
		assert.Equal(0, success)
		assert.Equal(1, fail)

		// default mycustom
		result, err = proxy.Bulk(acts3)
		assert.NoError(err)
		success, fail = result.Count()
		success, fail = result.Count()
		assert.Equal(1, success)
		assert.Equal(0, fail)
	}

	if os.Getenv("MOCK_ES7") != "" {
		proxy, err := createESProxy(testCfg(V7))
		assert.NoError(err)

		_, err = proxy.Bulk(errActs1)
		assert.Error(err)
		_, err = proxy.Bulk(errActs2)
		assert.Error(err)

		result, err := proxy.Bulk(acts)
		assert.NoError(err)
		success, fail := result.Count()
		assert.Equal(5, success)
		assert.Equal(0, fail)

		// not insert
		result, err = proxy.Bulk(acts2)
		assert.NoError(err)
		success, fail = result.Count()
		assert.Equal(0, success)
		assert.Equal(1, fail)

		// default mycustom
		result, err = proxy.Bulk(acts3)
		assert.NoError(err)
		success, fail = result.Count()
		success, fail = result.Count()
		assert.Equal(1, success)
		assert.Equal(0, fail)
	}
}

func testCfg(v ESVersion) *config {
	cfg := &config{}
	o := []Option{
		WithESVersionOption(v),
		WithTransportOption(http.DefaultTransport),
		WithGlobalQueueSizeOption(defaultGlobalQueueSize),
		WithWorkerSizeOption(defaultWorkerSize),
		WithWorkerQueueSizeOption(defaultWorkerQueueSize),
		WithWorkerWaitInterval(defaultWorkerWaitInterval),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	}
	for _, opt := range o {
		opt.apply(cfg)
	}
	return cfg
}
