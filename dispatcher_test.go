package esworker

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"

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

func testCfg(v ESVersion) *config {
	cfg := &config{}
	o := []Option{
		WithESVersionOption(v),
		WithTransportOption(http.DefaultTransport),
		WithGlobalQueueSizeOption(defaultGlobalQueueSize),
		WithWorkerSizeOption(defaultWorkerSize),
		WithWorkerQueueSizeOption(defaultWorkerQueueSize),
		WithWorkerWaitInterval(time.Second),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	}
	for _, opt := range o {
		opt.apply(cfg)
	}
	return cfg
}

func TestNewDispatcher(t *testing.T) {
	assert := assert.New(t)

	d, err := NewDispatcher(WithESVersionOption(V5))
	assert.NoError(err)
	assert.Equal(d.(*dispatcher).cfg.version, V5)

	d, err = NewDispatcher(WithAddressesOption([]string{"aa", "bb"}))
	assert.NoError(err)
	assert.Equal("aa", d.(*dispatcher).cfg.addrs[0])
	assert.Equal("bb", d.(*dispatcher).cfg.addrs[1])

	d, err = NewDispatcher(WithUsernameOption("username"))
	assert.NoError(err)
	assert.Equal("username", d.(*dispatcher).cfg.username)

	d, err = NewDispatcher(WithPasswordOption("password"))
	assert.NoError(err)
	assert.Equal("password", d.(*dispatcher).cfg.password)

	d, err = NewDispatcher(WithCloudIdOption("cloudid"))
	assert.NoError(err)
	assert.Equal("cloudid", d.(*dispatcher).cfg.cloudId)

	d, err = NewDispatcher(WithApiKeyOption("apikey"))
	assert.NoError(err)
	assert.Equal("apikey", d.(*dispatcher).cfg.apiKey)

	d, err = NewDispatcher(WithGlobalQueueSizeOption(100))
	assert.NoError(err)
	assert.Equal(cap(d.(*dispatcher).bk.queue), 100)

	d, err = NewDispatcher(WithWorkerSizeOption(20))
	assert.NoError(err)
	assert.Len(d.(*dispatcher).bk.workers, 20)

	d, err = NewDispatcher(WithWorkerQueueSizeOption(100))
	assert.NoError(err)
	assert.Equal(d.(*dispatcher).bk.workers[0].maxQueueSize, 100)

	d, err = NewDispatcher(WithWorkerWaitInterval(5 * time.Second))
	assert.NoError(err)
	assert.Equal(d.(*dispatcher).bk.workers[0].waitInterval, (5 * time.Second))

}

func TestDispatcher_AddAction(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	d, err := NewDispatcher(
		WithESVersionOption(V6),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	)
	assert.NoError(err)
	err = d.Start()
	assert.NoError(err)

	tests := map[string]struct {
		input   Action
		isError bool
	}{
		"action nil":        {input: nil, isError: true},
		"index nil":         {input: &mockAction{}, isError: true},
		"create and no id":  {input: &mockAction{index: "allan", op: ES_CREATE}, isError: true},
		"update and no doc": {input: &mockAction{index: "allan", op: ES_UPDATE}, isError: true},
		"ok": {input: &mockAction{index: "allan", op: ES_UPDATE, doc: map[string]interface{}{
			"doc": "aaa",
		}}, isError: false},
	}

	for _, t := range tests {
		err := d.AddAction(ctx, t.input)
		if t.isError {
			assert.Error(err)
		} else {
			assert.NoError(err)
		}
	}
}

func TestDispatcher_Start(t *testing.T) {
	assert := assert.New(t)

	d, err := NewDispatcher(
		WithESVersionOption(V6),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	)
	assert.NoError(err)

	err = d.Start()
	assert.NoError(err)

	err = d.Start()
	assert.Error(err)
}

func TestDispatcher_Stop(t *testing.T) {
	assert := assert.New(t)

	d, err := NewDispatcher(
		WithESVersionOption(V6),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	)
	assert.NoError(err)

	err = d.Stop()
	assert.Error(err)

	err = d.Start()
	assert.NoError(err)

	time.Sleep(1 * time.Second)
	err = d.Stop()
	assert.NoError(err)
}

func TestProcessDispatcher(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	escreq := testcontainers.ContainerRequest{
		Image:        "elasticsearch:6.8.0",
		Name:         "es6-dispatcher",
		Env:          map[string]string{"discovery.type": "single-node"},
		ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
		WaitingFor:   wait.ForLog("started"),
	}
	mockES, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: escreq,
		Started:          true,
	})
	assert.NoError(err)
	defer mockES.Terminate(ctx)

	dp, err := NewDispatcher(
		WithESVersionOption(V6),
		WithGlobalQueueSizeOption(1000),
		WithErrorHandler(func(err error) {
			fmt.Printf("[err] %+v\n", err)
		}),
	)
	assert.NoError(err)
	err = dp.Start()
	assert.NoError(err)

	for i := 0; i < 9999; i++ {
		m := &mockAction{
			op:      ES_INDEX,
			index:   "allan",
			docType: "benchmark-index",
			doc:     map[string]interface{}{"field1": 200},
		}
		err := dp.AddAction(ctx, m)
		assert.NoError(err)
	}

	fmt.Println("before stop")
	dp.(*dispatcher).Stop()
	fmt.Println("after stop")
	dp.(*dispatcher).Start()
	for i := 0; i < 9999; i++ {
		m := &mockAction{
			op:      ES_INDEX,
			index:   "allan",
			docType: "benchmark-index",
			doc:     map[string]interface{}{"field1": 200},
		}
		err := dp.AddAction(ctx, m)
		assert.NoError(err)
	}
	fmt.Println("before stop")
	dp.(*dispatcher).Stop()
	fmt.Println("after stop")
	time.Sleep(1 * time.Second)
}

var mockBenchmarkFlag bool

func BenchmarkDispatcher_AddAction(b *testing.B) {
	if !mockBenchmarkFlag {
		mockBenchmarkFlag = true
	} else {
		ctx := context.Background()
		escreq := testcontainers.ContainerRequest{
			Image:        "elasticsearch:6.8.0",
			Name:         "es6-benchmark",
			Env:          map[string]string{"discovery.type": "single-node"},
			ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
			WaitingFor:   wait.ForLog("started"),
		}
		mockES, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: escreq,
			Started:          true,
		})
		if err != nil {
			log.Panic(err)
		}
		defer mockES.Terminate(ctx)
		dp, err := NewDispatcher(
			WithESVersionOption(V6),
			WithGlobalQueueSizeOption(1000),
			WithErrorHandler(func(err error) {
				fmt.Printf("[err] %+v\n", err)
			}),
		)
		if err != nil {
			log.Panic(err)
		}
		if err := dp.Start(); err != nil {
			log.Panic(err)
		}
		// start benchmark test
		for i := 0; i < b.N; i++ {
			m := &mockAction{
				op:      ES_INDEX,
				index:   "allan",
				docType: "benchmark-index",
				doc:     map[string]interface{}{"field1": 200},
			}
			err := dp.AddAction(ctx, m)
			if err != nil {
				b.Error(err)
			}
		}
		fmt.Println("before stop")
		dp.(*dispatcher).Stop()
		fmt.Println("after stop")
	}
}
