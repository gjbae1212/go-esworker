package esworker

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	if os.Getenv("WITHOUT_CONTAINER") != "" {
		return
	}

	escreq := testcontainers.ContainerRequest{
		Image:        "elasticsearch:6.8.0",
		Name:         "es6-mock",
		Env:          map[string]string{"discovery.type": "single-node"},
		ExposedPorts: []string{"9200:9200/tcp", "9300:9300/tcp"},
		WaitingFor:   wait.ForLog("started"),
	}
	es6mock, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: escreq,
		Started:          true,
	})
	assert.NoError(err)
	defer es6mock.Terminate(ctx)
	cfg := testCfg(V6)

	var workers []*worker
	pool := make(chan chan Action, 2)
	for i := 0; i < 2; i++ {
		escli, err := createESProxy(cfg)
		assert.NoError(err)
		w := &worker{
			esClient:     escli,
			id:           1,
			pool:         pool,
			pipe:         make(chan Action),
			maxQueueSize: 10,
			waitInterval: 1 * time.Second,
			errorHandler: func(err error) { fmt.Println(err) },
			quit:         make(chan bool),
		}
		workers = append(workers, w)
	}

	// enqueue
	for _, w := range workers {
		err := w.enqueue(&mockAction{})
		assert.NoError(err)
		err = w.enqueue(&mockAction{})
		assert.NoError(err)
		assert.Equal(w.queueSize(), 2)
	}

	// check queue size
	for _, w := range workers {
		assert.Equal(w.queueSize(), 2)
	}

	// start
	for _, w := range workers {
		go w.start()
	}

	// check queue size
	for _, w := range workers {
		time.Sleep(3 * time.Second)
		// a queue is empty after wait interval time
		assert.Equal(0, w.queueSize())
	}

	// stop
	for _, w := range workers {
		w.stop()
		time.Sleep(100 * time.Millisecond)
		// insert mock items
		err := w.enqueue(&mockAction{})
		assert.NoError(err)
		err = w.enqueue(&mockAction{})
		assert.NoError(err)
		assert.Equal(2, w.queueSize())
	}

	// check queue size
	for _, w := range workers {
		assert.Equal(2, w.queueSize())
	}

}
