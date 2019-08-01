package esworker

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	assert "github.com/stretchr/testify/assert"
)

func TestWithESVersionOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithESVersionOption(V5)
	f.apply(cfg)
	assert.Equal(V5, cfg.version)
}

func TestWithAddressesOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithAddressesOption([]string{"1.1.1.1", "2.2.2.2"})
	f.apply(cfg)
	assert.Len(cfg.addrs, 2)
	assert.Equal("1.1.1.1", cfg.addrs[0])
	assert.Equal("2.2.2.2", cfg.addrs[1])
}

func TestWithUsernameOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithUsernameOption("allan")
	f.apply(cfg)
	assert.Equal("allan", cfg.username)
}

func TestWithPasswordOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithPasswordOption("password")
	f.apply(cfg)
	assert.Equal("password", cfg.password)
}

func TestWithCloudIdOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithCloudIdOption("cloud-id")
	f.apply(cfg)
	assert.Equal("cloud-id", cfg.cloudId)
}

func TestWithApiKeyOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithApiKeyOption("api-key")
	f.apply(cfg)
	assert.Equal("api-key", cfg.apiKey)
}

func TestWithTransportOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithTransportOption(http.DefaultTransport)
	f.apply(cfg)
	assert.Equal(cfg.transport, http.DefaultTransport)
}

func TestWithLoggerOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	logger := &Logger{EnableRequestBody: true, EnableResponseBody: false}
	f := WithLoggerOption(logger)
	f.apply(cfg)
	assert.Equal(logger, cfg.logger)
}

func TestWithGlobalQueueSizeOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithGlobalQueueSizeOption(10)
	f.apply(cfg)
	assert.Equal(10, cfg.globalQueueSize)
}

func TestWithWorkerSizeOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithWorkerSizeOption(20)
	f.apply(cfg)
	assert.Equal(20, cfg.workerSize)
}

func TestWithWorkerQueueSizeOption(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithWorkerQueueSizeOption(20)
	f.apply(cfg)
	assert.Equal(20, cfg.workerQueueSize)
}

func TestWithWorkerWaitInterval(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithWorkerWaitInterval(time.Second * 2)
	f.apply(cfg)
	assert.Equal(time.Second*2, cfg.workerWaitInterval)
}

func TestWithErrorHandler(t *testing.T) {
	assert := assert.New(t)

	cfg := &config{}
	f := WithErrorHandler(func(err error) {
		fmt.Println(err)
	})
	f.apply(cfg)
	assert.NotEmpty(cfg.errorHandler)
}
