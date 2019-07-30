package esworker

import (
	"testing"
	"github.com/stretchr/testify/assert"
	es6 "github.com/elastic/go-elasticsearch/v6"
)

func TestES(t *testing.T) {
	assert := assert.New(t)

	es6.NewDefaultClient()
}
