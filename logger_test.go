package esworker

import (
	"reflect"
	"testing"

	es5_logger "github.com/elastic/go-elasticsearch/v5/estransport"
	es6_logger "github.com/elastic/go-elasticsearch/v6/estransport"
	es7_logger "github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/stretchr/testify/assert"
)

func TestLogger_GetESLogger(t *testing.T) {
	assert := assert.New(t)

	tests := map[string]struct {
		v          ESVersion
		input      *Logger
		output     interface{}
		outputType interface{}
	}{
		"es5-text": {
			v:          V5,
			input:      &Logger{Type: LOGGER_TYPE_TEXT, EnableResponseBody: true},
			outputType: reflect.TypeOf(&es5_logger.TextLogger{}),
		},
		"es6-color": {
			v:          V6,
			input:      &Logger{Type: LOGGER_TYPE_COLOR, EnableRequestBody: true},
			outputType: reflect.TypeOf(&es6_logger.ColorLogger{}),
		},
		"es7-json": {
			v:          V7,
			input:      &Logger{Type: LOGGER_TYPE_JSON, EnableResponseBody: true, EnableRequestBody: true},
			outputType: reflect.TypeOf(&es7_logger.JSONLogger{}),
		},
		"es7-curl": {
			v:          V7,
			input:      &Logger{Type: LOGGER_TYPE_CURL},
			outputType: reflect.TypeOf(&es7_logger.CurlLogger{}),
		},
	}

	for _, t := range tests {
		result, err := t.input.GetESLogger(t.v)
		assert.NoError(err)
		assert.Equal(t.outputType, reflect.TypeOf(result))
		switch t.v {
		case V5:
			assert.Equal(t.input.EnableRequestBody, result.(es5_logger.Logger).RequestBodyEnabled())
			assert.Equal(t.input.EnableResponseBody, result.(es5_logger.Logger).ResponseBodyEnabled())
		case V6:
			assert.Equal(t.input.EnableRequestBody, result.(es6_logger.Logger).RequestBodyEnabled())
			assert.Equal(t.input.EnableResponseBody, result.(es6_logger.Logger).ResponseBodyEnabled())
		case V7:
			assert.Equal(t.input.EnableRequestBody, result.(es6_logger.Logger).RequestBodyEnabled())
			assert.Equal(t.input.EnableResponseBody, result.(es6_logger.Logger).ResponseBodyEnabled())
		}
	}

}
