package esworker

import (
	"fmt"
	"io"

	es5_logger "github.com/elastic/go-elasticsearch/v5/estransport"
	es6_logger "github.com/elastic/go-elasticsearch/v6/estransport"
	es7_logger "github.com/elastic/go-elasticsearch/v7/estransport"
)

type LoggerType int

const (
	LOGGER_TYPE_TEXT LoggerType = iota
	LOGGER_TYPE_COLOR
	LOGGER_TYPE_CURL
	LOGGER_TYPE_JSON
)

// Logger is an intermediate struct to be changed elastic logger.
type Logger struct {
	Type               LoggerType
	Output             io.Writer
	EnableRequestBody  bool
	EnableResponseBody bool
}

// GetESLogger is to return elastic search logger.
func (logger *Logger) GetESLogger(v ESVersion) (interface{}, error) {
	switch v {
	case V5:
		switch logger.Type {
		case LOGGER_TYPE_TEXT:
			return &es5_logger.TextLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_COLOR:
			return &es5_logger.ColorLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_CURL:
			return &es5_logger.CurlLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_JSON:
			return &es5_logger.JSONLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		}
	case V6:
		switch logger.Type {
		case LOGGER_TYPE_TEXT:
			return &es6_logger.TextLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_COLOR:
			return &es6_logger.ColorLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_CURL:
			return &es6_logger.CurlLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_JSON:
			return &es6_logger.JSONLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		}
	case V7:
		switch logger.Type {
		case LOGGER_TYPE_TEXT:
			return &es7_logger.TextLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_COLOR:
			return &es7_logger.ColorLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_CURL:
			return &es7_logger.CurlLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		case LOGGER_TYPE_JSON:
			return &es7_logger.JSONLogger{
				Output:             logger.Output,
				EnableRequestBody:  logger.EnableRequestBody,
				EnableResponseBody: logger.EnableResponseBody,
			}, nil
		}
	}
	return nil, fmt.Errorf("[err] not support es version %s", v.GetString())
}
