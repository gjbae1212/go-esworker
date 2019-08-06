package esworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	es5 "github.com/elastic/go-elasticsearch/v5"
	es6 "github.com/elastic/go-elasticsearch/v6"
	es7 "github.com/elastic/go-elasticsearch/v7"

	es5_logger "github.com/elastic/go-elasticsearch/v5/estransport"
	es6_logger "github.com/elastic/go-elasticsearch/v6/estransport"
	es7_logger "github.com/elastic/go-elasticsearch/v7/estransport"
)

// ESOperation is a type of elasticsearch.
type ESOperation int

// ESOperation supports to index, create, update, delete.
const (
	ES_INDEX ESOperation = iota
	ES_CREATE
	ES_UPDATE
	ES_DELETE
)

const (
	defaultESDocType   = "_doc"
	defaultESV5DocType = "doc"

	metaFormatA = `{"%s": {"_index": "%s"}}%s`
	metaFormatB = `{"%s": {"_index": "%s", "_type": "%s"}}%s`
	metaFormatC = `{"%s": {"_index": "%s", "_id": "%s"}}%s`
	metaFormatD = `{"%s": {"_index": "%s", "_type": "%s", "_id": "%s"}}%s`
)

// GetString converts int to string value.
func (ao ESOperation) GetString() string {
	switch ao {
	case ES_INDEX:
		return "index"
	case ES_CREATE:
		return "create"
	case ES_UPDATE:
		return "update"
	case ES_DELETE:
		return "delete"
	default:
		return ""
	}
}

// it is response structs of elasticserach.
type (
	ESResponseCause struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	}

	ESResponseError struct {
		Type   string          `json:"type"`
		Reason string          `json:"reason"`
		Cause  ESResponseCause `json:"caused_by"`
	}

	ESResponseStatus struct {
		Id     string          `json:"_id"`
		Result string          `json:"result"`
		Status int             `json:"status"`
		Error  ESResponseError `json:"error"`
	}

	ESResponseItem struct {
		Index  ESResponseStatus `json:"index"`
		Create ESResponseStatus `json:"create"`
		Update ESResponseStatus `json:"update"`
		Delete ESResponseStatus `json:"delete"`
	}

	ESResponseBulk struct {
		Errors bool             `json:"errors"`
		Items  []ESResponseItem `json:"items"`
	}
)

// Count returns a success and fail count.
func (bulk *ESResponseBulk) Count() (success int, fail int) {
	for _, item := range bulk.Items {
		switch {
		case item.Index.Status != 0:
			if item.Index.Status > 201 {
				fail++
			} else {
				success++
			}
		case item.Create.Status != 0:
			if item.Create.Status > 201 {
				fail++
			} else {
				success++
			}
		case item.Update.Status != 0:
			if item.Update.Status > 201 {
				fail++
			} else {
				success++
			}
		case item.Delete.Status != 0:
			if item.Delete.Status > 201 {
				fail++
			} else {
				success++
			}
		}
	}
	return
}

// ESProxy is an interface that actually request the elasticserach.
type ESProxy interface {
	Bulk(acts []Action) (bulk *ESResponseBulk, err error)
}

type esproxy struct {
	sync.RWMutex
	version   ESVersion
	es5Config es5.Config
	es6Config es6.Config
	es7Config es7.Config
	es5Client *es5.Client
	es6Client *es6.Client
	es7Client *es7.Client
	bufPool   *sync.Pool
}

// Bulk is to request a bulk action to the elasticsearch.
func (ep *esproxy) Bulk(acts []Action) (bulk *ESResponseBulk, err error) {
	result := &ESResponseBulk{}
	if len(acts) == 0 {
		return
	}

	buf, suberr := ep.makeReader(acts)
	if suberr != nil {
		err = suberr
		return
	}

	// set request timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// response body
	var body io.ReadCloser
	statusErr := false

	// execute a bulk operation depending on ES version.
	switch ep.version {
	case V5: // elasticsearch v5 could possibly have multiple doc_type in an index. (default: doc)
		client, suberr := ep.getES5()
		if suberr != nil {
			err = suberr
			return
		}
		resp, suberr := client.Bulk(
			bytes.NewReader(buf),
			client.Bulk.WithContext(ctx),
			client.Bulk.WithDocumentType(defaultESV5DocType),
		)
		if suberr != nil {
			err = suberr
			return
		}
		if resp.IsError() {
			statusErr = true
		}
		body = resp.Body
	case V6: // elasticsearch v6 must have only one the doc_type in an index. (default: _doc)
		client, suberr := ep.getES6()
		if suberr != nil {
			err = suberr
			return
		}
		resp, suberr := client.Bulk(
			bytes.NewReader(buf),
			client.Bulk.WithContext(ctx),
			client.Bulk.WithDocumentType(defaultESDocType))
		if suberr != nil {
			err = suberr
			return
		}
		if resp.IsError() {
			statusErr = true
		}
		body = resp.Body
	case V7: // elasticsearch v7 must have only one the doc_type in an index. (default: _doc)
		client, suberr := ep.getES7()
		if suberr != nil {
			err = suberr
			return
		}
		resp, suberr := client.Bulk(
			bytes.NewReader(buf),
			client.Bulk.WithContext(ctx),
			client.Bulk.WithDocumentType(defaultESDocType))
		if suberr != nil {
			err = suberr
			return
		}
		if resp.IsError() {
			statusErr = true
		}
		body = resp.Body
	default:
		err = fmt.Errorf("[err] Bulk (invalid version)")
		return
	}

	// status on response is less than 200 or more than 299.
	if statusErr {
		cause := make(map[string]interface{})
		if suberr := json.NewDecoder(body).Decode(&cause); suberr != nil {
			err = suberr
			return
		}
		err = fmt.Errorf("[err] Bulk %+v\n", cause)
		return
	}

	// parse response body
	if suberr := json.NewDecoder(body).Decode(result); suberr != nil {
		err = suberr
		return
	}

	bulk = result
	return
}

// makeReader makes reader on bytes package.
func (ep *esproxy) makeReader(acts []Action) ([]byte, error) {
	if len(acts) == 0 {
		return nil, fmt.Errorf("[err] makeReader (empty params)")
	}

	// get buffer from sync.pool
	buf := ep.bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		ep.bufPool.Put(buf)
	}()

	for _, act := range acts {
		// buffer
		var meta []byte
		if act.GetDocType() != "" && act.GetID() != "" {
			meta = []byte(fmt.Sprintf(metaFormatD,
				act.GetOperation().GetString(),
				act.GetIndex(),
				act.GetDocType(),
				act.GetID(),
				"\n",
			))
		} else if act.GetDocType() != "" {
			meta = []byte(fmt.Sprintf(metaFormatB,
				act.GetOperation().GetString(),
				act.GetIndex(),
				act.GetDocType(),
				"\n",
			))
		} else if act.GetID() != "" {
			meta = []byte(fmt.Sprintf(metaFormatC,
				act.GetOperation().GetString(),
				act.GetIndex(),
				act.GetID(),
				"\n",
			))
		} else {
			meta = []byte(fmt.Sprintf(metaFormatA,
				act.GetOperation().GetString(),
				act.GetIndex(),
				"\n",
			))
		}

		if len(act.GetDoc()) == 0 {
			buf.Write(meta)
		} else {
			doc, err := json.Marshal(act.GetDoc())
			if err != nil {
				return nil, err
			}
			doc = append(doc, "\n"...)

			buf.Grow(len(meta) + len(doc))
			buf.Write(meta)
			buf.Write(doc)
		}
	}
	return buf.Bytes(), nil
}

// getES5 is to get a client of es5.
func (ep *esproxy) getES5() (*es5.Client, error) {
	if ep.es5Client == nil {
		// lock read and write.
		ep.Lock()
		defer ep.Unlock()

		// check once more
		if ep.es5Client != nil {
			return ep.es5Client, nil
		}
		client, err := es5.NewClient(ep.es5Config)
		if err != nil {
			return nil, err
		}
		ep.es5Client = client
	}
	return ep.es5Client, nil
}

// getES6 is to get a client of es6.
func (ep *esproxy) getES6() (*es6.Client, error) {
	if ep.es6Client == nil {
		// lock read and write.
		ep.Lock()
		defer ep.Unlock()

		// check once more
		if ep.es6Client != nil {
			return ep.es6Client, nil
		}
		client, err := es6.NewClient(ep.es6Config)
		if err != nil {
			return nil, err
		}
		ep.es6Client = client
	}
	return ep.es6Client, nil
}

// getES7 is to get a client of es7.
func (ep *esproxy) getES7() (*es7.Client, error) {
	if ep.es7Client == nil {
		// lock read and write.
		ep.Lock()
		defer ep.Unlock()

		// check once more
		if ep.es7Client != nil {
			return ep.es7Client, nil
		}
		client, err := es7.NewClient(ep.es7Config)
		if err != nil {
			return nil, err
		}
		ep.es7Client = client
	}
	return ep.es7Client, nil
}

// createESProxy is to create ESProxy interface.
func createESProxy(cfg *config) (ESProxy, error) {
	if cfg == nil {
		return nil, fmt.Errorf("[err] createESProxy empty params")
	}

	// es5 config
	es5conf := es5.Config{
		Addresses: cfg.addrs,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: cfg.transport,
	}
	if cfg.logger != nil {
		logger, err := cfg.logger.GetESLogger(V5)
		if err != nil {
			return nil, err
		}
		es5conf.Logger = logger.(es5_logger.Logger)
	}

	// es6 config
	es6conf := es6.Config{
		Addresses: cfg.addrs,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: cfg.transport,
		CloudID:   cfg.cloudId,
		APIKey:    cfg.apiKey,
	}
	if cfg.logger != nil {
		logger, err := cfg.logger.GetESLogger(V6)
		if err != nil {
			return nil, err
		}
		es6conf.Logger = logger.(es6_logger.Logger)
	}

	// es7 config
	es7conf := es7.Config{
		Addresses: cfg.addrs,
		Username:  cfg.username,
		Password:  cfg.password,
		Transport: cfg.transport,
		CloudID:   cfg.cloudId,
		APIKey:    cfg.apiKey,
	}
	if cfg.logger != nil {
		logger, err := cfg.logger.GetESLogger(V7)
		if err != nil {
			return nil, err
		}
		es7conf.Logger = logger.(es7_logger.Logger)
	}

	return &esproxy{
		version:   cfg.version,
		es5Config: es5conf,
		es6Config: es6conf,
		es7Config: es7conf,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}, nil
}
