package esworker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStandardAction_GetOperation(t *testing.T) {
	assert := assert.New(t)
	da := &StandardAction{Op: ES_UPDATE}
	assert.Equal(ES_UPDATE, da.GetOperation())
}

func TestStandardAction_GetIndex(t *testing.T) {
	assert := assert.New(t)
	da := &StandardAction{Index: "index"}
	assert.Equal("index", da.GetIndex())
}

func TestStandardAction_GetDocType(t *testing.T) {
	assert := assert.New(t)
	da := &StandardAction{DocType: "_doc"}
	assert.Equal("_doc", da.GetDocType())
}

func TestStandardAction_GetID(t *testing.T) {
	assert := assert.New(t)
	da := &StandardAction{Id: "id"}
	assert.Equal("id", da.GetID())
}

func TestStandardAction_GetDoc(t *testing.T) {
	assert := assert.New(t)
	da := &StandardAction{Doc: map[string]interface{}{"allan": "hi"}}
	assert.Equal("hi", da.GetDoc()["allan"].(string))
}
