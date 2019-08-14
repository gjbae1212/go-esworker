package esworker

// StandardAction is a struct to implement an interface of Action.
type StandardAction struct {
	Op      ESOperation
	Index   string
	DocType string
	Id      string
	Doc     map[string]interface{}
}

// GetOperation returns an operation to process a document.
func (da *StandardAction) GetOperation() ESOperation {
	return da.Op
}

// GetIndex returns an index that want to insert on Elasticsearch.
func (da *StandardAction) GetIndex() string {
	return da.Index
}

// GetDocType returns a doctype that want to insert on Elasticsearch.
func (da *StandardAction) GetDocType() string {
	return da.DocType
}

// GetID returns an id for document on Elasticsearch.
func (da *StandardAction) GetID() string {
	return da.Id
}

// GetDoc returns value that should insert to a document on Elasticsearch.
func (da *StandardAction) GetDoc() map[string]interface{} {
	return da.Doc
}
