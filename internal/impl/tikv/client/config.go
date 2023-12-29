package client

// Operation represents the operation that will be performed by Couchbase.
type Operation string

const (
	// OperationGet Get operation.
	OperationGet Operation = "get"
	// OperationDelete Delete operation.
	OperationDelete Operation = "delete"
	// OperationPut Upsert operation.
	OperationPut Operation = "put"
)
