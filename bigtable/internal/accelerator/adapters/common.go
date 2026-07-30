package adapters

// ResourceKind identifies which kind of Bigtable resource a request targets.
// The adapter knows this from which V2 name field the caller populated, so
// downstream routing switches on the kind instead of re-parsing the name.
type ResourceKind int

const (
	// ResourceTable is a standard table ("projects/P/instances/I/tables/T").
	ResourceTable ResourceKind = iota
	// ResourceAuthorizedView is an authorized view
	// ("projects/P/instances/I/tables/T/authorizedViews/V").
	ResourceAuthorizedView
	// ResourceMaterializedView is a materialized view
	// ("projects/P/instances/I/materializedViews/V"). Read-only.
	ResourceMaterializedView
)

// Resource is the fully-qualified resource a request targets, tagged with its
// kind so callers route without inspecting the name string.
type Resource struct {
	Kind ResourceKind
	Name string
}

// Adapter defines a generic interface for adapting one type to another.
type Adapter[From any, To any] interface {
	Adapt(from From) (To, error)
}

// RequestAdapter represents a specialized adapter for request routing.
type RequestAdapter[From any, To any] interface {
	Adapter[From, To]
	ExtractResource(from From) (Resource, error)
}

// Default request and response adapter singletons.
var (
	DefaultReadRowRequestAdapter    = &ReadRowRequestAdapter{}
	DefaultReadRowResponseAdapter   = &ReadRowResponseAdapter{}
	DefaultMutateRowRequestAdapter  = &MutateRowRequestAdapter{}
	DefaultMutateRowResponseAdapter = &MutateRowResponseAdapter{}
)
