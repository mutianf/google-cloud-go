package adapters

import (
	v2pb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MutateRowRequestAdapter adapts V2 MutateRowRequest to SessionMutateRowRequest.
type MutateRowRequestAdapter struct{}

func (a *MutateRowRequestAdapter) Adapt(from *v2pb.MutateRowRequest) (*v2pb.SessionMutateRowRequest, error) {
	if from == nil {
		return nil, nil
	}
	return &v2pb.SessionMutateRowRequest{
		Key:       from.RowKey,
		Mutations: from.Mutations,
	}, nil
}

// ExtractResource returns the resource the request targets, tagged with its
// kind. A MutateRowRequest names either a table or an authorized view
// (materialized views are read-only and have no field here); the authorized
// view is checked first so the correct resource is surfaced regardless of which
// the caller populated.
func (a *MutateRowRequestAdapter) ExtractResource(from *v2pb.MutateRowRequest) (Resource, error) {
	if from == nil {
		return Resource{}, status.Errorf(codes.InvalidArgument, "request is nil")
	}
	switch {
	case from.AuthorizedViewName != "":
		return Resource{Kind: ResourceAuthorizedView, Name: from.AuthorizedViewName}, nil
	case from.TableName != "":
		return Resource{Kind: ResourceTable, Name: from.TableName}, nil
	default:
		return Resource{}, status.Errorf(codes.InvalidArgument, "MutateRowRequest names no table or authorized view")
	}
}

// MutateRowResponseAdapter adapts SessionMutateRowResponse to MutateRowResponse.
type MutateRowResponseAdapter struct{}

func (a *MutateRowResponseAdapter) Adapt(from *v2pb.SessionMutateRowResponse) (*v2pb.MutateRowResponse, error) {
	if from == nil {
		return nil, nil
	}
	// Bare minimum scaffold.
	return &v2pb.MutateRowResponse{}, nil
}
