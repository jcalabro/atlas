package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/pkg/atlas"
)

func (s *server) GetRecords(ctx context.Context, req *connect.Request[atlas.GetRecordsRequest]) (*connect.Response[atlas.GetRecordsResponse], error) {
	uris := make([]at.URI, 0, len(req.Msg.Uris))
	for _, str := range req.Msg.Uris {
		uri, err := at.ParseURI(str)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid at uri: %s", str))
		}
		uris = append(uris, uri)
	}

	records, err := s.store.GetRecords(uris)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&atlas.GetRecordsResponse{
		Records: records,
	}), nil
}
