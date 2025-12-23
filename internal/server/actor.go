package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/pkg/atlas"
)

func (s *server) GetActors(ctx context.Context, req *connect.Request[atlas.GetActorsRequest]) (*connect.Response[atlas.GetActorsResponse], error) {
	actors, err := s.store.GetActors(req.Msg.Dids)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&atlas.GetActorsResponse{
		Actors: actors,
	}), nil
}
