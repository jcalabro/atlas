package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/pkg/atlas"
)

func (s *server) GetActor(ctx context.Context, req *connect.Request[atlas.GetActorRequest]) (*connect.Response[atlas.GetActorResponse], error) {
	actor, err := s.store.GetActor(req.Msg.Did)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if actor == nil {
		return connect.NewResponse(&atlas.GetActorResponse{}), nil
	}

	return connect.NewResponse(&atlas.GetActorResponse{
		Actor: actor,
	}), nil
}
