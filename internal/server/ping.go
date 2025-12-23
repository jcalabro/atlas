package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/pkg/atlas"
)

func (s *server) Ping(ctx context.Context, req *connect.Request[atlas.PingRequest]) (*connect.Response[atlas.PingResponse], error) {
	return connect.NewResponse(&atlas.PingResponse{}), nil
}
