package tcp

import (
	"context"
	"errors"
	"jokedb/intetnal/semaphore"
	"net"
)

const bufferSize = 1024

type Logger interface {
	Error(args ...interface{})
}

type Limiter interface {
	Acquire()
	Release()
}

type Server struct {
	listener *net.TCPListener
	logger   Logger
	limiter  Limiter
	handler  func(ctx context.Context, s string) string
}

func NewServer(addr string, maxConnections uint, logger Logger, handler HandelQuery) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	return &Server{
		logger:   logger,
		handler:  handler,
		limiter:  semaphore.New(maxConnections),
		listener: listener,
	}, nil
}

func (s Server) Listen(ctx context.Context) {
	defer s.listener.Close()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			s.logger.Error(err)
			continue
		}
		h := HandlerConn{
			conn:   conn,
			buffer: make([]byte, bufferSize),
		}

		go func() {
			s.limiter.Acquire()
			defer s.limiter.Release()
			h.Handel(ctx, s.handler)
		}()
	}
}
