package tcp

import (
	"context"
	"errors"
	"io"
	"net"
)

const bufferSize = 1024

type Logger interface {
	Error(args ...interface{})
}

type HandelQuery func(ctx context.Context, s string) string

type HandlerConn struct {
	conn   net.Conn
	buffer []byte
	logger Logger
}

func (hc *HandlerConn) Handel(ctx context.Context, handler HandelQuery) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			hc.logger.Error(err)
		}
	}(hc.conn)

	for {
		n, err := hc.conn.Read(hc.buffer)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				hc.logger.Error(err)
			}

			return
		}

		_, err = hc.conn.Write([]byte(handler(ctx, string(hc.buffer[:n]))))
		if err != nil {
			hc.logger.Error(err)
			return
		}
	}
}

type Server struct {
	listener       *net.TCPListener
	logger         Logger
	maxConnections uint8
	handler        func(ctx context.Context, s string) string
}

func NewServer(addr string, maxConnections uint8, logger Logger, handler HandelQuery) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	return &Server{
		logger:         logger,
		handler:        handler,
		maxConnections: maxConnections,
		listener:       listener,
	}, nil
}

func (s Server) Listen(ctx context.Context) {
	defer s.listener.Close()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.logger.Error(err)
			continue
		}
		h := HandlerConn{
			conn:   conn,
			buffer: make([]byte, bufferSize),
		}

		go h.Handel(ctx, s.handler)
	}
}
