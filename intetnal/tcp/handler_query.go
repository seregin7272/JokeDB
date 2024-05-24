package tcp

import (
	"context"
	"errors"
	"io"
	"net"
)

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
