package tcp

import "net"

type Client struct {
	conn   net.Conn
	logger Logger
}

func NewClient(addr string, logger Logger) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Client{
		conn:   conn,
		logger: logger,
	}, nil
}

func (c *Client) Send(msg []byte) ([]byte, error) {
	buffer := make([]byte, bufferSize)

	if _, err := c.conn.Write(msg); err != nil {
		c.logger.Error(err)
		return nil, err
	}
	n, err := c.conn.Read(buffer)
	if err != nil {
		c.logger.Error(err)
		return nil, err
	}

	return buffer[:n], nil
}

func (c *Client) Close() {
	if err := c.conn.Close(); err != nil {
		c.logger.Error(err)
	}
}
