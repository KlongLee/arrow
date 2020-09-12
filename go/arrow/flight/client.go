package flight

import (
	context "context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Client is an interface wrapped around the generated FlightServiceClient which is
// generated by grpc protobuf definitions. This interface provides a useful hiding
// of the authentication handshake via calling Authenticate and using the
// ClientAuthHandler rather than manually having to implement the grpc communication
// and sending of the auth token.
type Client interface {
	// Authenticate uses the ClientAuthHandler that was used when creating the client
	// in order to use the Handshake endpoints of the service.
	Authenticate(context.Context, ...grpc.CallOption) error
	Close() error
	// join the interface from the FlightServiceClient instead of re-defining all
	// the endpoints here.
	FlightServiceClient
}

type client struct {
	conn        *grpc.ClientConn
	authHandler ClientAuthHandler

	FlightServiceClient
}

// NewFlightClient takes in the address of the grpc server and an auth handler for the
// application-level handshake. If using TLS or other grpc configurations they can still
// be passed via the grpc.DialOption list just as if connecting manually without this
// helper function.
//
// Alternatively, a grpc client can be constructed as normal without this helper as the
// grpc generated client code is still exported. This exists to add utility and helpers
// around the authentication and passing the token with requests.
func NewFlightClient(addr string, auth ClientAuthHandler, opts ...grpc.DialOption) (Client, error) {
	if auth != nil {
		opts = append([]grpc.DialOption{
			grpc.WithChainStreamInterceptor(createClientAuthStreamInterceptor(auth)),
			grpc.WithChainUnaryInterceptor(createClientAuthUnaryInterceptor(auth)),
		}, opts...)
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &client{conn: conn, FlightServiceClient: NewFlightServiceClient(conn), authHandler: auth}, nil
}

func (c *client) Authenticate(ctx context.Context, opts ...grpc.CallOption) error {
	if c.authHandler == nil {
		return status.Error(codes.NotFound, "cannot authenticate without an auth-handler")
	}

	stream, err := c.FlightServiceClient.Handshake(ctx, opts...)
	if err != nil {
		return err
	}

	return c.authHandler.Authenticate(&clientAuthConn{stream})
}

func (c *client) Close() error {
	c.FlightServiceClient = nil
	return c.conn.Close()
}
