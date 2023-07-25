package frontend_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/dbhao/pgmock/v5/frontend"
)

func TestScript(t *testing.T) {
	var (
		expMsgR = &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Message:  "some error",
		}

		expMsgS1  = &pgproto3.CancelRequest{}
		expMsgS2 = &pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters: map[string]string{
				"user":     "test_user",
				"database": "test_db",
			},
		}
	)
	script := &frontend.Script{
		Steps: []frontend.Step{
			frontend.SendMessage(expMsgS1),
			frontend.ExpectMessage(expMsgR),
			frontend.SendMessage(expMsgS2),
		},
	}
	script.Steps = append(script.Steps, frontend.AcceptAuthenticatedConnRequestSteps()...)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err)
	defer ln.Close()

	serverErrChan := make(chan error, 1)
	go func() {
		defer close(serverErrChan)

		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			serverErrChan <- err
			return
		}

		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			serverErrChan <- err
			return
		}

		err = script.Run(pgproto3.NewFrontend(conn, conn))
		if err != nil {
			serverErrChan <- err
			return
		}
	}()

	conn, err := ln.Accept()
	require.NoError(t, err)
	defer conn.Close()

	// read
	body := make([]byte, 4)
	_, err = conn.Read(body)
	require.NoError(t, err)
	require.Equal(t, uint32(16), binary.BigEndian.Uint32(body))
	body = make([]byte, 12)
	_, err = conn.Read(body)
	require.NoError(t, err)
	rmsg1 := &pgproto3.CancelRequest{}
	require.NoError(t, rmsg1.Decode(body))
	require.Equal(t, rmsg1, expMsgS1)

	// write
	_, err = conn.Write(expMsgR.Encode(nil))
	require.NoError(t, err)

	// read
	body = make([]byte, 4)
	_, err = conn.Read(body)
	require.NoError(t, err)
	body = make([]byte, len(expMsgS2.Encode(nil)))
	_, err = conn.Read(body)
	require.NoError(t, err)
	rmsg2 := &pgproto3.StartupMessage{}
	require.NoError(t, rmsg2.Decode(body))
	require.Equal(t, expMsgS2.ProtocolVersion, rmsg2.ProtocolVersion)
	require.Equal(t, expMsgS2.Parameters["user"], rmsg2.Parameters["user"])
	require.Equal(t, expMsgS2.Parameters["database"], rmsg2.Parameters["database"])

	// write
	buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err = conn.Write(buf)
	require.NoError(t, err)

	require.NoError(t, <-serverErrChan)
}
