// Package pgmock provides the ability to mock a PostgreSQL client.
package frontend

import (
	"fmt"
	"io"
	"reflect"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Step interface {
	Step(*pgproto3.Frontend) error
}

type Script struct {
	Steps []Step
}

func (s *Script) Run(frontend *pgproto3.Frontend) error {
	for _, step := range s.Steps {
		err := step.Step(frontend)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Script) Step(frontend *pgproto3.Frontend) error {
	return s.Run(frontend)
}

type expectMessageStep struct {
	want pgproto3.BackendMessage
	any  bool
}

func (e *expectMessageStep) Step(frontend *pgproto3.Frontend) error {
	msg, err := frontend.Receive()
	if err != nil {
		return err
	}

	if e.any && reflect.TypeOf(msg) == reflect.TypeOf(e.want) {
		return nil
	}

	if !reflect.DeepEqual(msg, e.want) {
		return fmt.Errorf("msg => %#v, e.want => %#v", msg, e.want)
	}

	return nil
}

func ExpectMessage(want pgproto3.BackendMessage) Step {
	return expectMessage(want, false)
}

func ExpectAnyMessage(want pgproto3.BackendMessage) Step {
	return expectMessage(want, true)
}

func expectMessage(want pgproto3.BackendMessage, any bool) Step {
	return &expectMessageStep{want: want, any: any}
}

type sendMessageStep struct {
	msg pgproto3.FrontendMessage
}

func (e *sendMessageStep) Step(frontend *pgproto3.Frontend) error {
	frontend.Send(e.msg)
	return frontend.Flush()
}

func SendMessage(msg pgproto3.FrontendMessage) Step {
	return &sendMessageStep{msg: msg}
}

type waitForCloseCompleteStep struct{}

func (e *waitForCloseCompleteStep) Step(frontend *pgproto3.Frontend) error {
	for {
		msg, err := frontend.Receive()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if _, ok := msg.(*pgproto3.CloseComplete); ok {
			return nil
		}
	}
}

func WaitForClose() Step {
	return &waitForCloseCompleteStep{}
}

func AcceptAuthenticatedConnRequestSteps() []Step {
	return []Step{
		ExpectMessage(&pgproto3.AuthenticationOk{}),
		ExpectMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'}),
	}
}
