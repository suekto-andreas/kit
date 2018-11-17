package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type mockWriter struct {
	writeError error
	writeMsgs  []kafka.Message
}

func (mw *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	for _, msg := range msgs {
		mw.writeMsgs = append(mw.writeMsgs, msg)
	}

	return mw.writeError
}

func (mw *mockWriter) SetWriteError(err error) *mockWriter {
	mw.writeError = err
	return mw
}

type testReq struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

func testReqEncoder(ctx context.Context, msg *kafka.Message, request interface{}) error {
	req, ok := request.(testReq)
	if !ok {
		return fmt.Errorf("invalid request")
	}

	value, err := json.Marshal(req)
	if err != nil {
		return err
	}

	msg.Key = []byte(req.ID)
	msg.Value = value
	msg.Time = time.Now()
	return nil
}

func TestSuccessfulPublish(t *testing.T) {
	mw := &mockWriter{}
	mw.SetWriteError(nil)

	publisher := NewPublisher(
		mw,
		testReqEncoder,
		PublisherBefore(
			func(ctx context.Context, msg *kafka.Message) context.Context {
				msg.Value = []byte(strings.ToUpper(string(msg.Value)))
				return ctx
			}),
	)

	req := testReq{
		ID:      "1",
		Content: "okie-dokie",
	}
	_, err := publisher.Endpoint()(context.Background(), req)
	if err != nil {
		t.Fatalf("there should be no error, instead got: \n%+v\n", err)
	}
	if string(mw.writeMsgs[0].Key) != req.ID {
		t.Fatalf("Write Message Key is wrong, \nexpect \n%+v\nactual \n%+v\n",
			req.ID, string(mw.writeMsgs[0].Key))
	}
	writtenValue := &testReq{}
	err = json.Unmarshal(mw.writeMsgs[0].Value, writtenValue)
	if err != nil {
		t.Fatalf("written message is error: %+v", err)
	}

	if writtenValue.Content != strings.ToUpper(req.Content) {
		t.Fatalf("content is not as expected.\nExpect:%+v\nActual:%+v",
			strings.ToUpper(req.Content), writtenValue.Content)
	}
}
