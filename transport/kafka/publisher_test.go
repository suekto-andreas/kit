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
	writeError  error
	writeMsgs   []kafka.Message
	processTime time.Duration
}

func (mw *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	msgchan := make(chan string)
	go func() {
		for _, msg := range msgs {
			mw.writeMsgs = append(mw.writeMsgs, msg)
			time.Sleep(mw.processTime)
		}
		msgchan <- "done"
	}()

	select {
	case <-msgchan:
		return mw.writeError
	case <-ctx.Done():
		return ctx.Err()
	}
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
	mw := &mockWriter{processTime: 1 * time.Millisecond}
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

func TestErrorPublishEncoding(t *testing.T) {
	mw := &mockWriter{processTime: 1 * time.Millisecond}
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

	// request is in bad structure causing error in encoding with testReqEncoder
	req := struct {
		ID   string `json:"id"`
		Text string `json:"text"`
	}{
		ID:   "1",
		Text: "okie-dokie",
	}
	_, err := publisher.Endpoint()(context.Background(), req)
	if err == nil {
		t.Fatalf("there should be an error.")
	}
}

func TestErrorPublishWriting(t *testing.T) {
	mw := &mockWriter{processTime: 1 * time.Millisecond}
	mw.SetWriteError(fmt.Errorf("error in writing"))

	publisher := NewPublisher(
		mw,
		testReqEncoder,
	)

	req := testReq{
		ID:      "1",
		Content: "okie-dokie",
	}
	_, err := publisher.Endpoint()(context.Background(), req)
	if err == nil {
		t.Fatalf("there should be an error")
	}
	if err.Error() != mw.writeError.Error() {
		t.Fatalf("write error is not as expected.\nExpect: %s\nActual: %s\n",
			mw.writeError.Error(), err.Error())
	}
}

func TestPublisherTimeout(t *testing.T) {
	mw := &mockWriter{}
	mw.SetWriteError(nil)

	ch := make(chan struct{})
	defer close(ch)

	publisher := NewPublisher(
		mw,
		testReqEncoder,
		PublisherBefore(
			func(ctx context.Context, msg *kafka.Message) context.Context {
				msg.Value = []byte(strings.ToUpper(string(msg.Value)))
				time.Sleep(20 * time.Millisecond)
				return ctx
			}),
		PublisherTimeout(10*time.Millisecond),
	)
	fmt.Printf("%+v\n", publisher.timeout)

	req := testReq{
		ID:      "1",
		Content: "okie-dokie",
	}
	_, err := publisher.Endpoint()(context.Background(), req)
	if err != context.DeadlineExceeded {
		t.Errorf("want %s, have %+v", context.DeadlineExceeded, err)
	}
}
