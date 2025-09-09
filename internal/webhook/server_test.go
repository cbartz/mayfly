package webhook

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/canonical/mayfly/internal/queue"
)

const webhookPath = "/webhook"
const payload = `{"message":"Hello, Alice!"}`
const secret = "fake-secret"
const valid_signature_header = "0aca2d7154cddad4f56f246cad61f1485df34b8056e10c4e4799494376fb3413" // HMAC SHA256 of body with secret "fake-secret"

type FakeQueue struct {
	Messages [][]byte
}

func (q *FakeQueue) Push(msg []byte) error {
	q.Messages = append(q.Messages, msg)
	return nil
}

type ErrorQueue struct{}

func (q *ErrorQueue) Push(msg []byte) error {
	return fmt.Errorf("queue error")
}

func TestWebhookForwarded(t *testing.T) {
	body := `{"message":"Hello, Alice!"}`
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(WebhookSignatureHeader, valid_signature_header)
	w := httptest.NewRecorder()
	fakeQueue := FakeQueue{}
	var msgQueue queue.Queue = &fakeQueue
	initQueue(msgQueue)
	initWebhookSecret(secret)
	webhookHandler(w, req)
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 got %v", res.Status)
		return
	}

	if fakeQueue.Messages == nil || len(fakeQueue.Messages) != 1 {
		t.Errorf("expected 1 message in queue, got %d", len(fakeQueue.Messages))
		return // avoid out of range panic
	}
	if string(fakeQueue.Messages[0]) != body {
		t.Errorf("expected message body %s, got %s", body, string(fakeQueue.Messages[0]))
	}
}

func TestWebhookQueueError(t *testing.T) {
	body := `{"message":"Hello, Alice!"}`
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(WebhookSignatureHeader, valid_signature_header)

	w := httptest.NewRecorder()

	var msgQueue queue.Queue = &ErrorQueue{}
	initQueue(msgQueue)
	initWebhookSecret(secret)

	webhookHandler(w, req)
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500 got %v", res.Status)
	}
}

func TestWebhookMissingSignatureHeader(t *testing.T) {
	body := `{"message":"Hello, Alice!"}`
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	fakeQueue := FakeQueue{}
	var msgQueue queue.Queue = &fakeQueue
	initQueue(msgQueue)
	initWebhookSecret(secret)
	webhookHandler(w, req)
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("expected status 403 got %v", res.Status)
		return
	}
}

func TestWebhookInvalidSignature(t *testing.T) {
	body := `{"message":"Hello, Alice!"}`
	secret := "fake-secret"
	invalid_signature_header := "0aca2d7154cinvalid56f246cad61f1485df34b8056e10c4e4799494376fb3412"
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(WebhookSignatureHeader, invalid_signature_header)
	w := httptest.NewRecorder()

	fakeQueue := FakeQueue{}
	var msgQueue queue.Queue = &fakeQueue
	initQueue(msgQueue)
	initWebhookSecret(secret)
	webhookHandler(w, req)
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("expected status 403 got %v", res.Status)
		return
	}
	respBody, _ := io.ReadAll(res.Body)
	if string(respBody) != "Invalid signature\n" {
		t.Errorf("expected body 'Invalid signature' got %s", string(respBody))
	}
}
