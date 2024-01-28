package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleProduce(t *testing.T) {
	// Create a new instance of your httpServer
	srv := &httpServer{}
	text := "text record"

	// Create a new request with sample data
	requestBody := ProduceRequest{Record: Record{Value: []byte(text)}}
	reqBodyBytes, _ := json.Marshal(requestBody)
	req, err := http.NewRequest("POST", "/produce", bytes.NewReader(reqBodyBytes))
	if err != nil {
		t.Fatal(err)
	}

	// Create a ResponseRecorder to record the response
	rr := httptest.NewRecorder()

	// Call the handler function directly and pass in the ResponseRecorder and Request
	srv.handleProduce(rr, req)

	// Check the status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Check the response body
	expectedResponseBody := `{"offset":0}`
	if rr.Body.String() != expectedResponseBody {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), expectedResponseBody)
	}
}

func TestHandleConsume(t *testing.T) {
	// Create a new instance of your httpServer
	srv := &httpServer{}

	// Create a new request with sample data
	reqBodyBytes, _ := json.Marshal(ConsumeRequest{Offset: 0})
	req, err := http.NewRequest("POST", "/consume", bytes.NewReader(reqBodyBytes))
	if err != nil {
		t.Fatal(err)
	}

	// Create a ResponseRecorder to record the response
	rr := httptest.NewRecorder()

	// Call the handler function directly and pass in the ResponseRecorder and Request
	srv.handleConsume(rr, req)

	// Check the status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Check the response body
	expectedResponseBody := `{"record":""}`
	if rr.Body.String() != expectedResponseBody {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), expectedResponseBody)
	}
}
