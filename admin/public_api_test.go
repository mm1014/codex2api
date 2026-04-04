package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestPublicGenerateRateLimitByIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newSQLiteDBForAdminTest(t)
	h := &Handler{db: db}

	const ip = "10.0.0.1"
	for i := 0; i < 3; i++ {
		_, err := db.InsertPublicAPIKeyWithMeta(context.Background(), "k", generatePublicKey(), "public_generate", ip, 0)
		if err != nil {
			t.Fatalf("insert key %d: %v", i, err)
		}
	}

	r := gin.New()
	h.RegisterPublicRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/public/generate", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", ip)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want=%d, body=%s", rec.Code, http.StatusTooManyRequests, rec.Body.String())
	}
}

func TestPublicRedeemSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := newSQLiteDBForAdminTest(t)
	h := &Handler{db: db}

	key := "pk-redeem-route-test-1234567890"
	keyID, err := db.InsertPublicAPIKeyWithMeta(context.Background(), "k", key, "public_generate", "10.0.0.2", 1.0)
	if err != nil {
		t.Fatalf("insert key: %v", err)
	}
	if _, _, err = db.ImportRedeemCodes(context.Background(), 0.5, []string{"R-500"}); err != nil {
		t.Fatalf("import redeem codes: %v", err)
	}

	r := gin.New()
	h.RegisterPublicRoutes(r)

	body, _ := json.Marshal(map[string]float64{"amount_usd": 0.8})
	req := httptest.NewRequest(http.MethodPost, "/public/redeem", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload struct {
		PublicAPIKeyID   int64   `json:"public_api_key_id"`
		Code             string  `json:"code"`
		RedeemedAmount   float64 `json:"redeemed_amount_usd"`
		RemainingBalance float64 `json:"remaining_balance_usd"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload.PublicAPIKeyID != keyID {
		t.Fatalf("public_api_key_id=%d, want=%d", payload.PublicAPIKeyID, keyID)
	}
	if payload.Code != "R-500" {
		t.Fatalf("code=%q, want=%q", payload.Code, "R-500")
	}
	if payload.RedeemedAmount != 0.5 {
		t.Fatalf("redeemed_amount=%.4f, want=0.5", payload.RedeemedAmount)
	}
	if payload.RemainingBalance != 0.5 {
		t.Fatalf("remaining_balance=%.4f, want=0.5", payload.RemainingBalance)
	}
}
