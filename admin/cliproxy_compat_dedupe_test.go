package admin

import (
	"context"
	"strings"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

func TestImportCompatPayloadRejectsDuplicateAccessToken(t *testing.T) {
	db := newSQLiteDBForAdminTest(t)
	store := auth.NewStore(db, cache.NewMemory(1), &database.SystemSettings{})
	h := &Handler{db: db, store: store}

	payload := []byte(`{"email":"dup@example.com","access_token":"dup-at-token-001"}`)

	uploaded, err := h.importCompatPayload(context.Background(), "dup.json", payload, nil)
	if err != nil {
		t.Fatalf("first import failed: %v", err)
	}
	if len(uploaded) != 1 {
		t.Fatalf("first import uploaded=%d, want=1", len(uploaded))
	}

	uploaded, err = h.importCompatPayload(context.Background(), "dup-again.json", payload, nil)
	if err == nil {
		t.Fatalf("second import should fail for duplicate token")
	}
	if !strings.Contains(err.Error(), "重复 token") {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
	if len(uploaded) != 0 {
		t.Fatalf("duplicate import uploaded=%d, want=0", len(uploaded))
	}

	rows, err := db.ListAll(context.Background())
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	count := 0
	for _, row := range rows {
		if row == nil {
			continue
		}
		if row.GetCredential("access_token") == "dup-at-token-001" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate token rows=%d, want=1", count)
	}
}
