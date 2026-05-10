package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestNewSQLiteInitializesFreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "codex2api.db")

	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) 返回错误: %v", err)
	}
	defer db.Close()

	if got := db.Driver(); got != "sqlite" {
		t.Fatalf("Driver() = %q, want %q", got, "sqlite")
	}
}

func TestSQLiteFreshAccountDefaultsToNotSold(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "codex2api.db")

	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) 返回错误: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "fresh-account", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}

	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountByID 返回错误: %v", err)
	}
	if row == nil {
		t.Fatalf("GetAccountByID(%d) 返回 nil", accountID)
	}
	if row.IsSold {
		t.Fatal("新建账号默认 is_sold = true，want false")
	}
}

func TestUpdateAccountSoldSyncsRegisteredAccountByEmail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "codex2api.db")

	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) 返回错误: %v", err)
	}
	defer db.Close()

	sidecarPath := filepath.Join(t.TempDir(), "webui.db")
	sidecar, err := sql.Open("sqlite", sidecarPath)
	if err != nil {
		t.Fatalf("open sidecar sqlite 返回错误: %v", err)
	}
	defer sidecar.Close()
	if _, err := sidecar.Exec(`CREATE TABLE registered_accounts (email TEXT NOT NULL COLLATE NOCASE, is_sold TEXT)`); err != nil {
		t.Fatalf("create registered_accounts 返回错误: %v", err)
	}
	if _, err := sidecar.Exec(`INSERT INTO registered_accounts (email, is_sold) VALUES (?, '')`, "buyer@example.com"); err != nil {
		t.Fatalf("insert registered account 返回错误: %v", err)
	}

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "buyer@example.com", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	if err := db.UpdateCredentials(ctx, accountID, map[string]interface{}{"email": "buyer@example.com"}); err != nil {
		t.Fatalf("UpdateCredentials 返回错误: %v", err)
	}
	db.SetRegisteredAccountsDBPath(sidecarPath)

	if err := db.UpdateAccountSold(ctx, accountID, true); err != nil {
		t.Fatalf("UpdateAccountSold 返回错误: %v", err)
	}

	var got string
	if err := sidecar.QueryRow(`SELECT is_sold FROM registered_accounts WHERE email = ?`, "buyer@example.com").Scan(&got); err != nil {
		t.Fatalf("query registered account 返回错误: %v", err)
	}
	if got != "true" {
		t.Fatalf("registered_accounts.is_sold = %q, want %q", got, "true")
	}
}

func TestUpdateAccountSoldPrefersRegisteredAccountsSyncURL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "codex2api.db")

	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) 返回错误: %v", err)
	}
	defer db.Close()

	var gotEmail string
	var gotSold bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Email  string `json:"email"`
			IsSold bool   `json:"is_sold"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request 返回错误: %v", err)
		}
		gotEmail = payload.Email
		gotSold = payload.IsSold
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "sync-url@example.com", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	db.SetRegisteredAccountsDBPath(filepath.Join(t.TempDir(), "missing-webui.db"))
	db.SetRegisteredAccountsSyncURL(server.URL, "")

	if err := db.UpdateAccountSold(ctx, accountID, true); err != nil {
		t.Fatalf("UpdateAccountSold 返回错误: %v", err)
	}
	if gotEmail != "sync-url@example.com" {
		t.Fatalf("sync email = %q, want %q", gotEmail, "sync-url@example.com")
	}
	if !gotSold {
		t.Fatal("sync is_sold = false, want true")
	}
}
