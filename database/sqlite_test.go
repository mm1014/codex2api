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
	var gotRefreshToken string
	var gotSold bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Email        string `json:"email"`
			RefreshToken string `json:"refresh_token"`
			IsSold       bool   `json:"is_sold"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request 返回错误: %v", err)
		}
		gotEmail = payload.Email
		gotRefreshToken = payload.RefreshToken
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
	if gotRefreshToken != "refresh-token" {
		t.Fatalf("sync refresh_token = %q, want %q", gotRefreshToken, "refresh-token")
	}
	if !gotSold {
		t.Fatal("sync is_sold = false, want true")
	}
}

func TestUpdateAccountSoldErrorsWhenSyncURLUpdatesNoRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "codex2api.db")

	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) 返回错误: %v", err)
	}
	defer db.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"email":"account-14","is_sold":true,"updated":0}`))
	}))
	defer server.Close()

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "account-14", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	db.SetRegisteredAccountsSyncURL(server.URL, "")

	if err := db.UpdateAccountSold(ctx, accountID, true); err == nil {
		t.Fatal("UpdateAccountSold 返回 nil，want error")
	}
	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountByID 返回错误: %v", err)
	}
	if row.IsSold {
		t.Fatal("accounts.is_sold = true, want rollback to false")
	}
}

func TestUpdateAccountSoldErrorsWhenSidecarUpdatesNoRows(t *testing.T) {
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

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "account-14", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	db.SetRegisteredAccountsDBPath(sidecarPath)

	if err := db.UpdateAccountSold(ctx, accountID, true); err == nil {
		t.Fatal("UpdateAccountSold 返回 nil，want error")
	}
	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountByID 返回错误: %v", err)
	}
	if row.IsSold {
		t.Fatal("accounts.is_sold = true, want rollback to false")
	}
}

func TestUpdateAccountSoldSyncsRegisteredAccountByRefreshTokenWhenEmailMissing(t *testing.T) {
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
	if _, err := sidecar.Exec(`CREATE TABLE registered_accounts (email TEXT NOT NULL COLLATE NOCASE, refresh_token TEXT, is_sold TEXT)`); err != nil {
		t.Fatalf("create registered_accounts 返回错误: %v", err)
	}
	if _, err := sidecar.Exec(`INSERT INTO registered_accounts (email, refresh_token, is_sold) VALUES (?, ?, ?)`, "buyer@example.com", "rt-banned", "false"); err != nil {
		t.Fatalf("insert registered account 返回错误: %v", err)
	}

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "account-14", "rt-banned", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	db.SetRegisteredAccountsDBPath(sidecarPath)

	if err := db.UpdateAccountSold(ctx, accountID, true); err != nil {
		t.Fatalf("UpdateAccountSold 返回错误: %v", err)
	}

	var got string
	if err := sidecar.QueryRow(`SELECT is_sold FROM registered_accounts WHERE refresh_token = ?`, "rt-banned").Scan(&got); err != nil {
		t.Fatalf("query registered account 返回错误: %v", err)
	}
	if got != "true" {
		t.Fatalf("registered_accounts.is_sold = %q, want %q", got, "true")
	}
}

func TestSyncAccountSoldFromRegisteredAccountUpdatesLatestSidecarState(t *testing.T) {
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
	if _, err := sidecar.Exec(`CREATE TABLE registered_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL COLLATE NOCASE, is_sold TEXT)`); err != nil {
		t.Fatalf("create registered_accounts 返回错误: %v", err)
	}
	if _, err := sidecar.Exec(`INSERT INTO registered_accounts (email, is_sold) VALUES (?, ?), (?, ?)`,
		"buyer@example.com", "false",
		"buyer@example.com", "true",
	); err != nil {
		t.Fatalf("insert registered accounts 返回错误: %v", err)
	}

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "buyer@example.com", "refresh-token", "")
	if err != nil {
		t.Fatalf("InsertAccount 返回错误: %v", err)
	}
	db.SetRegisteredAccountsDBPath(sidecarPath)

	updated, err := db.SyncAccountSoldFromRegisteredAccount(ctx, accountID)
	if err != nil {
		t.Fatalf("SyncAccountSoldFromRegisteredAccount 返回错误: %v", err)
	}
	if !updated {
		t.Fatal("updated = false, want true")
	}

	row, err := db.GetAccountByID(ctx, accountID)
	if err != nil {
		t.Fatalf("GetAccountByID 返回错误: %v", err)
	}
	if row == nil {
		t.Fatalf("GetAccountByID(%d) 返回 nil", accountID)
	}
	if !row.IsSold {
		t.Fatal("accounts.is_sold = false, want true")
	}
}

func TestSyncAccountSoldFromRegisteredAccountParsesCompatibleSoldValues(t *testing.T) {
	cases := []struct {
		name string
		raw  interface{}
		want bool
	}{
		{name: "text true", raw: "true", want: true},
		{name: "text false", raw: "false", want: false},
		{name: "integer one", raw: 1, want: true},
		{name: "integer zero", raw: 0, want: false},
		{name: "empty text", raw: "", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			if _, err := sidecar.Exec(`CREATE TABLE registered_accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL COLLATE NOCASE, is_sold)`); err != nil {
				t.Fatalf("create registered_accounts 返回错误: %v", err)
			}
			if _, err := sidecar.Exec(`INSERT INTO registered_accounts (email, is_sold) VALUES (?, ?)`, "compat@example.com", tc.raw); err != nil {
				t.Fatalf("insert registered account 返回错误: %v", err)
			}

			ctx := context.Background()
			accountID, err := db.InsertAccount(ctx, "compat@example.com", "refresh-token", "")
			if err != nil {
				t.Fatalf("InsertAccount 返回错误: %v", err)
			}
			if !tc.want {
				if err := db.UpdateAccountSold(ctx, accountID, true); err != nil {
					t.Fatalf("UpdateAccountSold 返回错误: %v", err)
				}
			}
			db.SetRegisteredAccountsDBPath(sidecarPath)

			updated, err := db.SyncAccountSoldFromRegisteredAccount(ctx, accountID)
			if err != nil {
				t.Fatalf("SyncAccountSoldFromRegisteredAccount 返回错误: %v", err)
			}
			if !updated {
				t.Fatal("updated = false, want true")
			}

			row, err := db.GetAccountByID(ctx, accountID)
			if err != nil {
				t.Fatalf("GetAccountByID 返回错误: %v", err)
			}
			if row.IsSold != tc.want {
				t.Fatalf("accounts.is_sold = %t, want %t", row.IsSold, tc.want)
			}
		})
	}
}
