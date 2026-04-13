package database

import (
	"context"
	"path/filepath"
	"testing"
)

func newSQLiteDBForTokenTest(t *testing.T) *DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "codex2api-token.db")
	db, err := New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("create sqlite db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestGetAllTokensOnlyActiveAccounts(t *testing.T) {
	ctx := context.Background()
	db := newSQLiteDBForTokenTest(t)

	_, err := db.InsertAccount(ctx, "active-refresh", "rt-active", "")
	if err != nil {
		t.Fatalf("insert active refresh account: %v", err)
	}

	_, err = db.InsertATAccount(ctx, "active-access", "at-active", "")
	if err != nil {
		t.Fatalf("insert active access account: %v", err)
	}

	errorRefreshID, err := db.InsertAccount(ctx, "error-refresh", "rt-error", "")
	if err != nil {
		t.Fatalf("insert error refresh account: %v", err)
	}
	if err := db.SetError(ctx, errorRefreshID, "refresh expired"); err != nil {
		t.Fatalf("set error for refresh account: %v", err)
	}

	errorAccessID, err := db.InsertATAccount(ctx, "error-access", "at-error", "")
	if err != nil {
		t.Fatalf("insert error access account: %v", err)
	}
	if err := db.SetError(ctx, errorAccessID, "access revoked"); err != nil {
		t.Fatalf("set error for access account: %v", err)
	}

	deletedRefreshID, err := db.InsertAccount(ctx, "deleted-refresh", "rt-deleted", "")
	if err != nil {
		t.Fatalf("insert deleted refresh account: %v", err)
	}
	if _, err := db.conn.ExecContext(ctx, "UPDATE accounts SET status = 'deleted', error_message = 'deleted', updated_at = CURRENT_TIMESTAMP WHERE id = ?", deletedRefreshID); err != nil {
		t.Fatalf("mark refresh account deleted: %v", err)
	}

	deletedAccessID, err := db.InsertATAccount(ctx, "deleted-access", "at-deleted", "")
	if err != nil {
		t.Fatalf("insert deleted access account: %v", err)
	}
	if _, err := db.conn.ExecContext(ctx, "UPDATE accounts SET status = 'deleted', error_message = 'deleted', updated_at = CURRENT_TIMESTAMP WHERE id = ?", deletedAccessID); err != nil {
		t.Fatalf("mark access account deleted: %v", err)
	}

	refreshTokens, err := db.GetAllRefreshTokens(ctx)
	if err != nil {
		t.Fatalf("get refresh tokens: %v", err)
	}
	if len(refreshTokens) != 1 || !refreshTokens["rt-active"] {
		t.Fatalf("expected only rt-active in refresh tokens, got %v", refreshTokens)
	}
	if _, ok := refreshTokens["rt-error"]; ok {
		t.Fatalf("error refresh token should be skipped")
	}
	if _, ok := refreshTokens["rt-deleted"]; ok {
		t.Fatalf("deleted refresh token should be skipped")
	}

	accessTokens, err := db.GetAllAccessTokens(ctx)
	if err != nil {
		t.Fatalf("get access tokens: %v", err)
	}
	if len(accessTokens) != 1 || !accessTokens["at-active"] {
		t.Fatalf("expected only at-active in access tokens, got %v", accessTokens)
	}
	if _, ok := accessTokens["at-error"]; ok {
		t.Fatalf("error access token should be skipped")
	}
	if _, ok := accessTokens["at-deleted"]; ok {
		t.Fatalf("deleted access token should be skipped")
	}
}
