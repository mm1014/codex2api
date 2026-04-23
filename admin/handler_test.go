package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/cache"
	"github.com/codex2api/database"
	"github.com/codex2api/proxy"
	"github.com/gin-gonic/gin"
)

func TestRefreshAccountRejectsInvalidID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &Handler{
		refreshAccount: func(context.Context, int64) error {
			t.Fatal("refresh should not be called for invalid id")
			return nil
		},
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "bad-id"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/admin/accounts/bad-id/refresh", nil)

	handler.RefreshAccount(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload["error"]; got != "无效的账号 ID" {
		t.Fatalf("error = %q, want %q", got, "无效的账号 ID")
	}
}

func TestRefreshAccountRunsSingleRefresh(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var called bool
	var gotID int64
	handler := &Handler{
		refreshAccount: func(_ context.Context, id int64) error {
			called = true
			gotID = id
			return nil
		},
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "42"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/admin/accounts/42/refresh", nil)

	handler.RefreshAccount(ctx)

	if !called {
		t.Fatal("expected refresh to be called")
	}
	if gotID != 42 {
		t.Fatalf("refresh id = %d, want %d", gotID, 42)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload["message"]; got != "账号刷新成功" {
		t.Fatalf("message = %q, want %q", got, "账号刷新成功")
	}
}

func TestRefreshAccountReturnsNotFoundForMissingAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &Handler{
		refreshAccount: func(context.Context, int64) error {
			return errors.New("账号 7 不存在")
		},
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "7"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/admin/accounts/7/refresh", nil)

	handler.RefreshAccount(ctx)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload["error"]; got != "账号 7 不存在" {
		t.Fatalf("error = %q, want %q", got, "账号 7 不存在")
	}
}

func TestRefreshAccountReturnsRefreshFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &Handler{
		refreshAccount: func(context.Context, int64) error {
			return errors.New("upstream unavailable")
		},
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "9"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/admin/accounts/9/refresh", nil)

	handler.RefreshAccount(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload["error"]; got != "刷新失败: upstream unavailable" {
		t.Fatalf("error = %q, want %q", got, "刷新失败: upstream unavailable")
	}
}

func TestUpdateAccountProxyRouteUpdatesPersistedAndInMemoryProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbPath := filepath.Join(t.TempDir(), "codex2api.db")
	db, err := database.New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) returned error: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "sticky-proxy-account", "refresh-token", "http://127.0.0.1:7890")
	if err != nil {
		t.Fatalf("InsertAccount returned error: %v", err)
	}

	store := auth.NewStore(db, cache.NewMemory(8), &database.SystemSettings{})
	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init returned error: %v", err)
	}

	handler := NewHandler(store, db, cache.NewMemory(8), nil, "")
	router := gin.New()
	handler.RegisterRoutes(router)

	body := bytes.NewBufferString(`{"proxy_url":"http://Codex.acc_541:123@127.0.0.1:2260"}`)
	req := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/admin/accounts/%d", accountID), body)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := payload["message"]; got != "账号代理已更新" {
		t.Fatalf("message = %q, want %q", got, "账号代理已更新")
	}

	rows, err := db.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("active rows = %d, want 1", len(rows))
	}
	if got := rows[0].ProxyURL; got != "http://Codex.acc_541:123@127.0.0.1:2260" {
		t.Fatalf("persisted proxy_url = %q, want %q", got, "http://Codex.acc_541:123@127.0.0.1:2260")
	}

	account := store.FindByID(accountID)
	if account == nil {
		t.Fatalf("store.FindByID(%d) returned nil", accountID)
	}
	account.Mu().RLock()
	gotProxy := account.ProxyURL
	account.Mu().RUnlock()
	if gotProxy != "http://Codex.acc_541:123@127.0.0.1:2260" {
		t.Fatalf("store proxy_url = %q, want %q", gotProxy, "http://Codex.acc_541:123@127.0.0.1:2260")
	}
}

func TestUpdateAccountProxyRouteRejectsRuntimeStateMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbPath := filepath.Join(t.TempDir(), "codex2api.db")
	db, err := database.New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) returned error: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	accountID, err := db.InsertAccount(ctx, "runtime-mismatch-account", "refresh-token", "http://127.0.0.1:7890")
	if err != nil {
		t.Fatalf("InsertAccount returned error: %v", err)
	}

	store := auth.NewStore(db, cache.NewMemory(8), &database.SystemSettings{})
	handler := NewHandler(store, db, cache.NewMemory(8), nil, "")
	router := gin.New()
	handler.RegisterRoutes(router)

	body := bytes.NewBufferString(`{"proxy_url":"http://Codex.acc_999:123@127.0.0.1:2260"}`)
	req := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/admin/accounts/%d", accountID), body)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}

	rows, err := db.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("active rows = %d, want 1", len(rows))
	}
	if got := rows[0].ProxyURL; got != "http://127.0.0.1:7890" {
		t.Fatalf("persisted proxy_url = %q, want %q", got, "http://127.0.0.1:7890")
	}
}

func TestGetSettingsIncludesSchedulerMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbPath := filepath.Join(t.TempDir(), "settings-get.db")
	db, err := database.New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) returned error: %v", err)
	}
	defer db.Close()

	settings := &database.SystemSettings{
		MaxConcurrency: 2,
		GlobalRPM:      33,
		TestModel:      "gpt-5.4",
		SchedulerMode:  auth.SchedulerModeStickySession,
	}
	if err := db.UpdateSystemSettings(context.Background(), settings); err != nil {
		t.Fatalf("UpdateSystemSettings() returned error: %v", err)
	}

	store := auth.NewStore(db, cache.NewMemory(8), settings)
	rateLimiter := proxy.NewRateLimiter(settings.GlobalRPM)
	handler := NewHandler(store, db, cache.NewMemory(8), rateLimiter, "")
	handler.SetPoolSizes(50, 30)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/admin/settings", nil)

	handler.GetSettings(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var resp settingsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.SchedulerMode != auth.SchedulerModeStickySession {
		t.Fatalf("scheduler_mode = %q, want %q", resp.SchedulerMode, auth.SchedulerModeStickySession)
	}
}

func TestUpdateSettingsPersistsSchedulerMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbPath := filepath.Join(t.TempDir(), "settings-update.db")
	db, err := database.New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) returned error: %v", err)
	}
	defer db.Close()

	settings := &database.SystemSettings{
		MaxConcurrency: 2,
		GlobalRPM:      10,
		TestModel:      "gpt-5.4",
		SchedulerMode:  auth.SchedulerModeBalanced,
	}
	if err := db.UpdateSystemSettings(context.Background(), settings); err != nil {
		t.Fatalf("UpdateSystemSettings() returned error: %v", err)
	}

	store := auth.NewStore(db, cache.NewMemory(8), settings)
	handler := NewHandler(store, db, cache.NewMemory(8), proxy.NewRateLimiter(settings.GlobalRPM), "")
	handler.SetPoolSizes(50, 30)

	body := bytes.NewBufferString(`{"scheduler_mode":"sticky_session"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/admin/settings", body)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = req

	handler.UpdateSettings(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := store.GetSchedulerMode(); got != auth.SchedulerModeStickySession {
		t.Fatalf("store scheduler_mode = %q, want %q", got, auth.SchedulerModeStickySession)
	}

	persisted, err := db.GetSystemSettings(context.Background())
	if err != nil {
		t.Fatalf("GetSystemSettings() returned error: %v", err)
	}
	if persisted == nil {
		t.Fatal("GetSystemSettings() returned nil")
	}
	if persisted.SchedulerMode != auth.SchedulerModeStickySession {
		t.Fatalf("persisted scheduler_mode = %q, want %q", persisted.SchedulerMode, auth.SchedulerModeStickySession)
	}
}

func TestUpdateSettingsRejectsInvalidSchedulerMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbPath := filepath.Join(t.TempDir(), "settings-invalid.db")
	db, err := database.New("sqlite", dbPath)
	if err != nil {
		t.Fatalf("New(sqlite) returned error: %v", err)
	}
	defer db.Close()

	settings := &database.SystemSettings{
		MaxConcurrency: 2,
		GlobalRPM:      10,
		TestModel:      "gpt-5.4",
		SchedulerMode:  auth.SchedulerModeBalanced,
	}
	store := auth.NewStore(db, cache.NewMemory(8), settings)
	handler := NewHandler(store, db, cache.NewMemory(8), proxy.NewRateLimiter(settings.GlobalRPM), "")
	handler.SetPoolSizes(50, 30)

	body := bytes.NewBufferString(`{"scheduler_mode":"hotspot"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/admin/settings", body)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = req

	handler.UpdateSettings(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}
