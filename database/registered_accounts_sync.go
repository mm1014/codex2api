package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func (db *DB) SetRegisteredAccountsDBPath(path string) {
	if db == nil {
		return
	}
	db.registeredAccountsDBPath = strings.TrimSpace(path)
}

func (db *DB) SetRegisteredAccountsSyncURL(rawURL string, token string) {
	if db == nil {
		return
	}
	db.registeredAccountsSyncURL = strings.TrimSpace(rawURL)
	db.registeredAccountsSyncToken = strings.TrimSpace(token)
}

func (db *DB) syncRegisteredAccountSold(ctx context.Context, email string, sold bool) error {
	if strings.TrimSpace(db.registeredAccountsSyncURL) != "" {
		return db.syncRegisteredAccountSoldViaHTTP(ctx, email, sold)
	}

	path := strings.TrimSpace(db.registeredAccountsDBPath)
	email = strings.TrimSpace(email)
	if path == "" || email == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("registered_accounts 数据库不可用: %w", err)
	}

	sidecar, err := sql.Open("sqlite", registeredAccountsSQLiteDSN(path))
	if err != nil {
		return fmt.Errorf("打开 registered_accounts 数据库失败: %w", err)
	}
	defer sidecar.Close()

	soldValue := "false"
	if sold {
		soldValue = "true"
	}
	if _, err := sidecar.ExecContext(
		ctx,
		`UPDATE registered_accounts SET is_sold = ? WHERE email = ? COLLATE NOCASE`,
		soldValue,
		email,
	); err != nil {
		return fmt.Errorf("同步 registered_accounts.is_sold 失败: %w", err)
	}
	return nil
}

func (db *DB) syncRegisteredAccountSoldViaHTTP(ctx context.Context, email string, sold bool) error {
	email = strings.TrimSpace(email)
	if email == "" {
		return nil
	}
	body, err := json.Marshal(map[string]interface{}{
		"email":   email,
		"is_sold": sold,
	})
	if err != nil {
		return fmt.Errorf("序列化 registered_accounts 同步请求失败: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, db.registeredAccountsSyncURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("创建 registered_accounts 同步请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if db.registeredAccountsSyncToken != "" {
		req.Header.Set("X-Sold-Sync-Token", db.registeredAccountsSyncToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("请求 registered_accounts 同步接口失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("registered_accounts 同步接口返回 HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

func registeredAccountsSQLiteDSN(path string) string {
	path = filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); volume != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	values := url.Values{}
	values.Set("mode", "rw")
	values.Add("_pragma", "busy_timeout(5000)")
	values.Add("_pragma", "synchronous(OFF)")
	return (&url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: values.Encode(),
	}).String()
}
