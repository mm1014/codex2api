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

func (db *DB) syncRegisteredAccountSold(ctx context.Context, email string, refreshToken string, sold bool) error {
	if strings.TrimSpace(db.registeredAccountsSyncURL) != "" {
		return db.syncRegisteredAccountSoldViaHTTP(ctx, email, refreshToken, sold)
	}

	path := strings.TrimSpace(db.registeredAccountsDBPath)
	email = strings.TrimSpace(email)
	refreshToken = strings.TrimSpace(refreshToken)
	if path == "" || (email == "" && refreshToken == "") {
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
	var affected int64
	if email != "" {
		result, err := sidecar.ExecContext(
			ctx,
			`UPDATE registered_accounts SET is_sold = ? WHERE email = ? COLLATE NOCASE`,
			soldValue,
			email,
		)
		if err != nil {
			return fmt.Errorf("同步 registered_accounts.is_sold 失败: %w", err)
		}
		affected, err = result.RowsAffected()
		if err != nil {
			return fmt.Errorf("读取 registered_accounts 同步结果失败: %w", err)
		}
	}
	if affected == 0 && refreshToken != "" && registeredAccountsColumnExists(ctx, sidecar, "refresh_token") {
		result, err := sidecar.ExecContext(
			ctx,
			`UPDATE registered_accounts SET is_sold = ? WHERE refresh_token = ?`,
			soldValue,
			refreshToken,
		)
		if err != nil {
			return fmt.Errorf("按 refresh_token 同步 registered_accounts.is_sold 失败: %w", err)
		}
		affected, err = result.RowsAffected()
		if err != nil {
			return fmt.Errorf("读取 registered_accounts 同步结果失败: %w", err)
		}
	}
	if affected == 0 {
		return fmt.Errorf("registered_accounts 未匹配到邮箱/refresh_token")
	}
	return nil
}

func (db *DB) lookupRegisteredAccountSold(ctx context.Context, email string, refreshToken string) (bool, bool, error) {
	path := strings.TrimSpace(db.registeredAccountsDBPath)
	email = strings.TrimSpace(email)
	refreshToken = strings.TrimSpace(refreshToken)
	if path == "" || (email == "" && refreshToken == "") {
		return false, false, nil
	}
	if _, err := os.Stat(path); err != nil {
		return false, false, fmt.Errorf("registered_accounts 数据库不可用: %w", err)
	}

	sold, found, err := lookupRegisteredAccountSoldAtPath(ctx, path, email, refreshToken)
	if err == nil {
		return sold, found, nil
	}
	snapshotSold, snapshotFound, snapshotErr := lookupRegisteredAccountSoldFromSnapshot(ctx, path, email, refreshToken)
	if snapshotErr == nil {
		return snapshotSold, snapshotFound, nil
	}
	return false, false, err
}

func (db *DB) syncRegisteredAccountSoldViaHTTP(ctx context.Context, email string, refreshToken string, sold bool) error {
	email = strings.TrimSpace(email)
	refreshToken = strings.TrimSpace(refreshToken)
	if email == "" && refreshToken == "" {
		return nil
	}
	body, err := json.Marshal(map[string]interface{}{
		"email":         email,
		"refresh_token": refreshToken,
		"is_sold":       sold,
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
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("registered_accounts 同步接口返回 HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if len(bytes.TrimSpace(data)) > 0 {
		var payload struct {
			Updated *int `json:"updated"`
		}
		if err := json.Unmarshal(data, &payload); err == nil && payload.Updated != nil && *payload.Updated == 0 {
			return fmt.Errorf("registered_accounts 未匹配到邮箱/refresh_token")
		}
	}
	return nil
}

func lookupRegisteredAccountSoldAtPath(ctx context.Context, path string, email string, refreshToken string) (bool, bool, error) {
	sidecar, err := sql.Open("sqlite", registeredAccountsSQLiteReadDSN(path))
	if err != nil {
		return false, false, fmt.Errorf("打开 registered_accounts 数据库失败: %w", err)
	}
	defer sidecar.Close()

	if strings.TrimSpace(email) != "" {
		sold, found, err := lookupRegisteredAccountSoldByColumn(ctx, sidecar, "email", email, true)
		if err != nil || found {
			return sold, found, err
		}
	}
	if strings.TrimSpace(refreshToken) != "" && registeredAccountsColumnExists(ctx, sidecar, "refresh_token") {
		return lookupRegisteredAccountSoldByColumn(ctx, sidecar, "refresh_token", refreshToken, false)
	}
	return false, false, nil
}

func lookupRegisteredAccountSoldByColumn(ctx context.Context, sidecar *sql.DB, column string, value string, nocase bool) (bool, bool, error) {
	var raw interface{}
	query := fmt.Sprintf(`SELECT is_sold FROM registered_accounts WHERE %s = ? ORDER BY id DESC LIMIT 1`, column)
	if nocase {
		query = fmt.Sprintf(`SELECT is_sold FROM registered_accounts WHERE %s = ? COLLATE NOCASE ORDER BY id DESC LIMIT 1`, column)
	}
	err := sidecar.QueryRowContext(
		ctx,
		query,
		value,
	).Scan(&raw)
	if err == sql.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("读取 registered_accounts.is_sold 失败: %w", err)
	}

	sold, err := parseDBBoolValue(raw)
	if err != nil {
		return false, false, fmt.Errorf("解析 registered_accounts.is_sold 失败: %w", err)
	}
	return sold, true, nil
}

func lookupRegisteredAccountSoldFromSnapshot(ctx context.Context, path string, email string, refreshToken string) (bool, bool, error) {
	dir, err := os.MkdirTemp("", "registered-accounts-*")
	if err != nil {
		return false, false, err
	}
	defer os.RemoveAll(dir)

	snapshotPath := filepath.Join(dir, filepath.Base(path))
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := path + suffix
		dst := snapshotPath + suffix
		if err := copyFileIfExists(src, dst); err != nil {
			return false, false, err
		}
	}
	return lookupRegisteredAccountSoldAtPath(ctx, snapshotPath, email, refreshToken)
}

func registeredAccountsColumnExists(ctx context.Context, sidecar *sql.DB, column string) bool {
	rows, err := sidecar.QueryContext(ctx, `PRAGMA table_info(registered_accounts)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false
		}
		if strings.EqualFold(name, column) {
			return true
		}
	}
	return false
}

func copyFileIfExists(src string, dst string) error {
	in, err := os.Open(src)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
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

func registeredAccountsSQLiteReadDSN(path string) string {
	path = filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); volume != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	values := url.Values{}
	values.Set("mode", "ro")
	values.Add("_pragma", "busy_timeout(5000)")
	return (&url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: values.Encode(),
	}).String()
}
