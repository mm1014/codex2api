import type { ReactNode } from 'react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { api } from '../api'
import PageHeader from '../components/PageHeader'
import Pagination from '../components/Pagination'
import StateShell from '../components/StateShell'
import { useDataLoader } from '../hooks/useDataLoader'
import { useToast } from '../hooks/useToast'
import type { UsageLog, UsageStats } from '../types'
import { formatRelativeTime } from '../utils/time'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Activity, Box, Clock, Zap, AlertTriangle } from 'lucide-react'

function formatTokens(value?: number | null): string {
  if (value === undefined || value === null) return '0'
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return value.toLocaleString()
}

const statIcons: Record<string, ReactNode> = {
  requests: <Activity className="size-[22px]" />,
  tokens: <Box className="size-[22px]" />,
  rpm: <Clock className="size-[22px]" />,
  tpm: <Zap className="size-[22px]" />,
  error: <AlertTriangle className="size-[22px]" />,
}

export default function Usage() {
  const { toast, showToast } = useToast()
  const [page, setPage] = useState(1)
  const [clearing, setClearing] = useState(false)
  const PAGE_SIZE = 20

  const loadUsageData = useCallback(async () => {
    const [stats, logsResponse] = await Promise.all([api.getUsageStats(), api.getUsageLogs(100)])
    return {
      stats,
      logs: logsResponse.logs ?? [],
    }
  }, [])

  const { data, loading, error, reload, reloadSilently } = useDataLoader<{
    stats: UsageStats | null
    logs: UsageLog[]
  }>({
    initialData: {
      stats: null,
      logs: [],
    },
    load: loadUsageData,
  })

  useEffect(() => {
    const timer = window.setInterval(() => {
      void reloadSilently()
    }, 30000)

    return () => window.clearInterval(timer)
  }, [reloadSilently])

  const { stats, logs } = data
  const totalPages = Math.max(1, Math.ceil(logs.length / PAGE_SIZE))
  const pagedLogs = useMemo(() => logs.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE), [logs, page])
  const totalRequests = stats?.total_requests ?? 0
  const totalTokens = stats?.total_tokens ?? 0
  const totalPromptTokens = stats?.total_prompt_tokens ?? 0
  const totalCompletionTokens = stats?.total_completion_tokens ?? 0
  const todayRequests = stats?.today_requests ?? 0
  const rpm = stats?.rpm ?? 0
  const tpm = stats?.tpm ?? 0
  const errorRate = stats?.error_rate ?? 0
  const avgDurationMs = stats?.avg_duration_ms ?? 0
  const successRequests = totalRequests - Math.round(totalRequests * errorRate / 100)

  return (
    <StateShell
      variant="page"
      loading={loading}
      error={error}
      onRetry={() => void reload()}
      loadingTitle="正在加载使用统计"
      loadingDescription="请求日志和性能指标正在同步。"
      errorTitle="统计页加载失败"
    >
      <>
        <PageHeader
          title="使用统计"
          description="请求日志与性能指标"
          onRefresh={() => void reload()}
        />

        {/* Top stats: 2 columns */}
        <div className="grid grid-cols-2 gap-4 mb-3 max-sm:grid-cols-1">
          <Card>
            <CardContent className="flex flex-col gap-4 min-h-[180px] p-6">
              <div className="flex items-center justify-between gap-3">
                <span className="text-xs font-bold tracking-[0.12em] uppercase text-muted-foreground">总请求数</span>
                <div className="size-14 flex items-center justify-center rounded-[20px] bg-primary/12 text-primary">
                  {statIcons.requests}
                </div>
              </div>
              <div className="text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter">
                {formatTokens(totalRequests)}
              </div>
              <div className="text-[13px] text-muted-foreground leading-relaxed">
                <span className="text-[hsl(var(--success))]">● 成功请求: {formatTokens(successRequests)}</span>
                <span className="ml-2 text-muted-foreground">● 今日请求: {formatTokens(todayRequests)}</span>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-4 min-h-[180px] p-6">
              <div className="flex items-center justify-between gap-3">
                <span className="text-xs font-bold tracking-[0.12em] uppercase text-muted-foreground">总 Token 数</span>
                <div className="size-14 flex items-center justify-center rounded-[20px] bg-[hsl(var(--info-bg))] text-[hsl(var(--info))]">
                  {statIcons.tokens}
                </div>
              </div>
              <div className="text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter">
                {formatTokens(totalTokens)}
              </div>
              <div className="text-[13px] text-muted-foreground leading-relaxed">
                <span>输入 Tokens: {formatTokens(totalPromptTokens)}</span>
                <span className="ml-2">输出 Tokens: {formatTokens(totalCompletionTokens)}</span>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Bottom stats: 3 columns */}
        <div className="grid grid-cols-3 gap-4 mb-6 max-sm:grid-cols-1">
          <Card>
            <CardContent className="flex flex-col gap-4 min-h-[180px] p-6">
              <div className="flex items-center justify-between gap-3">
                <span className="text-xs font-bold tracking-[0.12em] uppercase text-muted-foreground">RPM</span>
                <div className="size-14 flex items-center justify-center rounded-[20px] bg-[hsl(var(--success-bg))] text-[hsl(var(--success))]">
                  {statIcons.rpm}
                </div>
              </div>
              <div className="text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter">
                {Math.round(rpm)}
              </div>
              <div className="text-[13px] text-muted-foreground">每分钟请求数</div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-4 min-h-[180px] p-6">
              <div className="flex items-center justify-between gap-3">
                <span className="text-xs font-bold tracking-[0.12em] uppercase text-muted-foreground">TPM</span>
                <div className="size-14 flex items-center justify-center rounded-[20px] bg-destructive/12 text-destructive">
                  {statIcons.tpm}
                </div>
              </div>
              <div className="text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter">
                {formatTokens(tpm)}
              </div>
              <div className="text-[13px] text-muted-foreground">每分钟 Token 数</div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-4 min-h-[180px] p-6">
              <div className="flex items-center justify-between gap-3">
                <span className="text-xs font-bold tracking-[0.12em] uppercase text-muted-foreground">错误率</span>
                <div className="size-14 flex items-center justify-center rounded-[20px] bg-[hsl(36_72%_40%/0.12)] text-[hsl(36,72%,40%)]">
                  {statIcons.error}
                </div>
              </div>
              <div className="text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter">
                {errorRate.toFixed(1)}%
              </div>
              <div className="text-[13px] text-muted-foreground">平均延迟: {Math.round(avgDurationMs)}ms</div>
            </CardContent>
          </Card>
        </div>

        {/* Logs table */}
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
              <h3 className="text-base font-semibold text-foreground">请求记录</h3>
              <div className="flex items-center gap-3">
                <span className="text-xs text-muted-foreground">最近 {logs.length} 条</span>
                <Button
                  variant="destructive"
                  size="sm"
                  disabled={clearing || logs.length === 0}
                  onClick={async () => {
                    if (!confirm('确定清空所有使用日志吗？此操作不可恢复。')) return
                    setClearing(true)
                    try {
                      await api.clearUsageLogs()
                      showToast('日志已清空')
                      setPage(1)
                      void reload()
                    } catch (e) {
                      showToast(`清空失败: ${String(e)}`, 'error')
                    } finally {
                      setClearing(false)
                    }
                  }}
                >
                  {clearing ? '清空中...' : '清空日志'}
                </Button>
              </div>
            </div>
            <StateShell
              variant="section"
              isEmpty={logs.length === 0}
              emptyTitle="暂无请求记录"
              emptyDescription="请求进入代理后，会在这里展示最近日志和状态码。"
            >
              <div className="overflow-auto border border-border rounded-xl">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-[13px] font-semibold">状态</TableHead>
                      <TableHead className="text-[13px] font-semibold">模型</TableHead>
                      <TableHead className="text-[13px] font-semibold">推理强度</TableHead>
                      <TableHead className="text-[13px] font-semibold">端点</TableHead>
                      <TableHead className="text-[13px] font-semibold">类型</TableHead>
                      <TableHead className="text-[13px] font-semibold">TOKEN</TableHead>
                      <TableHead className="text-[13px] font-semibold">首字时间</TableHead>
                      <TableHead className="text-[13px] font-semibold">总耗时</TableHead>
                      <TableHead className="text-[13px] font-semibold">时间</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pagedLogs.map((log) => (
                      <TableRow key={log.id}>
                        <TableCell>
                          <Badge
                            variant={log.status_code < 400 ? 'default' : log.status_code < 500 ? 'secondary' : 'destructive'}
                            className="text-[13px]"
                          >
                            {log.status_code}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className="text-[13px]">
                            {log.model || '-'}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {log.reasoning_effort ? (
                            <Badge
                              variant="outline"
                              className="text-[12px]"
                              style={{
                                background: log.reasoning_effort === 'high' ? 'rgba(239, 68, 68, 0.12)' :
                                           log.reasoning_effort === 'medium' ? 'rgba(245, 158, 11, 0.12)' :
                                           'rgba(34, 197, 94, 0.12)',
                                color: log.reasoning_effort === 'high' ? '#ef4444' :
                                       log.reasoning_effort === 'medium' ? '#f59e0b' :
                                       '#22c55e',
                                borderColor: 'transparent',
                              }}
                            >
                              {log.reasoning_effort}
                            </Badge>
                          ) : <span className="text-[13px] text-muted-foreground">-</span>}
                        </TableCell>
                        <TableCell>
                          <div className="text-[13px] leading-relaxed">
                            <span className="font-mono text-muted-foreground">
                              {log.inbound_endpoint || log.endpoint || '-'}
                            </span>
                            {log.upstream_endpoint && log.upstream_endpoint !== log.inbound_endpoint && (
                              <span className="text-muted-foreground"> → {log.upstream_endpoint}</span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant="outline"
                            className="text-[12px]"
                            style={{
                              background: log.stream ? 'rgba(99, 102, 241, 0.12)' : 'rgba(107, 114, 128, 0.12)',
                              color: log.stream ? '#6366f1' : '#6b7280',
                              borderColor: 'transparent',
                            }}
                          >
                            {log.stream ? 'stream' : 'sync'}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {log.status_code < 400 && (log.input_tokens > 0 || log.output_tokens > 0) ? (
                            <div className="text-[13px] leading-relaxed">
                              <span className="text-blue-500">↓{formatTokens(log.input_tokens)}</span>
                              <span className="mx-1 text-border">|</span>
                              <span className="text-emerald-500">↑{formatTokens(log.output_tokens)}</span>
                              {log.reasoning_tokens > 0 && (
                                <>
                                  <span className="mx-1 text-border">|</span>
                                  <span className="text-amber-500">💡{formatTokens(log.reasoning_tokens)}</span>
                                </>
                              )}
                            </div>
                          ) : (
                            <span className="text-[13px] text-muted-foreground">-</span>
                          )}
                        </TableCell>
                        <TableCell>
                          {log.first_token_ms > 0 ? (
                            <span className={`font-mono text-[13px] ${log.first_token_ms > 5000 ? 'text-red-500' : log.first_token_ms > 2000 ? 'text-amber-500' : 'text-emerald-500'}`}>
                              {log.first_token_ms > 1000 ? `${(log.first_token_ms / 1000).toFixed(1)}s` : `${log.first_token_ms}ms`}
                            </span>
                          ) : <span className="text-[13px] text-muted-foreground">-</span>}
                        </TableCell>
                        <TableCell>
                          <span className={`font-mono text-[13px] ${log.duration_ms > 30000 ? 'text-red-500' : log.duration_ms > 10000 ? 'text-amber-500' : 'text-muted-foreground'}`}>
                            {log.duration_ms > 1000 ? `${(log.duration_ms / 1000).toFixed(1)}s` : `${log.duration_ms}ms`}
                          </span>
                        </TableCell>
                        <TableCell className="text-[13px] text-muted-foreground whitespace-nowrap">
                          {formatRelativeTime(log.created_at, { variant: 'compact', includeSeconds: true })}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <Pagination
                page={page}
                totalPages={totalPages}
                onPageChange={setPage}
                totalItems={logs.length}
                pageSize={PAGE_SIZE}
              />
            </StateShell>
          </CardContent>
        </Card>

        {toast ? (
          <div
            className={`fixed right-6 bottom-6 z-[2000] px-4 py-3 rounded-2xl text-white text-sm font-bold shadow-lg ${
              toast.type === 'error' ? 'bg-destructive' : 'bg-[hsl(var(--success))]'
            }`}
            style={{ animation: 'toast-slide-up 0.22s ease' }}
          >
            {toast.msg}
          </div>
        ) : null}
      </>
    </StateShell>
  )
}
