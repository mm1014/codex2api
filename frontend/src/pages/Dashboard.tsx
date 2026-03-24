import type { ReactNode } from 'react'
import { useCallback } from 'react'
import { api } from '../api'
import PageHeader from '../components/PageHeader'
import StateShell from '../components/StateShell'
import StatCard from '../components/StatCard'
import StatusBadge from '../components/StatusBadge'
import type { AccountRow, StatsResponse } from '../types'
import { useDataLoader } from '../hooks/useDataLoader'
import { formatRelativeTime } from '../utils/time'
import { Card, CardContent } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Users, CheckCircle, XCircle, Activity } from 'lucide-react'

export default function Dashboard() {
  const loadDashboardData = useCallback(async () => {
    const [stats, accountsResponse] = await Promise.all([api.getStats(), api.getAccounts()])
    return {
      stats,
      accounts: accountsResponse.accounts ?? [],
    }
  }, [])

  const { data, loading, error, reload } = useDataLoader<{
    stats: StatsResponse | null
    accounts: AccountRow[]
  }>({
    initialData: {
      stats: null,
      accounts: [],
    },
    load: loadDashboardData,
  })

  const { stats, accounts } = data
  const total = stats?.total ?? 0
  const available = stats?.available ?? 0
  const errorCount = stats?.error ?? 0
  const todayRequests = stats?.today_requests ?? 0

  const icons: Record<string, ReactNode> = {
    total: <Users className="size-[22px]" />,
    available: <CheckCircle className="size-[22px]" />,
    error: <XCircle className="size-[22px]" />,
    requests: <Activity className="size-[22px]" />,
  }

  return (
    <StateShell
      variant="page"
      loading={loading}
      error={error}
      onRetry={() => void reload()}
      loadingTitle="正在加载仪表盘"
      loadingDescription="系统统计和账号状态正在同步。"
      errorTitle="仪表盘加载失败"
    >
      <>
        <PageHeader
          title="仪表盘"
          description="系统概览"
          onRefresh={() => void reload()}
        />

        <div className="grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-4 mb-6">
          <StatCard icon={icons.total} iconClass="blue" label="总账号" value={total} />
          <StatCard
            icon={icons.available}
            iconClass="green"
            label="可用"
            value={available}
            sub={`${total ? Math.round((available / total) * 100) : 0}% 可用率`}
          />
          <StatCard icon={icons.error} iconClass="red" label="异常" value={errorCount} />
          <StatCard icon={icons.requests} iconClass="purple" label="今日请求" value={todayRequests} />
        </div>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between gap-4 mb-4">
              <h3 className="text-base font-semibold text-foreground">账号状态</h3>
              <span className="text-xs text-muted-foreground">{accounts.length} 个账号</span>
            </div>
            <StateShell
              variant="section"
              isEmpty={accounts.length === 0}
              emptyTitle="暂无账号数据"
              emptyDescription="账号加入代理池后，会在这里展示状态和最近更新时间。"
            >
              <div className="overflow-auto border border-border rounded-xl">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-[13px] font-semibold">名称</TableHead>
                      <TableHead className="text-[13px] font-semibold">邮箱</TableHead>
                      <TableHead className="text-[13px] font-semibold">套餐</TableHead>
                      <TableHead className="text-[13px] font-semibold">状态</TableHead>
                      <TableHead className="text-[13px] font-semibold">更新时间</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accounts.map((account) => (
                      <TableRow key={account.id}>
                        <TableCell className="text-[14px] font-medium">{account.name || `账号 #${account.id}`}</TableCell>
                        <TableCell className="text-[14px] text-muted-foreground">{account.email || '-'}</TableCell>
                        <TableCell className="text-[14px] font-mono">{account.plan_type || '-'}</TableCell>
                        <TableCell><StatusBadge status={account.status} /></TableCell>
                        <TableCell className="text-[14px] text-muted-foreground">{formatRelativeTime(account.updated_at)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </StateShell>
          </CardContent>
        </Card>
      </>
    </StateShell>
  )
}
