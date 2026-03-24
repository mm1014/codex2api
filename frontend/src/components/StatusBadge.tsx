import { Badge } from '@/components/ui/badge'

interface StatusBadgeProps {
  status?: string | null
}

const statusConfig: Record<string, { variant: 'default' | 'secondary' | 'destructive' | 'outline'; label: string; dotColor: string }> = {
  active: { variant: 'default', label: '可用', dotColor: 'bg-emerald-500' },
  ready: { variant: 'default', label: '就绪', dotColor: 'bg-emerald-500' },
  cooldown: { variant: 'secondary', label: '冷却中', dotColor: 'bg-amber-500' },
  rate_limited: { variant: 'secondary', label: '限流', dotColor: 'bg-yellow-500' },
  unauthorized: { variant: 'destructive', label: '封禁', dotColor: 'bg-red-500' },
  error: { variant: 'destructive', label: '错误', dotColor: 'bg-red-400' },
  paused: { variant: 'outline', label: '已暂停', dotColor: 'bg-blue-500' },
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const key = status ?? 'unknown'
  const config = statusConfig[key] ?? { variant: 'outline' as const, label: key, dotColor: 'bg-gray-400' }

  return (
    <Badge variant={config.variant} className="gap-1.5 text-[13px]">
      <span className={`size-1.5 rounded-full ${config.dotColor}`} />
      {config.label}
    </Badge>
  )
}
