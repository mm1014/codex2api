import type { ReactNode } from 'react'
import { Card, CardContent } from '@/components/ui/card'

interface StatCardProps {
  icon: ReactNode
  iconClass: string
  label: string
  value: number | string
  sub?: string
}

const iconColors: Record<string, string> = {
  blue: 'bg-[hsl(var(--info-bg))] text-[hsl(var(--info))]',
  green: 'bg-[hsl(var(--success-bg))] text-[hsl(var(--success))]',
  red: 'bg-destructive/12 text-destructive',
  purple: 'bg-primary/12 text-primary',
}

export default function StatCard({ icon, iconClass, label, value, sub }: StatCardProps) {
  return (
    <Card className="transition-all duration-150 hover:-translate-y-0.5 hover:shadow-md">
      <CardContent className="flex flex-col justify-between gap-4 min-h-[168px] p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <label className="block text-[11px] font-bold tracking-[0.16em] uppercase text-muted-foreground">
              {label}
            </label>
            <div className="mt-4 text-[clamp(34px,4vw,44px)] font-bold leading-none tracking-tighter text-foreground">
              {value}
            </div>
          </div>
          <div className={`size-14 flex items-center justify-center shrink-0 rounded-[20px] ${iconColors[iconClass] || 'bg-primary/12 text-primary'}`} aria-hidden="true">
            <span className="[&_svg]:size-[22px]">{icon}</span>
          </div>
        </div>
        {sub ? (
          <div className="pt-4 border-t border-border text-[13px] text-muted-foreground">
            {sub}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
