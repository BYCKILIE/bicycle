'use client'

import { Server, Cpu, Play, FileText } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useClusterInfo, useMetrics } from '@/lib/hooks/useApi'
import { formatNumber, formatUptime } from '@/lib/utils'

export function ClusterStats() {
  const { data: cluster, isLoading: clusterLoading } = useClusterInfo()
  const { data: metrics, isLoading: metricsLoading } = useMetrics()

  const stats = [
    {
      label: 'Workers',
      value: cluster?.workers ?? 0,
      subValue: `${cluster?.total_slots ?? 0} slots`,
      icon: Server,
      color: 'text-blue-500',
    },
    {
      label: 'Available Slots',
      value: cluster?.available_slots ?? 0,
      subValue: `of ${cluster?.total_slots ?? 0}`,
      icon: Cpu,
      color: 'text-green-500',
    },
    {
      label: 'Running Jobs',
      value: cluster?.running_jobs ?? 0,
      subValue: '',
      icon: Play,
      color: 'text-yellow-500',
    },
    {
      label: 'Records Processed',
      value: formatNumber(metrics?.total_records_processed ?? 0),
      subValue: formatUptime(metrics?.uptime_seconds ?? 0) + ' uptime',
      icon: FileText,
      color: 'text-purple-500',
    },
  ]

  if (clusterLoading || metricsLoading) {
    return (
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {[...Array(4)].map((_, i) => (
          <Card key={i}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-8 w-8 rounded" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-16 mb-1" />
              <Skeleton className="h-3 w-24" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      {stats.map((stat) => (
        <Card key={stat.label}>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">{stat.label}</CardTitle>
            <stat.icon className={`h-5 w-5 ${stat.color}`} />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stat.value}</div>
            {stat.subValue && (
              <p className="text-xs text-muted-foreground">{stat.subValue}</p>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
