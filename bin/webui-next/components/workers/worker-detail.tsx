'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { useWorkers } from '@/lib/hooks/useApi'

interface WorkerDetailProps {
  workerId: string
}

export function WorkerDetail({ workerId }: WorkerDetailProps) {
  const { data: workers, isLoading, error } = useWorkers()
  const worker = workers?.find((w) => w.worker_id === workerId)

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <Skeleton className="h-6 w-48" />
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2">
              {[...Array(4)].map((_, i) => (
                <Skeleton key={i} className="h-20" />
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (error || !worker) {
    return (
      <Card>
        <CardContent className="py-12">
          <div className="text-center text-destructive">
            {error ? 'Failed to load worker' : 'Worker not found'}
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>{worker.hostname}</span>
            <Badge variant="success">Active</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid gap-4 md:grid-cols-2">
            <div>
              <dt className="text-sm text-muted-foreground">Worker ID</dt>
              <dd className="font-mono text-sm">{worker.worker_id}</dd>
            </div>
            <div>
              <dt className="text-sm text-muted-foreground">Hostname</dt>
              <dd>{worker.hostname}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Slots</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold mb-2">
              {worker.slots_used} / {worker.slots}
            </div>
            <Progress
              value={(worker.slots_used / worker.slots) * 100}
              className="h-3"
            />
            <div className="text-sm text-muted-foreground mt-2">
              {worker.slots - worker.slots_used} available
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">CPU Usage</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold mb-2">
              {worker.cpu_usage.toFixed(1)}%
            </div>
            <Progress value={worker.cpu_usage} className="h-3" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Memory Usage</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold mb-2">
              {worker.memory_used_mb} MB
            </div>
            <Progress value={worker.memory_used_mb / 10} max={100} className="h-3" />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
