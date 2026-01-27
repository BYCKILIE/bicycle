'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { ArrowRight } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { useWorkers } from '@/lib/hooks/useApi'

export function WorkersSummary() {
  const router = useRouter()
  const { data: workers, isLoading } = useWorkers()

  const handleWorkerClick = (workerId: string) => {
    router.push(`/workers/${workerId}`)
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Workers</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {[...Array(3)].map((_, i) => (
            <Skeleton key={i} className="h-16 w-full" />
          ))}
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>Workers</CardTitle>
        <Link href="/workers">
          <Button variant="ghost" size="sm" className="gap-1">
            View All <ArrowRight className="h-4 w-4" />
          </Button>
        </Link>
      </CardHeader>
      <CardContent>
        {workers?.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No workers connected</div>
        ) : (
          <div className="space-y-4">
            {workers?.slice(0, 5).map((worker) => (
              <div
                key={worker.worker_id}
                className="space-y-2 p-2 rounded-md hover:bg-muted cursor-pointer transition-colors"
                onClick={() => handleWorkerClick(worker.worker_id)}
              >
                <div className="flex items-center justify-between">
                  <span className="font-mono text-sm text-primary">
                    {worker.worker_id}
                  </span>
                  <span className="text-sm text-muted-foreground">
                    {worker.slots_used}/{worker.slots} slots
                  </span>
                </div>
                <div className="text-xs text-muted-foreground">{worker.hostname}</div>
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span className="text-muted-foreground">CPU</span>
                      <span>{worker.cpu_usage.toFixed(1)}%</span>
                    </div>
                    <Progress value={worker.cpu_usage} className="h-2" />
                  </div>
                  <div>
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span className="text-muted-foreground">Memory</span>
                      <span>{worker.memory_used_mb} MB</span>
                    </div>
                    <Progress value={worker.memory_used_mb / 10} max={100} className="h-2" />
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
