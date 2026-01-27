'use client'

import { useRouter } from 'next/navigation'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { useWorkers } from '@/lib/hooks/useApi'

export function WorkersTable() {
  const router = useRouter()
  const { data: workers, isLoading, error } = useWorkers()

  const handleRowClick = (workerId: string) => {
    router.push(`/workers/${workerId}`)
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Workers</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <Skeleton key={i} className="h-16 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Workers</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-destructive">Failed to load workers</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Workers ({workers?.length ?? 0})</CardTitle>
      </CardHeader>
      <CardContent>
        {workers?.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No workers connected</div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Worker ID</TableHead>
                <TableHead>Hostname</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Slots</TableHead>
                <TableHead>CPU</TableHead>
                <TableHead>Memory</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {workers?.map((worker) => (
                <TableRow
                  key={worker.worker_id}
                  onClick={() => handleRowClick(worker.worker_id)}
                  className="cursor-pointer"
                >
                  <TableCell className="font-mono text-sm text-primary">
                    {worker.worker_id}
                  </TableCell>
                  <TableCell className="font-medium">{worker.hostname}</TableCell>
                  <TableCell>
                    <Badge variant="success">Active</Badge>
                  </TableCell>
                  <TableCell>
                    <span className="text-primary">{worker.slots_used}</span>
                    <span className="text-muted-foreground"> / {worker.slots}</span>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Progress value={worker.cpu_usage} className="w-16 h-2" />
                      <span className="text-sm">{worker.cpu_usage.toFixed(1)}%</span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <span className="font-mono text-sm">{worker.memory_used_mb} MB</span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}
