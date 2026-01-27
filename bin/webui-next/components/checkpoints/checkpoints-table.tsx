'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { useJobCheckpoints } from '@/lib/hooks/useApi'
import { formatTimestamp, formatDuration, formatBytes } from '@/lib/utils'

interface CheckpointsTableProps {
  jobId: string
}

export function CheckpointsTable({ jobId }: CheckpointsTableProps) {
  const { data: checkpoints, isLoading, error } = useJobCheckpoints(jobId)

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Checkpoints</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
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
          <CardTitle>Checkpoints</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-destructive">Failed to load checkpoints</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Checkpoints ({checkpoints?.length ?? 0})</CardTitle>
      </CardHeader>
      <CardContent>
        {checkpoints?.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No checkpoints yet</div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Trigger Time</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead>State Size</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {checkpoints?.map((cp) => (
                <TableRow key={cp.checkpoint_id}>
                  <TableCell className="font-mono">{cp.checkpoint_id}</TableCell>
                  <TableCell>{formatTimestamp(cp.timestamp)}</TableCell>
                  <TableCell>{formatDuration(cp.duration_ms)}</TableCell>
                  <TableCell>{formatBytes(cp.state_size_bytes)}</TableCell>
                  <TableCell>
                    <Badge variant="success">Completed</Badge>
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
