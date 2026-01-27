'use client'

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { JobStatusBadge } from '@/components/jobs/job-status-badge'
import { useJobTasks } from '@/lib/hooks/useApi'
import { formatNumber, formatBytes } from '@/lib/utils'

interface TasksTableProps {
  jobId: string
}

export function TasksTable({ jobId }: TasksTableProps) {
  const { data: tasks, isLoading, error } = useJobTasks(jobId)

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Tasks</CardTitle>
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
          <CardTitle>Tasks</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-destructive">Failed to load tasks</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Tasks ({tasks?.length ?? 0})</CardTitle>
      </CardHeader>
      <CardContent>
        {tasks?.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No tasks found</div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Task ID</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Records</TableHead>
                <TableHead className="text-right">Bytes</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tasks?.map((task) => (
                <TableRow key={task.task_id}>
                  <TableCell className="font-mono text-sm">
                    {task.task_id}
                  </TableCell>
                  <TableCell>
                    <JobStatusBadge state={task.state} />
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatNumber(task.records_processed)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatBytes(task.bytes_processed)}
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
