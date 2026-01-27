'use client'

import { useParams } from 'next/navigation'
import { TasksTable } from '@/components/tasks/tasks-table'

export default function JobTasksPage() {
  const params = useParams()
  const jobId = params.jobId as string

  return <TasksTable jobId={jobId} />
}
