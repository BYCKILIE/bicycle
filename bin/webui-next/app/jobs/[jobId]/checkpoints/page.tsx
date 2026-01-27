'use client'

import { useParams } from 'next/navigation'
import { CheckpointsTable } from '@/components/checkpoints/checkpoints-table'

export default function JobCheckpointsPage() {
  const params = useParams()
  const jobId = params.jobId as string

  return <CheckpointsTable jobId={jobId} />
}
