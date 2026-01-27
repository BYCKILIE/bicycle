'use client'

import { useParams } from 'next/navigation'
import { JobGraphView } from '@/components/graph/job-graph-view'

export default function JobGraphPage() {
  const params = useParams()
  const jobId = params.jobId as string

  return <JobGraphView jobId={jobId} />
}
