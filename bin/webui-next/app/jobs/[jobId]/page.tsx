'use client'

import { useParams } from 'next/navigation'
import { JobOverview } from '@/components/jobs/job-overview'

export default function JobOverviewPage() {
  const params = useParams()
  const jobId = params.jobId as string

  return <JobOverview jobId={jobId} />
}
