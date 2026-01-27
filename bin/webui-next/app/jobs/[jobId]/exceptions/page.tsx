'use client'

import { useParams } from 'next/navigation'
import { JobExceptions } from '@/components/jobs/job-exceptions'

export default function JobExceptionsPage() {
  const params = useParams()
  const jobId = params.jobId as string

  return <JobExceptions jobId={jobId} />
}
