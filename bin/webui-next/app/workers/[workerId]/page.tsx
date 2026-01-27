'use client'

import { useParams } from 'next/navigation'
import { WorkerDetail } from '@/components/workers/worker-detail'

export default function WorkerDetailPage() {
  const params = useParams()
  const workerId = params.workerId as string

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Worker Details</h1>
        <p className="text-muted-foreground font-mono">{workerId}</p>
      </div>

      <WorkerDetail workerId={workerId} />
    </div>
  )
}
