import { ClusterStats } from '@/components/dashboard/cluster-stats'
import { RecentJobs } from '@/components/dashboard/recent-jobs'
import { WorkersSummary } from '@/components/dashboard/workers-summary'

export default function DashboardPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Dashboard</h1>
        <p className="text-muted-foreground">Cluster overview and recent activity</p>
      </div>

      <ClusterStats />

      <div className="grid gap-6 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <RecentJobs />
        </div>
        <div>
          <WorkersSummary />
        </div>
      </div>
    </div>
  )
}
