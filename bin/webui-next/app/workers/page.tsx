import { WorkersTable } from '@/components/workers/workers-table'

export default function WorkersPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Workers</h1>
        <p className="text-muted-foreground">Cluster worker nodes</p>
      </div>

      <WorkersTable />
    </div>
  )
}
