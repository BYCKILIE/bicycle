import { MetricsCharts } from '@/components/metrics/metrics-charts'

export default function MetricsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Metrics</h1>
        <p className="text-muted-foreground">Cluster performance metrics</p>
      </div>

      <MetricsCharts />
    </div>
  )
}
