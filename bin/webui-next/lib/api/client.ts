import type {
  ClusterInfo,
  JobDetail,
  JobSummary,
  JobGraph,
  JobExceptions,
  MetricsResponse,
  MetricsHistory,
  TaskSummary,
  CheckpointInfo,
  WorkerInfo,
} from '@/types'

const API_BASE = '/api'

async function fetchApi<T>(endpoint: string): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`)
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`)
  }
  return response.json()
}

async function postApi<T, R>(endpoint: string, data: T): Promise<R> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`)
  }
  return response.json()
}

async function deleteApi<T>(endpoint: string): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, { method: 'DELETE' })
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`)
  }
  return response.json()
}

// Cluster
export async function getClusterInfo(): Promise<ClusterInfo> {
  return fetchApi<ClusterInfo>('/cluster')
}

// Jobs
export async function listJobs(): Promise<JobSummary[]> {
  return fetchApi<JobSummary[]>('/jobs')
}

export async function getJob(jobId: string): Promise<JobDetail> {
  return fetchApi<JobDetail>(`/jobs/${jobId}`)
}

export async function cancelJob(jobId: string): Promise<{ success: boolean; message: string }> {
  return deleteApi(`/jobs/${jobId}`)
}

export async function getJobTasks(jobId: string): Promise<TaskSummary[]> {
  return fetchApi<TaskSummary[]>(`/jobs/${jobId}/tasks`)
}

export async function getJobCheckpoints(jobId: string): Promise<CheckpointInfo[]> {
  return fetchApi<CheckpointInfo[]>(`/jobs/${jobId}/checkpoints`)
}

export async function getJobGraph(jobId: string): Promise<JobGraph> {
  return fetchApi<JobGraph>(`/jobs/${jobId}/graph`)
}

export async function getJobExceptions(jobId: string): Promise<JobExceptions> {
  return fetchApi<JobExceptions>(`/jobs/${jobId}/exceptions`)
}

// Workers
export async function listWorkers(): Promise<WorkerInfo[]> {
  return fetchApi<WorkerInfo[]>('/cluster/workers')
}

// Metrics
export async function getMetrics(): Promise<MetricsResponse> {
  return fetchApi<MetricsResponse>('/metrics')
}

export async function getMetricsHistory(minutes: number = 15): Promise<MetricsHistory> {
  return fetchApi<MetricsHistory>(`/metrics/history?minutes=${minutes}`)
}
