'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import * as api from '@/lib/api/client'

// Cluster
export function useClusterInfo() {
  return useQuery({
    queryKey: ['cluster'],
    queryFn: api.getClusterInfo,
    refetchInterval: 5000,
  })
}

// Jobs
export function useJobs() {
  return useQuery({
    queryKey: ['jobs'],
    queryFn: api.listJobs,
    refetchInterval: 5000,
  })
}

export function useJob(jobId: string) {
  return useQuery({
    queryKey: ['job', jobId],
    queryFn: () => api.getJob(jobId),
    refetchInterval: 3000,
    enabled: !!jobId,
  })
}

export function useCancelJob() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (jobId: string) => api.cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
    },
  })
}

export function useJobTasks(jobId: string) {
  return useQuery({
    queryKey: ['job', jobId, 'tasks'],
    queryFn: () => api.getJobTasks(jobId),
    refetchInterval: 3000,
    enabled: !!jobId,
  })
}

export function useJobCheckpoints(jobId: string) {
  return useQuery({
    queryKey: ['job', jobId, 'checkpoints'],
    queryFn: () => api.getJobCheckpoints(jobId),
    refetchInterval: 5000,
    enabled: !!jobId,
  })
}

export function useJobGraph(jobId: string) {
  return useQuery({
    queryKey: ['job', jobId, 'graph'],
    queryFn: () => api.getJobGraph(jobId),
    refetchInterval: 5000,
    enabled: !!jobId,
  })
}

export function useJobExceptions(jobId: string) {
  return useQuery({
    queryKey: ['job', jobId, 'exceptions'],
    queryFn: () => api.getJobExceptions(jobId),
    refetchInterval: 5000,
    enabled: !!jobId,
  })
}

// Workers
export function useWorkers() {
  return useQuery({
    queryKey: ['workers'],
    queryFn: api.listWorkers,
    refetchInterval: 5000,
  })
}

// Metrics
export function useMetrics() {
  return useQuery({
    queryKey: ['metrics'],
    queryFn: api.getMetrics,
    refetchInterval: 5000,
  })
}

export function useMetricsHistory(minutes: number = 15) {
  return useQuery({
    queryKey: ['metrics', 'history', minutes],
    queryFn: () => api.getMetricsHistory(minutes),
    refetchInterval: 10000,
  })
}
