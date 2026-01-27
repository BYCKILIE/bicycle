import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`
}

export function formatNumber(num: number): string {
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`
  if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K`
  return num.toLocaleString()
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ${minutes % 60}m`
}

export function formatTimestamp(timestamp: number): string {
  if (timestamp === 0) return '-'
  return new Date(timestamp).toLocaleString()
}

export function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const mins = Math.floor((seconds % 3600) / 60)

  if (days > 0) return `${days}d ${hours}h ${mins}m`
  if (hours > 0) return `${hours}h ${mins}m`
  return `${mins}m`
}

export function getJobStateColor(state: string): string {
  switch (state) {
    case 'Running':
      return 'bg-green-500'
    case 'Failed':
      return 'bg-red-500'
    case 'Finished':
      return 'bg-blue-500'
    case 'Created':
      return 'bg-yellow-500'
    case 'Canceling':
    case 'Canceled':
      return 'bg-gray-500'
    default:
      return 'bg-gray-400'
  }
}

export function getTaskStateColor(state: string): string {
  switch (state) {
    case 'Running':
      return 'bg-green-500'
    case 'Failed':
      return 'bg-red-500'
    case 'Finished':
      return 'bg-blue-500'
    case 'Created':
    case 'Deploying':
      return 'bg-yellow-500'
    case 'Canceling':
    case 'Canceled':
      return 'bg-gray-500'
    default:
      return 'bg-gray-400'
  }
}

export function getOperatorTypeColor(type: string): string {
  switch (type) {
    case 'Source':
      return '#22c55e' // green
    case 'Sink':
      return '#ef4444' // red
    case 'Map':
    case 'Filter':
      return '#3b82f6' // blue
    case 'KeyBy':
    case 'Window':
      return '#a855f7' // purple
    case 'Join':
      return '#f59e0b' // amber
    default:
      return '#6b7280' // gray
  }
}
