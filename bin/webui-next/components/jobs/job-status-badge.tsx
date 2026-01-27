import { Badge } from '@/components/ui/badge'
import type { JobState, TaskState } from '@/types'

interface JobStatusBadgeProps {
  state: JobState | TaskState | string
}

export function JobStatusBadge({ state }: JobStatusBadgeProps) {
  const getVariant = () => {
    switch (state) {
      case 'Running':
        return 'success'
      case 'Failed':
        return 'destructive'
      case 'Finished':
        return 'info'
      case 'Created':
      case 'Deploying':
        return 'warning'
      case 'Canceling':
      case 'Canceled':
        return 'secondary'
      default:
        return 'outline'
    }
  }

  return <Badge variant={getVariant()}>{state}</Badge>
}
