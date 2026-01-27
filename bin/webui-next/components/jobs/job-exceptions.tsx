'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight, AlertTriangle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useJobExceptions } from '@/lib/hooks/useApi'
import { formatTimestamp } from '@/lib/utils'

interface JobExceptionsProps {
  jobId: string
}

export function JobExceptions({ jobId }: JobExceptionsProps) {
  const { data, isLoading, error } = useJobExceptions(jobId)
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())

  const toggleExpand = (index: number) => {
    setExpandedIds((prev) => {
      const next = new Set(prev)
      if (next.has(index)) {
        next.delete(index)
      } else {
        next.add(index)
      }
      return next
    })
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Exceptions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[...Array(3)].map((_, i) => (
              <Skeleton key={i} className="h-20 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Exceptions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-destructive">Failed to load exceptions</div>
        </CardContent>
      </Card>
    )
  }

  const exceptions = data?.exceptions ?? []

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertTriangle className="h-5 w-5 text-destructive" />
          Exceptions ({exceptions.length})
        </CardTitle>
      </CardHeader>
      <CardContent>
        {exceptions.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No exceptions</div>
        ) : (
          <div className="space-y-3">
            {exceptions.map((exception, index) => {
              const isExpanded = expandedIds.has(index)
              return (
                <div
                  key={index}
                  className="border rounded-lg overflow-hidden"
                >
                  <button
                    className="w-full px-4 py-3 flex items-start gap-3 text-left hover:bg-muted/50 transition-colors"
                    onClick={() => toggleExpand(index)}
                  >
                    {isExpanded ? (
                      <ChevronDown className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    ) : (
                      <ChevronRight className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    )}
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-destructive truncate">
                        {exception.message}
                      </div>
                      <div className="text-xs text-muted-foreground mt-1 flex gap-4">
                        <span>Task: {exception.task_id}</span>
                        <span>Operator: {exception.operator_name}</span>
                        <span>{formatTimestamp(exception.timestamp)}</span>
                      </div>
                    </div>
                  </button>
                  {isExpanded && (
                    <div className="px-4 pb-4 pt-2 border-t bg-muted/30">
                      {exception.root_cause && (
                        <div className="mb-3">
                          <div className="text-xs font-medium text-muted-foreground mb-1">
                            Root Cause
                          </div>
                          <div className="text-sm text-destructive">
                            {exception.root_cause}
                          </div>
                        </div>
                      )}
                      <div>
                        <div className="text-xs font-medium text-muted-foreground mb-1">
                          Stack Trace
                        </div>
                        <pre className="text-xs font-mono bg-background p-3 rounded overflow-x-auto max-h-64">
                          {exception.stack_trace}
                        </pre>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
            {data?.truncated && (
              <div className="text-center text-sm text-muted-foreground">
                Some exceptions have been truncated
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
