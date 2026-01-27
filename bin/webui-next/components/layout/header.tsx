'use client'

import { usePathname } from 'next/navigation'
import Link from 'next/link'
import { ChevronRight, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useSettings } from '@/lib/store'
import { cn } from '@/lib/utils'

function getBreadcrumbs(pathname: string) {
  const segments = pathname.split('/').filter(Boolean)
  const breadcrumbs: { label: string; href: string }[] = []

  let currentPath = ''
  for (const segment of segments) {
    currentPath += `/${segment}`

    // Convert segment to label
    let label = segment.charAt(0).toUpperCase() + segment.slice(1)

    // Keep full ID for job IDs and worker IDs
    if (segment.match(/^[a-f0-9-]+$/i) && segment.length > 8) {
      label = segment
    }

    breadcrumbs.push({ label, href: currentPath })
  }

  return breadcrumbs
}

export function Header() {
  const pathname = usePathname()
  const breadcrumbs = getBreadcrumbs(pathname)
  const { autoRefresh, toggleAutoRefresh } = useSettings()

  return (
    <header className="sticky top-0 z-10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 border-b">
      <div className="flex items-center justify-between h-14 px-6">
        <nav className="flex items-center gap-1 text-sm">
          {breadcrumbs.map((crumb, index) => (
            <span key={crumb.href} className="flex items-center gap-1">
              {index > 0 && <ChevronRight className="h-4 w-4 text-muted-foreground" />}
              {index === breadcrumbs.length - 1 ? (
                <span className="font-medium">{crumb.label}</span>
              ) : (
                <Link
                  href={crumb.href}
                  className="text-muted-foreground hover:text-foreground transition-colors"
                >
                  {crumb.label}
                </Link>
              )}
            </span>
          ))}
        </nav>

        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={toggleAutoRefresh}
            className={cn('gap-2', autoRefresh && 'text-green-500')}
          >
            <RefreshCw className={cn('h-4 w-4', autoRefresh && 'animate-spin')} />
            {autoRefresh ? 'Auto-refresh On' : 'Auto-refresh Off'}
          </Button>
        </div>
      </div>
    </header>
  )
}
