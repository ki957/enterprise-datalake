import React from 'react'

export default function SkeletonBubble() {
  return (
    <div className="flex items-start gap-3 px-4 md:px-6 py-3">
      <div className="w-7 h-7 rounded-full bg-elevated animate-pulse shrink-0" />
      <div className="flex-1 max-w-xl space-y-2 pt-1">
        <div className="h-3 bg-elevated rounded animate-pulse w-3/4" />
        <div className="h-3 bg-elevated rounded animate-pulse w-full" />
        <div className="h-3 bg-elevated rounded animate-pulse w-5/6" />
        <div className="h-3 bg-elevated rounded animate-pulse w-2/3" />
      </div>
    </div>
  )
}
