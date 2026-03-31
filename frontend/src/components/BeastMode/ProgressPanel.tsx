import type { CrawlProgress } from './types'
import { STEP_ICONS, formatTime, estimateRemaining } from './constants'
import { useI18n } from '../../i18n'

interface Props {
  crawlProgress: CrawlProgress
  cancelCrawl: () => void
}

export default function ProgressPanel({ crawlProgress, cancelCrawl }: Props) {
  const { lang } = useI18n()
  const steps = crawlProgress.steps || {}
  const stepKeys = Object.keys(steps).sort()
  const totalSteps = stepKeys.length || 5
  const overallPercent = totalSteps > 0
    ? Math.round(stepKeys.reduce((sum, k) => sum + (steps[k]?.percent || 0), 0) / totalSteps)
    : 0
  const gridClass = totalSteps <= 2 ? 'grid-cols-2' : totalSteps <= 3 ? 'grid-cols-3' : 'grid-cols-5'

  return (
    <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-[var(--color-border)] flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-[var(--color-accent-blue)]/15 flex items-center justify-center">
            <div className="w-4 h-4 border-2 border-[var(--color-accent-blue)]/30 border-t-[var(--color-accent-blue)] rounded-full animate-spin" />
          </div>
          <div>
            <h3 className="font-semibold text-sm">{lang === 'ja' ? 'データクロール中' : 'Đang crawl dữ liệu'}</h3>
            <p className="text-xs text-gray-500 mt-0.5">{crawlProgress.message}</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={cancelCrawl}
            className="px-3 py-1.5 rounded-lg bg-[var(--color-accent-red)]/15 text-[var(--color-accent-red)] text-xs font-medium hover:bg-[var(--color-accent-red)]/25 transition-all"
          >
            ✕ {lang === 'ja' ? 'キャンセル' : 'Hủy'}
          </button>
          <div>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider">{lang === 'ja' ? '経過' : 'Đã chạy'}</p>
            <p className="text-sm font-mono font-semibold text-[var(--color-accent-cyan)]">
              {formatTime(crawlProgress.elapsed || 0)}
            </p>
          </div>
          <div>
            <p className="text-[10px] text-gray-500 uppercase tracking-wider">{lang === 'ja' ? '残り' : 'Còn lại'}</p>
            <p className="text-sm font-mono font-semibold text-[var(--color-accent-orange)]">
              {estimateRemaining(crawlProgress.elapsed || 0, overallPercent)}
            </p>
          </div>
        </div>
      </div>

      {/* Overall progress bar */}
      <div className="px-6 py-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-gray-500">{lang === 'ja' ? '全体の進捗' : 'Tổng tiến trình'}</span>
          <span className="text-sm font-bold text-[var(--color-accent-cyan)]">
            {overallPercent}%
          </span>
        </div>
        <div className="w-full h-2.5 bg-[var(--color-bg-secondary)] rounded-full overflow-hidden">
          <div
            className="h-full bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)] rounded-full transition-all duration-700 ease-out relative"
            style={{ width: `${overallPercent}%` }}
          >
            <div className="absolute inset-0 bg-white/20 animate-pulse rounded-full" />
          </div>
        </div>
      </div>

      {/* Steps timeline */}
      <div className="px-6 pb-5">
        <div className={`grid ${gridClass} gap-2`}>
          {stepKeys.map((key) => {
            const step = steps[key]
            const isCurrent = step.status === 'running'
            const isDone = step.status === 'done'
            const isPending = step.status === 'pending'
            const stepPct = step.percent || 0
            const stepDetail = step.total > 0
              ? `${step.processed.toLocaleString()} / ${step.total.toLocaleString()}`
              : ''

            return (
              <div
                key={key}
                className={`relative rounded-lg p-3 transition-all duration-300 ${
                  isCurrent
                    ? 'bg-[var(--color-accent-blue)]/10 border border-[var(--color-accent-blue)]/30 shadow-lg shadow-[var(--color-accent-blue)]/5'
                    : isDone
                      ? 'bg-[var(--color-accent-green)]/5 border border-[var(--color-accent-green)]/20'
                      : 'bg-[var(--color-bg-secondary)]/50 border border-transparent'
                }`}
              >
                <div className="flex items-center gap-2 mb-2">
                  <span className={`text-base ${isPending ? 'grayscale opacity-40' : ''}`}>
                    {isDone ? '✅' : (STEP_ICONS[Number(key) - 1] || '⏳')}
                  </span>
                  <span className={`text-[10px] font-semibold uppercase tracking-wider ${
                    isCurrent ? 'text-[var(--color-accent-blue)]'
                    : isDone ? 'text-[var(--color-accent-green)]'
                    : 'text-gray-600'
                  }`}>
                    {step.name}
                  </span>
                </div>

                <div className="w-full h-1.5 bg-[var(--color-bg-primary)] rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all duration-500 ${
                      isDone
                        ? 'bg-[var(--color-accent-green)]'
                        : isCurrent
                          ? 'bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)]'
                          : 'bg-gray-700'
                    }`}
                    style={{ width: `${stepPct}%` }}
                  />
                </div>

                <div className="mt-1.5 h-4">
                  {isCurrent && (
                    <p className="text-[10px] text-gray-500 font-mono">
                      {stepDetail || `${stepPct}%`}
                    </p>
                  )}
                  {isDone && (
                    <p className="text-[10px] text-[var(--color-accent-green)]">{lang === 'ja' ? '完了 ✓' : 'Xong ✓'}</p>
                  )}
                </div>

                {isCurrent && (
                  <div className="absolute top-2 right-2">
                    <div className="w-2 h-2 rounded-full bg-[var(--color-accent-blue)] animate-pulse" />
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
