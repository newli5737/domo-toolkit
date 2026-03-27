import { useState, useEffect, useRef, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { apiGet, apiPost, apiDownload, apiDelete } from '../api'
import { useI18n } from '../i18n'

interface StepProgress {
  name: string
  status: string
  processed: number
  total: number
  percent: number
}

interface CrawlProgress {
  status: string
  job_id: number | null
  started_at: number | null
  elapsed: number
  message: string
  steps: Record<string, StepProgress>
}

interface GroupInfo {
  group_number: number
  group_label: string
  count: number
}

interface Summary {
  total: number
  groups: GroupInfo[]
  duplicates_exact: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_normalized: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_structure: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_names: { name: string; cnt: number; bm_ids: number[] }[]
  top_dirty_datasets: { dataset_id: string; url: string; total: number; unused: number; cleanup_candidates: number }[]
}

interface BmRow {
  bm_id: number
  bm_name: string
  legacy_id: string
  group_number: number
  group_label: string
  active_cards_count: number
  total_views: number
  referenced_by_count: number
  dataset_names: string
  naming_flag: string
  complexity_score: number
  url: string
}

const GROUP_CONFIG_VI = [
  { num: 1, label: 'Không sử dụng', color: 'red', icon: '🗑️' },
  { num: 2, label: 'Từng được dùng', color: 'orange', icon: '⏸️' },
  { num: 3, label: 'Card ít xem', color: 'cyan', icon: '👁️' },
  { num: 4, label: 'Đang hoạt động', color: 'green', icon: '✅' },
]
const GROUP_CONFIG_JA = [
  { num: 1, label: '未使用', color: 'red', icon: '🗑️' },
  { num: 2, label: '過去使用', color: 'orange', icon: '⏸️' },
  { num: 3, label: '低閲覧', color: 'cyan', icon: '👁️' },
  { num: 4, label: '稼働中', color: 'green', icon: '✅' },
]

const COLOR_MAP: Record<string, string> = {
  red: 'from-[var(--color-accent-red)] to-[var(--color-accent-orange)]',
  orange: 'from-[var(--color-accent-orange)] to-[var(--color-accent-yellow)]',
  cyan: 'from-[var(--color-accent-cyan)] to-[var(--color-accent-green)]',
  green: 'from-[var(--color-accent-green)] to-[var(--color-accent-cyan)]',
  blue: 'from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)]',
}

const TEXT_COLOR: Record<string, string> = {
  red: 'text-[var(--color-accent-red)]',
  orange: 'text-[var(--color-accent-orange)]',
  cyan: 'text-[var(--color-accent-cyan)]',
  green: 'text-[var(--color-accent-green)]',
  blue: 'text-[var(--color-accent-blue)]',
}

const BADGE_BG: Record<string, string> = {
  red: 'bg-[var(--color-accent-red)]/15 text-[var(--color-accent-red)]',
  orange: 'bg-[var(--color-accent-orange)]/15 text-[var(--color-accent-orange)]',
  cyan: 'bg-[var(--color-accent-cyan)]/15 text-[var(--color-accent-cyan)]',
  green: 'bg-[var(--color-accent-green)]/15 text-[var(--color-accent-green)]',
}

const STEP_ICONS = ['🔍', '📋', '🃏', '👁️', '📊']

function formatTime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  const m = Math.floor(seconds / 60)
  const s = seconds % 60
  return `${m}m ${s}s`
}

function estimateRemaining(elapsed: number, percent: number): string {
  if (percent <= 0 || elapsed <= 0) return '—'
  const totalEstimate = (elapsed / percent) * 100
  const remaining = Math.max(0, Math.round(totalEstimate - elapsed))
  if (remaining === 0) return '—'
  return `~${formatTime(remaining)}`
}

interface Props {
  readOnly?: boolean
}

export default function BeastModeCleanup({ readOnly = false }: Props) {
  const { lang } = useI18n()
  const GROUP_CONFIG = lang === 'ja' ? GROUP_CONFIG_JA : GROUP_CONFIG_VI
  const [crawlProgress, setCrawlProgress] = useState<CrawlProgress | null>(null)
  const [summary, setSummary] = useState<Summary | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = Number(searchParams.get('tab') || 1)
  const setActiveTab = (tab: number) => setSearchParams({ tab: String(tab) }, { replace: true })
  const [groupData, setGroupData] = useState<BmRow[]>([])
  const [loadingGroup, setLoadingGroup] = useState(false)
  const [crawling, setCrawling] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  // Search state
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<BmRow[] | null>(null)
  const [searching, setSearching] = useState(false)
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Delete state
  const [deleteTarget, setDeleteTarget] = useState<BmRow | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [deleteResult, setDeleteResult] = useState<{ success: boolean; message: string } | null>(null)

  // Pagination
  const [currentPage, setCurrentPage] = useState(1)
  const PAGE_SIZE = 50

  // WebSocket connection
  const connectWs = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const apiBase = import.meta.env.VITE_API_BASE || ''
    const host = apiBase ? new URL(apiBase).host : window.location.host
    const ws = new WebSocket(`${protocol}//${host}/api/beastmode/ws/progress`)
    wsRef.current = ws

    ws.onmessage = (event) => {
      try {
        const data: CrawlProgress = JSON.parse(event.data)
        setCrawlProgress(data)
        if (data.status === 'running') {
          setCrawling(true)
        } else if (data.status === 'done' || data.status === 'error' || data.status === 'cancelled') {
          setCrawling(false)
          if (data.status === 'done') loadSummary()
        }
      } catch { /* ignore */ }
    }

    ws.onclose = () => {
      // Reconnect after 3 seconds
      setTimeout(() => connectWs(), 3000)
    }
  }, [])

  useEffect(() => {
    connectWs()
    loadSummary()
    return () => {
      wsRef.current?.close()
    }
  }, [])

  // Load group data khi đổi tab (chỉ khi không search)
  useEffect(() => {
    if (!searchQuery) loadGroupData(activeTab)
    setCurrentPage(1)
  }, [activeTab, summary])

  const startCrawl = async () => {
    try {
      setCrawling(true)
      setSummary(null)
      setCrawlProgress(null)
      connectWs()
      await apiPost('/api/beastmode/crawl')
    } catch (err) {
      setCrawling(false)
      alert(err instanceof Error ? err.message : 'Lỗi')
    }
  }

  const startReanalyze = async () => {
    try {
      setCrawling(true)
      setSummary(null)
      setCrawlProgress(null)
      connectWs()
      await apiPost('/api/beastmode/crawl/reanalyze')
    } catch (err) {
      setCrawling(false)
      alert(err instanceof Error ? err.message : 'Lỗi')
    }
  }

  const cancelCrawl = async () => {
    try {
      await apiPost('/api/beastmode/crawl/cancel')
      setCrawling(false)
    } catch (err) {
      console.error('Cancel error:', err)
    }
  }

  const retryDetails = async () => {
    try {
      setCrawling(true)
      setSummary(null)
      setCrawlProgress(null)
      connectWs()
      await apiPost('/api/beastmode/crawl/retry-details')
    } catch (err) {
      setCrawling(false)
      alert(err instanceof Error ? err.message : 'Lỗi')
    }
  }

  const loadSummary = async () => {
    try {
      const data = await apiGet<Summary>('/api/beastmode/summary')
      setSummary(data)
    } catch {
      // ignore
    }
  }

  const loadGroupData = async (group: number) => {
    setLoadingGroup(true)
    try {
      const data = await apiGet<{ data: BmRow[] }>(`/api/beastmode/group/${group}?limit=5000`)
      setGroupData(data.data)
    } catch {
      setGroupData([])
    } finally {
      setLoadingGroup(false)
    }
  }

  const handleSearch = (value: string) => {
    setSearchQuery(value)
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)

    if (!value.trim()) {
      setSearchResults(null)
      loadGroupData(activeTab)
      return
    }

    searchTimerRef.current = setTimeout(async () => {
      setSearching(true)
      try {
        const data = await apiGet<{ data: BmRow[] }>(`/api/beastmode/search?q=${encodeURIComponent(value)}&limit=50`)
        setSearchResults(data.data)
      } catch {
        setSearchResults([])
      } finally {
        setSearching(false)
      }
    }, 400)
  }

  const handleDelete = async () => {
    if (!deleteTarget) return
    setDeleting(true)
    setDeleteResult(null)
    try {
      await apiDelete(`/api/beastmode/${deleteTarget.bm_id}`)
      setDeleteResult({ success: true, message: `Đã xóa BM "${deleteTarget.bm_name}" thành công!` })
      // Reload
      setTimeout(() => {
        if (searchQuery) handleSearch(searchQuery)
        else loadGroupData(activeTab)
        loadSummary()
        setDeleteTarget(null)
        setDeleteResult(null)
      }, 2000)
    } catch (err) {
      setDeleteResult({ success: false, message: err instanceof Error ? err.message : 'Xóa thất bại' })
    } finally {
      setDeleting(false)
    }
  }

  const getGroupCount = (num: number) => {
    if (!summary) return 0
    const g = summary.groups.find(g => g.group_number === num)
    return g?.count ?? 0
  }

  return (
    <div className="page-body">
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">{lang === 'ja' ? 'Beast Modeクリーンアップ' : 'Beast Mode Cleanup'}</h2>
          <p className="text-sm text-gray-400 mt-1">
            {readOnly
              ? (lang === 'ja' ? 'Beast Mode分析結果の閲覧' : 'Xem kết quả phân tích Beast Mode')
              : (lang === 'ja' ? '冗長なBeast Modeの分析・分類' : 'Phân tích và phân loại Beast Mode dư thừa')}
          </p>
        </div>
        {!readOnly && (
          <div className="flex gap-3">
            {summary && (
              <>
                <button
                  onClick={() => apiDownload(`/api/beastmode/export/csv?lang=${lang}`)}
                  className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-green)] to-[var(--color-accent-cyan)] text-[var(--color-bg-primary)] font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-green)]/20 hover:-translate-y-0.5"
                >
                  ⬇ Export CSV
                </button>
                <button
                  onClick={startReanalyze}
                  disabled={crawling}
                  className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-orange)] to-[var(--color-accent-yellow)] text-[var(--color-bg-primary)] font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-orange)]/20 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  🔄 Reanalyze
                </button>
                <button
                  onClick={retryDetails}
                  disabled={crawling}
                  className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-cyan)] to-[var(--color-accent-blue)] text-[var(--color-bg-primary)] font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-cyan)]/20 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  🔄 Retry Details
                </button>
              </>
            )}
            <button
              onClick={startCrawl}
              disabled={crawling}
              className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-purple)] text-white font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-blue)]/30 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:transform-none flex items-center gap-2"
            >
              {crawling ? (
                <>
                  <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  {lang === 'ja' ? 'クロール中...' : 'Đang crawl...'}
                </>
              ) : (
                lang === 'ja' ? '🔍 クロール開始' : '🔍 Bắt đầu Crawl'
              )}
            </button>
          </div>
        )}
      </div>

      {/* === CRAWL PROGRESS PANEL === */}
      {crawling && crawlProgress && (() => {
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

          {/* Steps timeline (per-step) */}
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
      })()}

      {/* Error */}
      {crawlProgress?.status === 'error' && (
        <div className="px-5 py-4 rounded-xl bg-red-500/10 border border-red-500/20 text-[var(--color-accent-red)] text-sm">
          ❌ {lang === 'ja' ? 'クロール失敗: ' : 'Crawl thất bại: '}{crawlProgress.message}
        </div>
      )}

      {/* No data yet */}
      {!summary && !crawling && (
        <div className="text-center py-20">
          <div className="text-6xl mb-5">🔮</div>
          <h3 className="text-xl font-semibold text-gray-300 mb-2">{lang === 'ja' ? 'データなし' : 'Chưa có dữ liệu'}</h3>
          <p className="text-sm text-gray-500 max-w-sm mx-auto leading-relaxed">
            {readOnly
              ? (lang === 'ja' ? '未クロール。管理者ページでクロールを開始してください。' : 'Dữ liệu chưa được crawl. Vui lòng truy cập trang Admin để bắt đầu crawl.')
              : (lang === 'ja' ? '「クロール開始」でDomoインスタンスの全Beast Modeをスキャン' : 'Nhấn "Bắt đầu Crawl" để quét toàn bộ Beast Mode trong Domo instance')}
          </p>
        </div>
      )}

      {/* Results */}
      {summary && (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
            {/* Tổng */}
            <div className="relative bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-6 overflow-hidden transition-all hover:-translate-y-0.5 hover:shadow-xl hover:shadow-black/20">
              <div className={`absolute top-0 inset-x-0 h-0.5 bg-gradient-to-r ${COLOR_MAP.blue}`} />
              <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500 mb-3">{lang === 'ja' ? 'Beast Mode合計' : 'Tổng Beast Mode'}</p>
              <p className={`text-3xl font-extrabold tracking-tight ${TEXT_COLOR.blue}`}>
                {summary.total.toLocaleString()}
              </p>
            </div>

            {/* 4 nhóm */}
            {GROUP_CONFIG.map(g => (
              <div
                key={g.num}
                className="relative bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-6 overflow-hidden transition-all hover:-translate-y-0.5 hover:shadow-xl hover:shadow-black/20 cursor-pointer"
                onClick={() => setActiveTab(g.num)}
              >
                <div className={`absolute top-0 inset-x-0 h-0.5 bg-gradient-to-r ${COLOR_MAP[g.color]}`} />
                <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500 mb-3">
                  {g.icon} {g.label}
                </p>
                <p className={`text-3xl font-extrabold tracking-tight ${TEXT_COLOR[g.color]}`}>
                  {getGroupCount(g.num).toLocaleString()}
                </p>
              </div>
            ))}
          </div>

          {/* Extra stats */}
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-purple)]/15 flex items-center justify-center text-lg">📋</div>
              <div>
                <p className="text-xs text-gray-500">{lang === 'ja' ? '重複BM (完全一致)' : 'Trùng expression'}</p>
                <p className="text-xl font-bold text-[var(--color-accent-purple)]">{(summary.duplicates_exact || []).length}</p>
              </div>
            </div>
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-yellow)]/15 flex items-center justify-center text-lg">🔄</div>
              <div>
                <p className="text-xs text-gray-500">{lang === 'ja' ? '構造重複' : 'Trùng cấu trúc'}</p>
                <p className="text-xl font-bold text-[var(--color-accent-yellow)]">{(summary.duplicates_structure || []).length}</p>
              </div>
            </div>
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-blue)]/15 flex items-center justify-center text-lg">📛</div>
              <div>
                <p className="text-xs text-gray-500">{lang === 'ja' ? '同名BM' : 'Trùng tên'}</p>
                <p className="text-xl font-bold text-[var(--color-accent-blue)]">{(summary.duplicates_names || []).length}</p>
              </div>
            </div>
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-red)]/15 flex items-center justify-center text-lg">📊</div>
              <div>
                <p className="text-xs text-gray-500">{lang === 'ja' ? 'クリーンアップ対象' : 'Dataset cần dọn'}</p>
                <p className="text-xl font-bold text-[var(--color-accent-red)]">{summary.top_dirty_datasets.length}</p>
              </div>
            </div>
          </div>

          {/* Search + Tabs + Table */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold">{searchResults ? (lang === 'ja' ? `検索結果 (${searchResults.length})` : `Kết quả tìm kiếm (${searchResults.length})`) : (lang === 'ja' ? 'グループ別詳細' : 'Chi tiết theo nhóm')}</h3>
            </div>

            {/* Search bar */}
            <div className="relative mb-4">
              <input
                type="text"
                value={searchQuery}
                onChange={e => handleSearch(e.target.value)}
                placeholder={lang === 'ja' ? '🔍 名前またはIDで検索...' : '🔍 Tìm theo tên hoặc ID...'}
                className="w-full px-5 py-3 rounded-xl bg-[var(--color-bg-card)] border border-[var(--color-border)] text-sm text-gray-200 placeholder:text-gray-600 focus:outline-none focus:border-[var(--color-accent-cyan)] focus:shadow-[0_0_0_2px] focus:shadow-[var(--color-accent-cyan)]/10 transition-all"
              />
              {searching && (
                <div className="absolute right-4 top-1/2 -translate-y-1/2">
                  <div className="w-4 h-4 border-2 border-white/20 border-t-[var(--color-accent-cyan)] rounded-full animate-spin" />
                </div>
              )}
              {searchQuery && !searching && (
                <button
                  onClick={() => handleSearch('')}
                  className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 text-sm"
                >
                  ✕
                </button>
              )}
            </div>

            {/* Tabs (ẩn khi đang search) */}
            {!searchResults && (
            <div className="flex gap-1 p-1 bg-[var(--color-bg-secondary)] rounded-lg w-fit mb-5">
              {GROUP_CONFIG.map(g => (
                <button
                  key={g.num}
                  onClick={() => setActiveTab(g.num)}
                  className={`px-5 py-2.5 rounded-md text-sm font-medium transition-all ${
                    activeTab === g.num
                      ? 'bg-[var(--color-accent-blue)] text-white shadow-md shadow-[var(--color-accent-blue)]/30'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {g.icon} {g.label} ({getGroupCount(g.num)})
                </button>
              ))}
            </div>
            )}

            {/* Table */}
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl overflow-hidden">
              {(loadingGroup || searching) ? (
                <div className="p-12 text-center">
                  <div className="w-6 h-6 border-2 border-white/20 border-t-[var(--color-accent-cyan)] rounded-full animate-spin mx-auto" />
                  <p className="text-sm text-gray-500 mt-3">{lang === 'ja' ? '読み込み中...' : 'Đang tải...'}</p>
                </div>
              ) : (searchResults ?? groupData).length === 0 ? (
                <div className="p-12 text-center text-gray-500 text-sm">
                  {searchResults !== null
                    ? (lang === 'ja' ? 'Beast Modeが見つかりません' : 'Không tìm thấy Beast Mode nào')
                    : (lang === 'ja' ? 'このグループにBeast Modeはありません' : 'Không có Beast Mode nào trong nhóm này')}
                </div>
              ) : (() => {
                const allData = searchResults ?? groupData
                const totalPages = Math.ceil(allData.length / PAGE_SIZE)
                const pageData = allData.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE)
                return (
                <>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-[var(--color-border)]">
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">ID</th>
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">Legacy ID</th>
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? '名前' : 'Tên'}</th>
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? 'グループ' : 'Nhóm'}</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Cards</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Views</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Refs</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Complexity</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pageData.map(bm => {
                        const gcfg = GROUP_CONFIG.find(g => g.num === bm.group_number)
                        const badgeColor = gcfg?.color ?? 'cyan'
                        return (
                          <tr key={bm.bm_id} className="border-b border-[var(--color-border)] last:border-b-0 hover:bg-white/[0.02] transition-colors">
                            <td className="px-4 py-3 text-xs font-mono text-gray-500">{bm.bm_id}</td>
                            <td className="px-4 py-3 text-xs font-mono text-gray-600 max-w-[120px] truncate" title={bm.legacy_id}>
                              {bm.legacy_id ? bm.legacy_id.replace('calculation_', '').slice(0, 8) + '…' : '—'}
                            </td>
                            <td className="px-4 py-3">
                              <a
                                href={bm.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-sm text-[var(--color-accent-cyan)] hover:underline"
                              >
                                {bm.bm_name}
                              </a>
                              {bm.dataset_names && (
                                <p className="text-[11px] text-gray-600 mt-0.5 truncate max-w-xs">{bm.dataset_names}</p>
                              )}
                            </td>
                            <td className="px-4 py-3">
                              <span className={`inline-flex px-2.5 py-0.5 rounded-full text-[10px] font-semibold ${BADGE_BG[badgeColor]}`}>
                                {bm.group_label}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.active_cards_count}</td>
                            <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.total_views.toLocaleString()}</td>
                            <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.referenced_by_count}</td>
                            <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.complexity_score}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
                {/* Pagination */}
                {totalPages > 1 && (
                  <div className="flex items-center justify-between px-4 py-3 border-t border-[var(--color-border)]">
                    <span className="text-xs text-gray-500">
                      {(currentPage - 1) * PAGE_SIZE + 1}–{Math.min(currentPage * PAGE_SIZE, allData.length)} / {allData.length}
                    </span>
                    <div className="flex gap-1">
                      <button
                        onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                        disabled={currentPage === 1}
                        className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-[var(--color-bg-secondary)] text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                      >←</button>
                      {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
                        let page: number
                        if (totalPages <= 7) page = i + 1
                        else if (currentPage <= 4) page = i + 1
                        else if (currentPage >= totalPages - 3) page = totalPages - 6 + i
                        else page = currentPage - 3 + i
                        return (
                          <button
                            key={page}
                            onClick={() => setCurrentPage(page)}
                            className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                              currentPage === page
                                ? 'bg-[var(--color-accent-blue)] text-white'
                                : 'bg-[var(--color-bg-secondary)] text-gray-400 hover:text-white'
                            }`}
                          >{page}</button>
                        )
                      })}
                      <button
                        onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                        disabled={currentPage === totalPages}
                        className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-[var(--color-bg-secondary)] text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                      >→</button>
                    </div>
                  </div>
                )}
                </>
                )
              })()}
            </div>
          </div>

          {/* Top dirty datasets */}
          {summary.top_dirty_datasets.length > 0 && (
            <div>
              <h3 className="text-lg font-bold mb-4">{lang === 'ja' ? '🏭 クリーンアップ対象DataSet' : '🏭 Dataset cần dọn dẹp nhất'}</h3>
              <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-6 space-y-3">
                {summary.top_dirty_datasets.map((ds, i) => {
                  const maxUnused = Math.max(...summary.top_dirty_datasets.map(d => d.unused), 1)
                  const pct = Math.round((ds.unused / maxUnused) * 100)
                  return (
                    <div key={i} className="flex items-center gap-3">
                      <a
                        href={ds.url || '#'}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="w-48 text-xs text-[var(--color-accent-cyan)] hover:underline text-right truncate flex-shrink-0 font-mono"
                        title={ds.dataset_id}
                      >
                        {ds.dataset_id || 'N/A'}
                      </a>
                      <div className="flex-1 h-6 bg-[var(--color-bg-secondary)] rounded overflow-hidden">
                        <div
                          className="h-full bg-gradient-to-r from-[var(--color-accent-red)] to-[var(--color-accent-orange)] rounded flex items-center pl-2 text-[10px] font-bold text-white transition-all duration-700"
                          style={{ width: `${Math.max(pct, 8)}%` }}
                        >
                          {ds.unused}
                        </div>
                      </div>
                      <span className="text-[10px] text-gray-500 w-16 text-right flex-shrink-0">
                        {ds.cleanup_candidates}/{ds.total} BM
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </>
      )}

      {/* Delete Confirmation Modal */}
      {deleteTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-2xl p-8 max-w-md w-full mx-4 shadow-2xl">
            <div className="text-center mb-6">
              <div className="text-4xl mb-3">⚠️</div>
              <h3 className="text-lg font-bold mb-2">{lang === 'ja' ? 'Beast Mode削除の確認' : 'Xác nhận xóa Beast Mode'}</h3>
              <p className="text-sm text-gray-400 leading-relaxed">
                {lang === 'ja' ? 'このBMを関連するすべてのカードから削除しますか？' : 'Bạn có chắc muốn xóa BM này khỏi tất cả cards liên kết?'}
              </p>
            </div>

            <div className="bg-[var(--color-bg-secondary)] rounded-lg p-4 mb-6 space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">ID:</span>
                <span className="font-mono font-semibold">{deleteTarget.bm_id}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Tên:</span>
                <span className="font-semibold text-[var(--color-accent-cyan)] truncate ml-4">{deleteTarget.bm_name}</span>
              </div>
              {deleteTarget.legacy_id && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Legacy ID:</span>
                  <span className="font-mono text-xs text-gray-400 truncate ml-4">{deleteTarget.legacy_id}</span>
                </div>
              )}
            </div>

            {deleteResult && (
              <div className={`mb-4 px-4 py-3 rounded-lg text-sm ${
                deleteResult.success
                  ? 'bg-[var(--color-accent-green)]/10 text-[var(--color-accent-green)]'
                  : 'bg-[var(--color-accent-red)]/10 text-[var(--color-accent-red)]'
              }`}>
                {deleteResult.success ? '✅' : '❌'} {deleteResult.message}
              </div>
            )}

            <div className="flex gap-3">
              <button
                onClick={() => { setDeleteTarget(null); setDeleteResult(null) }}
                disabled={deleting}
                className="flex-1 px-5 py-2.5 rounded-lg bg-[var(--color-bg-secondary)] text-gray-400 font-semibold text-sm hover:bg-white/10 transition-colors disabled:opacity-50"
              >
                {lang === 'ja' ? 'キャンセル' : 'Hủy'}
              </button>
              <button
                onClick={handleDelete}
                disabled={deleting || deleteResult?.success === true}
                className="flex-1 px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-red)] to-[var(--color-accent-orange)] text-white font-semibold text-sm hover:shadow-lg hover:shadow-[var(--color-accent-red)]/30 transition-all disabled:opacity-50 flex items-center justify-center gap-2"
              >
                {deleting ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    {lang === 'ja' ? '削除中...' : 'Đang xóa...'}
                  </>
                ) : (
                  lang === 'ja' ? '🗑️ 削除' : '🗑️ Xóa'
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
    </div>
  )
}
