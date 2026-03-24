import { useState, useEffect, useRef } from 'react'
import { apiGet, apiPost, apiDownload } from '../api'

interface CrawlStatus {
  job_id: number
  status: string
  total: number
  processed: number
  found: number
  errors: number
  message: string
  started_at: string | null
  finished_at: string | null
}

interface GroupInfo {
  group_number: number
  group_label: string
  count: number
}

interface Summary {
  total: number
  groups: GroupInfo[]
  duplicates: { duplicate_hash: string; cnt: number; bm_ids: number[] }[]
  naming_issues_count: number
  top_dirty_datasets: { dataset_names: string; total: number; unused: number; cleanup_candidates: number }[]
}

interface BmRow {
  bm_id: number
  bm_name: string
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

const GROUP_CONFIG = [
  { num: 1, label: 'Không sử dụng', color: 'red', icon: '🗑️' },
  { num: 2, label: 'Từng được dùng', color: 'orange', icon: '⏸️' },
  { num: 3, label: 'Card ít xem', color: 'cyan', icon: '👁️' },
  { num: 4, label: 'Đang hoạt động', color: 'green', icon: '✅' },
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

export default function BeastModeCleanup() {
  const [crawlStatus, setCrawlStatus] = useState<CrawlStatus | null>(null)
  const [summary, setSummary] = useState<Summary | null>(null)
  const [activeTab, setActiveTab] = useState(1)
  const [groupData, setGroupData] = useState<BmRow[]>([])
  const [loadingGroup, setLoadingGroup] = useState(false)
  const [crawling, setCrawling] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Kiểm tra trạng thái crawl khi mount
  useEffect(() => {
    checkStatus()
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [])

  // Load group data khi đổi tab
  useEffect(() => {
    if (summary) {
      loadGroupData(activeTab)
    }
  }, [activeTab, summary])

  const checkStatus = async () => {
    try {
      const status = await apiGet<CrawlStatus>('/api/beastmode/status')
      setCrawlStatus(status)
      if (status.status === 'running') {
        setCrawling(true)
        startPolling()
      } else if (status.status === 'done') {
        loadSummary()
      }
    } catch {
      // Chưa có crawl nào
    }
  }

  const startCrawl = async () => {
    try {
      setCrawling(true)
      await apiPost('/api/beastmode/crawl')
      startPolling()
    } catch (err) {
      setCrawling(false)
      alert(err instanceof Error ? err.message : 'Lỗi')
    }
  }

  const startPolling = () => {
    if (pollRef.current) clearInterval(pollRef.current)
    pollRef.current = setInterval(async () => {
      try {
        const status = await apiGet<CrawlStatus>('/api/beastmode/status')
        setCrawlStatus(status)
        if (status.status === 'done' || status.status === 'error') {
          if (pollRef.current) clearInterval(pollRef.current)
          setCrawling(false)
          if (status.status === 'done') {
            loadSummary()
          }
        }
      } catch {
        // ignore
      }
    }, 2000)
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
      const data = await apiGet<{ data: BmRow[] }>(`/api/beastmode/group/${group}?limit=200`)
      setGroupData(data.data)
    } catch {
      setGroupData([])
    } finally {
      setLoadingGroup(false)
    }
  }

  const progress = crawlStatus && crawlStatus.total > 0
    ? Math.round((crawlStatus.processed / crawlStatus.total) * 100)
    : 0

  const getGroupCount = (num: number) => {
    if (!summary) return 0
    const g = summary.groups.find(g => g.group_number === num)
    return g?.count ?? 0
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Beast Mode Cleanup</h2>
          <p className="text-sm text-gray-400 mt-1">Phân tích và phân loại Beast Mode dư thừa</p>
        </div>
        <div className="flex gap-3">
          {summary && (
            <button
              onClick={() => apiDownload('/api/beastmode/export/csv')}
              className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-green)] to-[var(--color-accent-cyan)] text-[var(--color-bg-primary)] font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-green)]/20 hover:-translate-y-0.5"
            >
              ⬇ Export CSV
            </button>
          )}
          <button
            onClick={startCrawl}
            disabled={crawling}
            className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-purple)] text-white font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-blue)]/30 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:transform-none flex items-center gap-2"
          >
            {crawling ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Đang crawl...
              </>
            ) : (
              '🔍 Bắt đầu Crawl'
            )}
          </button>
        </div>
      </div>

      {/* Progress */}
      {crawling && crawlStatus && (
        <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-8">
          <div className="text-center mb-4">
            <p className="text-sm text-gray-400">{crawlStatus.message}</p>
          </div>
          <div className="w-full h-2 bg-[var(--color-bg-secondary)] rounded-full overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)] rounded-full transition-all duration-500"
              style={{ width: `${progress}%` }}
            />
          </div>
          <div className="flex justify-between mt-3 text-xs text-gray-500">
            <span>{crawlStatus.processed.toLocaleString()} / {crawlStatus.total.toLocaleString()}</span>
            <span className="font-semibold text-[var(--color-accent-cyan)]">{progress}%</span>
          </div>
        </div>
      )}

      {/* Error */}
      {crawlStatus?.status === 'error' && (
        <div className="px-5 py-4 rounded-xl bg-red-500/10 border border-red-500/20 text-[var(--color-accent-red)] text-sm">
          ❌ Crawl thất bại: {crawlStatus.message}
        </div>
      )}

      {/* No data yet */}
      {!summary && !crawling && (
        <div className="text-center py-20">
          <div className="text-6xl mb-5">🔮</div>
          <h3 className="text-xl font-semibold text-gray-300 mb-2">Chưa có dữ liệu</h3>
          <p className="text-sm text-gray-500 max-w-sm mx-auto leading-relaxed">
            Nhấn "Bắt đầu Crawl" để quét toàn bộ Beast Mode trong Domo instance
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
              <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500 mb-3">Tổng Beast Mode</p>
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
                <p className="text-xs text-gray-500">BM trùng lặp</p>
                <p className="text-xl font-bold text-[var(--color-accent-purple)]">{summary.duplicates.length}</p>
              </div>
            </div>
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-yellow)]/15 flex items-center justify-center text-lg">⚠️</div>
              <div>
                <p className="text-xs text-gray-500">Tên có vấn đề</p>
                <p className="text-xl font-bold text-[var(--color-accent-yellow)]">{summary.naming_issues_count}</p>
              </div>
            </div>
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-5 flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-[var(--color-accent-red)]/15 flex items-center justify-center text-lg">📊</div>
              <div>
                <p className="text-xs text-gray-500">Dataset cần dọn</p>
                <p className="text-xl font-bold text-[var(--color-accent-red)]">{summary.top_dirty_datasets.length}</p>
              </div>
            </div>
          </div>

          {/* Tabs + Table */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold">Chi tiết theo nhóm</h3>
            </div>

            {/* Tabs */}
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

            {/* Table */}
            <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl overflow-hidden">
              {loadingGroup ? (
                <div className="p-12 text-center">
                  <div className="w-6 h-6 border-2 border-white/20 border-t-[var(--color-accent-cyan)] rounded-full animate-spin mx-auto" />
                  <p className="text-sm text-gray-500 mt-3">Đang tải...</p>
                </div>
              ) : groupData.length === 0 ? (
                <div className="p-12 text-center text-gray-500 text-sm">
                  Không có Beast Mode nào trong nhóm này
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-[var(--color-border)]">
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">Tên</th>
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">Nhóm</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Cards</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Views</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Refs</th>
                        <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">Flag</th>
                        <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Complexity</th>
                      </tr>
                    </thead>
                    <tbody>
                      {groupData.map(bm => {
                        const gcfg = GROUP_CONFIG.find(g => g.num === bm.group_number)
                        const badgeColor = gcfg?.color ?? 'cyan'
                        return (
                          <tr key={bm.bm_id} className="border-b border-[var(--color-border)] last:border-b-0 hover:bg-white/[0.02] transition-colors">
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
                            <td className="px-4 py-3">
                              {bm.naming_flag && (
                                <span className="inline-flex px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-[var(--color-accent-yellow)]/15 text-[var(--color-accent-yellow)]">
                                  {bm.naming_flag}
                                </span>
                              )}
                            </td>
                            <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.complexity_score}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          {/* Top dirty datasets */}
          {summary.top_dirty_datasets.length > 0 && (
            <div>
              <h3 className="text-lg font-bold mb-4">🏭 Dataset cần dọn dẹp nhất</h3>
              <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-xl p-6 space-y-3">
                {summary.top_dirty_datasets.map((ds, i) => {
                  const maxUnused = Math.max(...summary.top_dirty_datasets.map(d => d.unused), 1)
                  const pct = Math.round((ds.unused / maxUnused) * 100)
                  return (
                    <div key={i} className="flex items-center gap-3">
                      <span className="w-40 text-xs text-gray-400 text-right truncate flex-shrink-0">
                        {ds.dataset_names || 'N/A'}
                      </span>
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
    </div>
  )
}
