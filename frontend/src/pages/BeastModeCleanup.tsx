import { useState, useEffect, useRef, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { apiGet, apiPost, apiDownload, apiDelete } from '../api'
import { useI18n } from '../i18n'

import type { CrawlProgress, Summary, BmRow } from '../components/BeastMode/types'
import { GROUP_CONFIG_JA, GROUP_CONFIG_VI } from '../components/BeastMode/constants'

import ProgressPanel from '../components/BeastMode/ProgressPanel'
import StatSummary from '../components/BeastMode/StatSummary'
import BeastModeTable from '../components/BeastMode/BeastModeTable'

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
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Search state
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<BmRow[] | null>(null)
  const [searching, setSearching] = useState(false)
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Delete state
  const [deleteTarget, setDeleteTarget] = useState<BmRow | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [deleteResult, setDeleteResult] = useState<{ success: boolean; message: string } | null>(null)

  const [lowViewThreshold, setLowViewThreshold] = useState(10)

  // WebSocket connection
  const connectWs = useCallback((retryCount = 0) => {
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
      wsRef.current = null
      // Exponential backoff reconnect logic, max 5 retries to prevent endless looping
      if (retryCount < 5) {
        const timeout = Math.min(3000 * Math.pow(1.5, retryCount), 15000)
        reconnectTimeoutRef.current = setTimeout(() => connectWs(retryCount + 1), timeout)
      }
    }
  }, [])

  useEffect(() => {
    connectWs()
    loadSummary()
    return () => {
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current)
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
      wsRef.current?.close()
    }
  }, [connectWs])

  // Load group data khi đổi tab (chỉ khi không search)
  useEffect(() => {
    if (!searchQuery) {
      loadGroupData(activeTab)
    }
  }, [activeTab, summary])

  const startCrawl = async () => {
    try {
      setCrawling(true)
      setSummary(null)
      setCrawlProgress(null)
      connectWs(0)
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
      connectWs(0)
      await apiPost('/api/beastmode/crawl/reanalyze', { low_view_threshold: lowViewThreshold })
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
      connectWs(0)
      await apiPost('/api/beastmode/crawl/retry-details')
    } catch (err) {
      setCrawling(false)
      alert(err instanceof Error ? err.message : 'Lỗi')
    }
  }

  const startBmOnly = async () => {
    try {
      setCrawling(true)
      setSummary(null)
      setCrawlProgress(null)
      connectWs(0)
      await apiPost('/api/beastmode/crawl/bm-only')
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
                    onClick={() => apiDownload(`/api/beastmode/export/csv?lang=${lang}&group=${activeTab}`)}
                    className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-green)] to-[var(--color-accent-cyan)] text-[var(--color-bg-primary)] font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-green)]/20 hover:-translate-y-0.5"
                    title={activeTab >= 1 && activeTab <= 4 ? (lang === 'ja' ? `グループ${activeTab}のみ出力` : `Chỉ xuất nhóm ${activeTab}`) : ''}
                  >
                    ⬇ Export CSV {activeTab >= 1 && activeTab <= 4 ? `(${GROUP_CONFIG.find(g => g.num === activeTab)?.label ?? activeTab})` : ''}
                  </button>
                  <div className="flex items-center gap-1 bg-[var(--color-bg-secondary)] border border-[var(--color-border)] rounded-lg px-2 py-1.5">
                    <label className="text-[10px] text-gray-500 whitespace-nowrap">
                      {lang === 'ja' ? '低閲覧閾値' : 'Ngưỡng ít xem'}
                    </label>
                    <input
                      type="number" min={1} max={10000}
                      value={lowViewThreshold}
                      onChange={e => setLowViewThreshold(Math.max(1, Number(e.target.value)))}
                      className="w-14 bg-transparent text-sm font-bold text-center outline-none text-[var(--color-accent-orange)]"
                    />
                    <span className="text-[10px] text-gray-500">views</span>
                  </div>
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
                  <button
                    onClick={startBmOnly}
                    disabled={crawling}
                    className="px-5 py-2.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-purple)] to-[var(--color-accent-blue)] text-white font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-purple)]/20 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    🔍 Crawl BM Only
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

        {/* CRAWL PROGRESS PANEL */}
        {crawling && crawlProgress && (
          <ProgressPanel 
            crawlProgress={crawlProgress} 
            cancelCrawl={cancelCrawl} 
          />
        )}

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
            <StatSummary 
              summary={summary} 
              setActiveTab={setActiveTab} 
            />

            <BeastModeTable 
              groupData={groupData}
              searchResults={searchResults}
              loadingGroup={loadingGroup}
              searching={searching}
              searchQuery={searchQuery}
              handleSearch={handleSearch}
              activeTab={activeTab}
              setActiveTab={setActiveTab}
              getGroupCount={getGroupCount}
              setDeleteTarget={setDeleteTarget}
            />

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
