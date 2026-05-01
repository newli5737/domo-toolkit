import { useState, useEffect, useCallback } from 'react'
import {
  Play, RefreshCw, CheckCircle2, XCircle, Clock, Database,
  ChevronLeft, ChevronRight, Search, Filter, Download,
  TrendingUp, BarChart3, PieChart, Layers, Loader2
} from 'lucide-react'
import { apiGet, apiPost, apiDownload } from '../api'

interface ModelStep {
  name: string
  duration_ms: number
  row_count: number | null
  error: string | null
}

interface PipelineStatus {
  status: 'idle' | 'running' | 'success' | 'failed'
  dataflow_id?: string
  started_at?: number
  finished_at?: number
  duration_ms?: number
  output_row_count?: number
  reference_date?: string
  error?: string
  models?: ModelStep[]
}

interface SummaryData {
  exists: boolean
  total_rows: number
  categories: { name: string; count: number }[]
  erawan_top: { name: string; count: number }[]
  statuses: { name: string; count: number }[]
  monthly: { year: number; month: number; count: number; revenue: number }[]
  budget: { category: string; month: number; target: number; cumulative: number }[]
}

interface PipelineData {
  columns: string[]
  data: Record<string, unknown>[]
  total_rows: number
  page: number
  page_size: number
}

const STATUS_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  idle: { bg: '#f1f5f9', text: '#64748b', border: '#e2e8f0' },
  running: { bg: '#dbeafe', text: '#2563eb', border: '#93c5fd' },
  success: { bg: '#dcfce7', text: '#15803d', border: '#86efac' },
  failed: { bg: '#fee2e2', text: '#dc2626', border: '#fca5a5' },
}

const STATUS_ICONS: Record<string, typeof CheckCircle2> = {
  idle: Clock,
  running: RefreshCw,
  success: CheckCircle2,
  failed: XCircle,
}

export default function PipelineManager() {
  const [status, setStatus] = useState<PipelineStatus>({ status: 'idle' })
  const [summary, setSummary] = useState<SummaryData | null>(null)
  const [tableData, setTableData] = useState<PipelineData | null>(null)
  const [refDate, setRefDate] = useState('')
  const [page, setPage] = useState(1)
  const [category, setCategory] = useState('')
  const [search, setSearch] = useState('')
  const [activeTab, setActiveTab] = useState<'overview' | 'data' | 'pipeline'>('overview')
  const [loading, setLoading] = useState(false)

  // Fetch pipeline status
  const fetchStatus = useCallback(async () => {
    try {
      const data = await apiGet<PipelineStatus>('/api/pipeline/status')
      setStatus(data)
    } catch { /* ignore */ }
  }, [])

  // Fetch summary
  const fetchSummary = useCallback(async () => {
    try {
      const data = await apiGet<SummaryData>('/api/pipeline/summary')
      setSummary(data)
    } catch { /* ignore */ }
  }, [])

  // Fetch table data
  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({ page: String(page), page_size: '50' })
      if (category) params.set('category', category)
      if (search) params.set('search', search)
      const data = await apiGet<PipelineData>(`/api/pipeline/data?${params}`)
      setTableData(data)
    } catch { /* ignore */ }
    setLoading(false)
  }, [page, category, search])

  useEffect(() => { fetchStatus(); fetchSummary() }, [])
  useEffect(() => {
    if (status.status === 'running') {
      const interval = setInterval(fetchStatus, 2000)
      return () => clearInterval(interval)
    }
    if (status.status === 'success') fetchSummary()
  }, [status.status])
  useEffect(() => { if (activeTab === 'data') fetchData() }, [activeTab, page, category, search])

  const runPipeline = async () => {
    try {
      await apiPost('/api/pipeline/run', {
        dataflow_id: '215',
        reference_date: refDate || null,
      })
      fetchStatus()
    } catch { /* ignore */ }
  }

  const StatusIcon = STATUS_ICONS[status.status] || Clock
  const statusColor = STATUS_COLORS[status.status] || STATUS_COLORS.idle

  return (
    <div className="animate-fadein">
      {/* Header */}
      <div className="page-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h1 style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <Database className="icon" style={{ color: '#3b82f6' }} />
              Pipeline Manager
            </h1>
            <p>Dataflow 215 — Backlog PTJ加盟店サイト関連</p>
          </div>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <input
              type="date"
              value={refDate}
              onChange={e => setRefDate(e.target.value)}
              className="btn-outline"
              style={{ padding: '7px 12px', fontSize: 13, borderRadius: 8, border: '1px solid #e2e8f0' }}
              title="Reference date (matches DOMO execution date)"
            />
            <button
              className="btn btn-primary"
              onClick={runPipeline}
              disabled={status.status === 'running'}
              style={{ minWidth: 130 }}
            >
              {status.status === 'running' ? (
                <><Loader2 className="icon" style={{ animation: 'spin 1s linear infinite' }} /> Running...</>
              ) : (
                <><Play className="icon" /> Run Pipeline</>
              )}
            </button>
          </div>
        </div>
      </div>

      <div className="page-body">
        {/* Status Bar */}
        <div className="card" style={{ marginBottom: 20, padding: '16px 20px', display: 'flex', alignItems: 'center', gap: 16, borderLeft: `4px solid ${statusColor.border}` }}>
          <div style={{ padding: '8px', borderRadius: 10, background: statusColor.bg }}>
            <StatusIcon style={{ width: 22, height: 22, color: statusColor.text, ...(status.status === 'running' ? { animation: 'spin 1s linear infinite' } : {}) }} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 700, color: statusColor.text, textTransform: 'capitalize' }}>{status.status}</div>
            <div style={{ fontSize: 12, color: '#64748b' }}>
              {status.duration_ms ? `${(status.duration_ms / 1000).toFixed(1)}s` : ''}
              {status.output_row_count ? ` · ${status.output_row_count.toLocaleString()} rows` : ''}
              {status.reference_date ? ` · ref: ${status.reference_date}` : ''}
            </div>
          </div>
          {status.error && <div style={{ fontSize: 12, color: '#dc2626', maxWidth: 400 }}>{status.error}</div>}
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', gap: 4, marginBottom: 20, background: '#f1f5f9', borderRadius: 10, padding: 4 }}>
          {([
            { key: 'overview', label: 'Overview', icon: PieChart },
            { key: 'data', label: 'Data Explorer', icon: Layers },
            { key: 'pipeline', label: 'Pipeline Steps', icon: BarChart3 },
          ] as const).map(tab => {
            const Icon = tab.icon
            return (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                style={{
                  flex: 1, padding: '10px 16px', borderRadius: 8, border: 'none', cursor: 'pointer',
                  fontSize: 13, fontWeight: 600, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
                  background: activeTab === tab.key ? '#fff' : 'transparent',
                  color: activeTab === tab.key ? '#0f172a' : '#64748b',
                  boxShadow: activeTab === tab.key ? '0 1px 3px rgba(0,0,0,0.08)' : 'none',
                  transition: 'all 0.15s ease',
                }}
              >
                <Icon style={{ width: 16, height: 16 }} /> {tab.label}
              </button>
            )
          })}
        </div>

        {/* Overview Tab */}
        {activeTab === 'overview' && summary?.exists && (
          <div className="animate-fadein">
            {/* Stat cards */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 24 }}>
              <StatCard label="Total Rows" value={summary.total_rows.toLocaleString()} color="#3b82f6" icon={Database} />
              <StatCard label="課題リスト" value={(summary.categories.find(c => c.name === '課題リスト')?.count ?? 0).toLocaleString()} color="#8b5cf6" icon={Layers} />
              <StatCard label="ウェイト別課題" value={(summary.categories.find(c => c.name === 'ウェイト別課題')?.count ?? 0).toLocaleString()} color="#06b6d4" icon={BarChart3} />
              <StatCard label="予算" value={(summary.categories.find(c => c.name === '予算')?.count ?? 0).toLocaleString()} color="#10b981" icon={TrendingUp} />
            </div>

            {/* Charts row */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>
              {/* Monthly Revenue Bar Chart */}
              <div className="card">
                <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <TrendingUp style={{ width: 16, height: 16, color: '#3b82f6' }} />
                  Monthly Revenue (税抜費用)
                </div>
                <div className="card-body">
                  <BarChartSimple data={summary.monthly.filter(m => m.revenue > 0).slice(0, 12)} />
                </div>
              </div>

              {/* Top ERAWAN codes */}
              <div className="card">
                <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <PieChart style={{ width: 16, height: 16, color: '#8b5cf6' }} />
                  Top ERAWANコード
                </div>
                <div className="card-body">
                  {summary.erawan_top.slice(0, 8).map((e, i) => (
                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                      <div style={{ flex: 1, fontSize: 13, color: '#0f172a', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.name || '(none)'}</div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: '#64748b' }}>{e.count.toLocaleString()}</div>
                      <div style={{ width: 80, height: 6, borderRadius: 3, background: '#f1f5f9', overflow: 'hidden' }}>
                        <div style={{ width: `${(e.count / (summary.erawan_top[0]?.count || 1)) * 100}%`, height: '100%', background: `hsl(${250 - i * 20}, 70%, 55%)`, borderRadius: 3 }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Budget table */}
            {summary.budget.length > 0 && (
              <div className="card">
                <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <BarChart3 style={{ width: 16, height: 16, color: '#10b981' }} />
                  予算 (Budget)
                </div>
                <div className="card-body table-wrapper">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>カテゴリ</th>
                        {Array.from(new Set(summary.budget.map(b => b.month))).sort((a, b) => a - b).map(m => (
                          <th key={m} style={{ textAlign: 'right' }}>{m}月</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {Array.from(new Set(summary.budget.map(b => b.category))).map(cat => (
                        <tr key={cat}>
                          <td style={{ fontWeight: 600, fontSize: 13 }}>{cat}</td>
                          {Array.from(new Set(summary.budget.map(b => b.month))).sort((a, b) => a - b).map(m => {
                            const val = summary.budget.find(b => b.category === cat && b.month === m)
                            return <td key={m} style={{ textAlign: 'right', fontSize: 13 }}>¥{(val?.target ?? 0).toLocaleString()}</td>
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'overview' && !summary?.exists && (
          <div style={{ textAlign: 'center', padding: 60, color: '#94a3b8' }}>
            <Database style={{ width: 48, height: 48, margin: '0 auto 16px', opacity: 0.3 }} />
            <p style={{ fontSize: 15, fontWeight: 600 }}>No pipeline data yet</p>
            <p style={{ fontSize: 13 }}>Run the pipeline to generate output data</p>
          </div>
        )}

        {/* Data Explorer Tab */}
        {activeTab === 'data' && (
          <div className="animate-fadein">
            {/* Filters */}
            <div style={{ display: 'flex', gap: 10, marginBottom: 16 }}>
              <div style={{ position: 'relative', flex: 1 }}>
                <Search style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', width: 16, height: 16, color: '#94a3b8' }} />
                <input
                  type="text"
                  placeholder="Search 課題タイトル..."
                  value={search}
                  onChange={e => { setSearch(e.target.value); setPage(1) }}
                  style={{ width: '100%', padding: '9px 12px 9px 36px', borderRadius: 8, border: '1px solid #e2e8f0', fontSize: 13, outline: 'none' }}
                />
              </div>
              <select
                value={category}
                onChange={e => { setCategory(e.target.value); setPage(1) }}
                style={{ padding: '9px 12px', borderRadius: 8, border: '1px solid #e2e8f0', fontSize: 13, minWidth: 160, cursor: 'pointer' }}
              >
                <option value="">All categories</option>
                <option value="課題リスト">課題リスト</option>
                <option value="ウェイト別課題">ウェイト別課題</option>
                <option value="予算">予算</option>
              </select>
            </div>

            {/* Table */}
            <div className="card">
              <div className="card-body table-wrapper" style={{ maxHeight: 500, overflowY: 'auto' }}>
                {loading ? (
                  <div style={{ textAlign: 'center', padding: 40 }}><div className="spinner" style={{ margin: '0 auto' }} /></div>
                ) : tableData && tableData.data.length > 0 ? (
                  <table className="data-table" style={{ fontSize: 12 }}>
                    <thead>
                      <tr>
                        {tableData.columns.slice(0, 12).map(col => (
                          <th key={col} style={{ whiteSpace: 'nowrap' }}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {tableData.data.map((row, i) => (
                        <tr key={i}>
                          {tableData.columns.slice(0, 12).map(col => (
                            <td key={col} style={{ whiteSpace: 'nowrap', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                              {String(row[col] ?? '')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div style={{ textAlign: 'center', padding: 40, color: '#94a3b8' }}>No data</div>
                )}
              </div>
              {/* Pagination */}
              {tableData && tableData.total_rows > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 20px', borderTop: '1px solid #f1f5f9', fontSize: 13 }}>
                  <span style={{ color: '#64748b' }}>
                    {((page - 1) * 50 + 1).toLocaleString()}–{Math.min(page * 50, tableData.total_rows).toLocaleString()} of {tableData.total_rows.toLocaleString()}
                  </span>
                  <div style={{ display: 'flex', gap: 4 }}>
                    <button className="btn btn-outline" onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1} style={{ padding: '6px 10px' }}>
                      <ChevronLeft style={{ width: 16, height: 16 }} />
                    </button>
                    <button className="btn btn-outline" onClick={() => setPage(p => p + 1)} disabled={page * 50 >= tableData.total_rows} style={{ padding: '6px 10px' }}>
                      <ChevronRight style={{ width: 16, height: 16 }} />
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Pipeline Steps Tab */}
        {activeTab === 'pipeline' && (
          <div className="animate-fadein">
            {status.models && status.models.length > 0 ? (
              <div className="card">
                <div className="card-body" style={{ padding: 0 }}>
                  {status.models.map((model, i) => {
                    const pct = model.duration_ms / Math.max(...status.models!.map(m => m.duration_ms)) * 100
                    const isError = !!model.error
                    return (
                      <div key={i} style={{
                        display: 'flex', alignItems: 'center', gap: 14, padding: '14px 20px',
                        borderBottom: i < status.models!.length - 1 ? '1px solid #f1f5f9' : 'none',
                      }}>
                        <div style={{
                          width: 28, height: 28, borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center',
                          background: isError ? '#fee2e2' : '#dcfce7', fontSize: 12, fontWeight: 800,
                          color: isError ? '#dc2626' : '#15803d',
                        }}>
                          {isError ? '✗' : '✓'}
                        </div>
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a' }}>{model.name}</div>
                          <div style={{ display: 'flex', gap: 12, fontSize: 11, color: '#64748b', marginTop: 2 }}>
                            <span>{model.duration_ms.toFixed(0)}ms</span>
                            {model.row_count != null && <span>{model.row_count.toLocaleString()} rows</span>}
                          </div>
                        </div>
                        <div style={{ width: 120, height: 6, borderRadius: 3, background: '#f1f5f9', overflow: 'hidden' }}>
                          <div style={{
                            width: `${pct}%`, height: '100%', borderRadius: 3,
                            background: isError ? '#ef4444' : `hsl(${210 - pct * 1.2}, 80%, 55%)`,
                            transition: 'width 0.5s ease',
                          }} />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: 60, color: '#94a3b8' }}>
                <BarChart3 style={{ width: 48, height: 48, margin: '0 auto 16px', opacity: 0.3 }} />
                <p style={{ fontSize: 15, fontWeight: 600 }}>No pipeline run data</p>
                <p style={{ fontSize: 13 }}>Run the pipeline to see execution steps</p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Simple Bar Chart Component ──
function BarChartSimple({ data }: { data: { year: number; month: number; revenue: number }[] }) {
  if (data.length === 0) return <div style={{ textAlign: 'center', padding: 20, color: '#94a3b8', fontSize: 13 }}>No revenue data</div>
  const max = Math.max(...data.map(d => d.revenue))
  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, height: 140 }}>
      {data.map((d, i) => {
        const h = max > 0 ? (d.revenue / max) * 120 : 0
        return (
          <div key={i} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
            <div style={{ fontSize: 9, color: '#94a3b8', fontWeight: 600 }}>
              ¥{(d.revenue / 1000000).toFixed(1)}M
            </div>
            <div style={{
              width: '100%', maxWidth: 28, height: h, borderRadius: '4px 4px 0 0',
              background: `linear-gradient(180deg, hsl(${210 + i * 5}, 80%, 55%), hsl(${210 + i * 5}, 70%, 45%))`,
              transition: 'height 0.5s ease',
            }} />
            <div style={{ fontSize: 10, color: '#64748b', fontWeight: 600 }}>{d.month}月</div>
          </div>
        )
      })}
    </div>
  )
}

// ── Stat Card Component ──
function StatCard({ label, value, color, icon: Icon }: { label: string; value: string; color: string; icon: typeof Database }) {
  return (
    <div className="stat-card">
      <div className="stat-icon" style={{ background: `${color}15` }}>
        <Icon style={{ width: 20, height: 20, color }} />
      </div>
      <div>
        <div className="stat-value">{value}</div>
        <div className="stat-label">{label}</div>
      </div>
    </div>
  )
}
