import { useState, useEffect } from 'react'
import {
  Activity,
  Play,
  AlertTriangle,
  CheckCircle,
  Clock,
  Database,
  GitBranch,
  RefreshCw,
  Filter,
  Loader,
  Download,
} from 'lucide-react'
import { apiPost, apiGet } from '../api'
import { useI18n } from '../i18n'
import { ChevronDown, ExternalLink } from 'lucide-react'

interface Alert {
  type: string
  status: string
  id: string
  name: string
  provider_type?: string
  database_type?: string
  card_count?: number
  last_updated?: string
  last_execution_time?: string
  last_execution_state?: string
  hours_ago?: number
}

interface CheckResult {
  summary: {
    datasets: { total_crawled: number; checked: number; ok: number; stale: number }
    dataflows: { total_crawled: number; checked: number; ok: number; failed: number; stale: number }
    total_alerts: number
  }
  alerts: Alert[]
  checked_at: string
}

interface DatasetRow {
  id: string; name: string; row_count: number; column_count: number;
  card_count: number; data_flow_count: number; provider_type: string;
  stream_id: string; schedule_state: string; last_execution_state: string;
  last_updated: string; updated_at: string;
}

interface DataflowRow {
  id: string; name: string; status: string; paused: boolean;
  database_type: string; last_execution_time: string; last_execution_state: string;
  execution_count: number; owner: string; output_dataset_count: number;
  updated_at: string;
}

type Tab = 'overview' | 'datasets' | 'dataflows'

export default function Monitor() {
  const { t, lang } = useI18n()
  const [loading, setLoading] = useState(false)
  const [crawling, setCrawling] = useState(false)
  const [crawlType, setCrawlType] = useState('')
  const [result, setResult] = useState<CheckResult | null>(null)
  const [error, setError] = useState('')
  const [tab, setTab] = useState<Tab>('overview')
  const [progress, setProgress] = useState<{ step: string; processed: number; total: number; percent: number } | null>(null)

  // DOMO instance for links
  const DOMO_BASE = 'https://astecpaints-co-jp.domo.com'

  // Filters
  const [staleHours, setStaleHours] = useState(24)
  const [minCardCount, setMinCardCount] = useState(0)
  const [providerType, setProviderType] = useState('')
  const [dsFilterType, setDsFilterType] = useState('')
  const [dsFilterCardDir, setDsFilterCardDir] = useState<string>('')  // '' | 'gte' | 'lt'
  const [dsFilterCardVal, setDsFilterCardVal] = useState<number>(40)  // Datasets tab filter

  // Provider types dropdown options
  const [providerTypes, setProviderTypes] = useState<string[]>([])

  // Dataset / Dataflow lists
  const [datasets, setDatasets] = useState<DatasetRow[]>([])
  const [dataflows, setDataflows] = useState<DataflowRow[]>([])
  const [dsTotal, setDsTotal] = useState(0)
  const [dfTotal, setDfTotal] = useState(0)

  const loadDatasets = () => {
    apiGet<{ total: number; datasets: DatasetRow[] }>('/api/monitor/datasets?limit=100')
      .then(d => {
        console.log('[DEBUG] Datasets loaded:', d.datasets?.length, 'first row:', d.datasets?.[0])
        console.log('[DEBUG] schedule_state values:', d.datasets?.slice(0, 5).map(ds => ({ name: ds.name, schedule_state: ds.schedule_state })))
        setDatasets(d.datasets || []); setDsTotal(d.total || 0)
      })
      .catch(() => {})
  }

  const loadDataflows = () => {
    apiGet<{ total: number; dataflows: DataflowRow[] }>('/api/monitor/dataflows?limit=100')
      .then(d => { setDataflows(d.dataflows || []); setDfTotal(d.total || 0) })
      .catch(() => {})
  }

  const loadProviderTypes = () => {
    apiGet<{ provider_types: string[] }>('/api/monitor/provider-types')
      .then(d => setProviderTypes(d.provider_types || []))
      .catch(() => {})
  }

  useEffect(() => {
    loadDatasets()
    loadDataflows()
    loadProviderTypes()
    checkStatus()
  }, [])

  const startPolling = (onLoad?: () => void) => {
    const poll = setInterval(async () => {
      try {
        const status = await apiGet<{ status: string; result?: CheckResult; progress?: typeof progress }>('/api/monitor/status')
        if (status.progress) setProgress(status.progress)
        if (status.status === 'completed') {
          if (status.result) setResult(status.result)
          setLoading(false); setCrawling(false); setCrawlType(''); setProgress(null)
          clearInterval(poll)
          loadDatasets(); loadDataflows(); loadProviderTypes()
          onLoad?.()
        }
      } catch { /* keep polling */ }
    }, 2000)
    setTimeout(() => { clearInterval(poll); setLoading(false); setCrawling(false); setCrawlType(''); setProgress(null) }, 300000)
  }

  const triggerCheck = async () => {
    setLoading(true); setError(''); setCrawlType('health')
    try {
      await apiPost(`/api/monitor/check?stale_hours=${staleHours}&min_card_count=${minCardCount}&provider_type=${providerType}&max_workers=10`)
      setCrawling(true); startPolling()
    } catch (err) { setError(err instanceof Error ? err.message : 'Error'); setLoading(false); setCrawlType('') }
  }

  const crawlDatasets = async () => {
    setLoading(true); setError(''); setCrawlType('datasets')
    try {
      await apiPost('/api/monitor/crawl/datasets?max_workers=10')
      setCrawling(true); startPolling()
    } catch (err) { setError(err instanceof Error ? err.message : 'Error'); setLoading(false); setCrawlType('') }
  }

  const crawlDataflows = async () => {
    setLoading(true); setError(''); setCrawlType('dataflows')
    try {
      await apiPost('/api/monitor/crawl/dataflows?max_workers=10')
      setCrawling(true); startPolling()
    } catch (err) { setError(err instanceof Error ? err.message : 'Error'); setLoading(false); setCrawlType('') }
  }

  const checkStatus = async () => {
    try {
      const status = await apiGet<{ status: string; result?: CheckResult }>('/api/monitor/status')
      if (status.status === 'running') { setCrawling(true); setLoading(true); startPolling() }
      if (status.result) setResult(status.result)
    } catch { /* ignore */ }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'failed': case 'FAILED': return <span className="badge badge-failed"><AlertTriangle className="w-3 h-3" /> {t('common.failed')}</span>
      case 'stale': return <span className="badge badge-stale"><Clock className="w-3 h-3" /> {t('common.stale')}</span>
      case 'no_update': return <span className="badge badge-gray"><Clock className="w-3 h-3" /> {t('common.noUpdate')}</span>
      case 'SUCCESS': return <span className="badge badge-success"><CheckCircle className="w-3 h-3" /> {t('common.ok')}</span>
      default: return <span className="badge badge-info">{status || '-'}</span>
    }
  }

  // Format timestamp localized to VN (UTC+7) or JP (UTC+9)
  const fmtTime = (s: string | null | number) => {
    if (!s) return '-'
    try {
      const d = typeof s === 'number' ? new Date(s) : new Date(s)
      if (isNaN(d.getTime())) return String(s)
      const tz = lang === 'ja' ? 'Asia/Tokyo' : 'Asia/Ho_Chi_Minh'
      const locale = lang === 'ja' ? 'ja-JP' : 'vi-VN'
      return d.toLocaleString(locale, {
        timeZone: tz,
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
      })
    } catch { return String(s) }
  }

  const fmtTimeShort = (s: string | null | number) => {
    if (!s) return '-'
    try {
      const d = typeof s === 'number' ? new Date(s) : new Date(s)
      if (isNaN(d.getTime())) return String(s)
      const tz = lang === 'ja' ? 'Asia/Tokyo' : 'Asia/Ho_Chi_Minh'
      const locale = lang === 'ja' ? 'ja-JP' : 'vi-VN'
      return d.toLocaleString(locale, {
        timeZone: tz,
        month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit',
      })
    } catch { return String(s) }
  }

  const getScheduleBadge = (state: string) => {
    if (!state) return <span className="badge badge-gray">-</span>
    switch (state.toUpperCase()) {
      case 'ACTIVE': return <span className="badge badge-success"><CheckCircle className="w-3 h-3" /> {lang === 'vi' ? 'Hoạt động' : 'アクティブ'}</span>
      case 'INACTIVE': return <span className="badge badge-gray"><Clock className="w-3 h-3" /> {lang === 'vi' ? 'Không hoạt động' : '非アクティブ'}</span>
      default: return <span className="badge badge-info">{state}</span>
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: 'overview', label: t('monitor.tab.overview') },
    { key: 'datasets', label: `${t('monitor.tab.datasets')} (${dsTotal})` },
    { key: 'dataflows', label: `${t('monitor.tab.dataflows')} (${dfTotal})` },
  ]

  return (
    <div className="animate-fadein">
      {/* Header */}
      <div className="page-header">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="flex items-center gap-2">
              <Activity className="w-6 h-6 text-blue-500" />
              {t('monitor.title')}
            </h1>
            <p>{t('monitor.desc')}</p>
          </div>
          <div className="flex gap-2">
            <button onClick={() => { loadDatasets(); loadDataflows(); checkStatus() }} className="btn btn-outline">
              <RefreshCw className="w-4 h-4" /> {t('common.refresh')}
            </button>
            <button onClick={crawlDatasets} disabled={loading} className="btn btn-outline">
              {crawlType === 'datasets' ? <Loader className="w-4 h-4 animate-spin" /> : <Database className="w-4 h-4" />}
              {t('monitor.crawlDatasets')}
            </button>
            <button onClick={crawlDataflows} disabled={loading} className="btn btn-outline">
              {crawlType === 'dataflows' ? <Loader className="w-4 h-4 animate-spin" /> : <GitBranch className="w-4 h-4" />}
              {t('monitor.crawlDataflows')}
            </button>
            <button onClick={triggerCheck} disabled={loading} className="btn btn-primary">
              {crawlType === 'health' ? <Loader className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              {loading ? t('common.running') : t('monitor.runHealthCheck')}
            </button>
          </div>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 mt-4 border-b border-slate-200 -mb-[21px]">
          {tabs.map(tb => (
            <button key={tb.key} onClick={() => setTab(tb.key)}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-all ${
                tab === tb.key ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-600'
              }`}>
              {tb.label}
            </button>
          ))}
        </div>
      </div>

      <div className="page-body">
        {error && (
          <div className="mb-4 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm">{error}</div>
        )}

        {crawling && (
          <div className="card mb-6">
            <div className="card-body flex flex-col items-center justify-center gap-3 py-10">
              <div className="spinner" />
              <span className="text-slate-500 text-sm">
                {crawlType === 'datasets' ? t('monitor.crawlDatasets') : crawlType === 'dataflows' ? t('monitor.crawlDataflows') : t('monitor.crawling')}
                ... {lang === 'vi' ? 'Đang xử lý...' : '処理中...'}
                {progress && (
                  <div className="text-xs text-blue-400 mt-1">
                    {progress.step} ({progress.percent}%)
                  </div>
                )}
              </span>
              {progress && (
                <div className="w-full bg-blue-100 rounded-full h-2 mt-3 overflow-hidden">
                  <div className="bg-blue-500 h-2 rounded-full transition-all duration-500" style={{ width: `${progress.percent}%` }} />
                </div>
              )}
            </div>
          </div>
        )}

        {/* ─── Tab: Overview ─── */}
        {tab === 'overview' && (
          <>
            {/* Filters */}
            <div className="card mb-6">
              <div className="card-header flex items-center gap-2">
                <Filter className="w-4 h-4 text-slate-400" /> {t('monitor.filters')}
              </div>
              <div className="card-body">
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('monitor.staleThreshold')}</label>
                    <input type="number" value={staleHours} onChange={e => setStaleHours(Number(e.target.value))}
                      className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100" />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('monitor.minCardCount')}</label>
                    <input type="number" value={minCardCount} onChange={e => setMinCardCount(Number(e.target.value))}
                      className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100" />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('monitor.providerType')}</label>
                    <div className="relative">
                      <select value={providerType} onChange={e => setProviderType(e.target.value)}
                        className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100 appearance-none bg-white pr-8">
                        <option value="">{lang === 'vi' ? 'Tất cả' : 'すべて'}</option>
                        {providerTypes.map(pt => (
                          <option key={pt} value={pt}>{pt}</option>
                        ))}
                      </select>
                      <ChevronDown className="absolute right-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Summary stats */}
            {result && !crawling && (
              <>
                <div className="grid grid-cols-4 gap-4 mb-6">
                  <div className="stat-card">
                    <div className="stat-icon bg-blue-50"><Database className="w-5 h-5 text-blue-500" /></div>
                    <div>
                      <div className="stat-value">{result.summary?.datasets?.total_crawled ?? '-'}</div>
                      <div className="stat-label">{t('monitor.datasetsCrawled')}</div>
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-icon bg-purple-50"><GitBranch className="w-5 h-5 text-purple-500" /></div>
                    <div>
                      <div className="stat-value">{result.summary?.dataflows?.total_crawled ?? '-'}</div>
                      <div className="stat-label">{t('monitor.dataflowsCrawled')}</div>
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-icon bg-green-50"><CheckCircle className="w-5 h-5 text-green-500" /></div>
                    <div>
                      <div className="stat-value">{(result.summary?.datasets?.ok ?? 0) + (result.summary?.dataflows?.ok ?? 0)}</div>
                      <div className="stat-label">{t('common.ok')}</div>
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-icon bg-red-50"><AlertTriangle className="w-5 h-5 text-red-500" /></div>
                    <div>
                      <div className="stat-value">{result.summary?.total_alerts ?? 0}</div>
                      <div className="stat-label">{t('monitor.alerts')}</div>
                    </div>
                  </div>
                </div>

                <div className="text-xs text-slate-400 mb-4 flex items-center gap-1">
                  <Clock className="w-3 h-3" /> {t('monitor.lastCheck')}: {fmtTime(result.checked_at)}
                </div>

                {(result.alerts?.length ?? 0) > 0 ? (
                  <div className="card">
                    <div className="card-header flex items-center gap-2">
                      <AlertTriangle className="w-4 h-4 text-orange-500" /> {t('monitor.alerts')} ({result.alerts.length})
                    </div>
                    <div className="table-wrapper">
                      <table className="data-table">
                        <thead><tr>
                          <th>{t('common.type')}</th><th>{t('common.status')}</th><th>{t('common.name')}</th>
                          <th>{t('common.details')}</th><th>{t('common.lastUpdated')}</th>
                        </tr></thead>
                        <tbody>
                          {result.alerts.map((alert, i) => (
                            <tr key={i}>
                              <td><span className="badge badge-info">
                                {alert.type === 'dataset' ? <Database className="w-3 h-3" /> : <GitBranch className="w-3 h-3" />} {alert.type}
                              </span></td>
                              <td>{getStatusBadge(alert.status)}</td>
                              <td className="font-medium max-w-[300px] truncate">{alert.name}</td>
                              <td className="text-slate-500 text-sm">
                                {alert.provider_type && `Type: ${alert.provider_type}`}
                                {alert.card_count != null && alert.card_count > 0 && ` | Cards: ${alert.card_count}`}
                                {alert.hours_ago != null && ` | ${alert.hours_ago}h ago`}
                              </td>
                              <td className="text-slate-500 text-sm">{fmtTime(alert.last_updated || alert.last_execution_time)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <div className="card">
                    <div className="card-body flex flex-col items-center justify-center py-12">
                      <CheckCircle className="w-12 h-12 text-green-400 mb-3" />
                      <div className="text-lg font-semibold text-slate-700">{t('monitor.allClear')}</div>
                      <div className="text-sm text-slate-400">{t('monitor.noAlerts')}</div>
                    </div>
                  </div>
                )}
              </>
            )}

            {!result && !crawling && (
              <div className="card">
                <div className="card-body flex flex-col items-center justify-center py-16 text-slate-400">
                  <Activity className="w-10 h-10 mb-3 text-slate-300" />
                  <div className="text-sm">{t('monitor.runHealthCheck')}</div>
                </div>
              </div>
            )}
          </>
        )}

        {/* ─── Tab: Datasets ─── */}
        {tab === 'datasets' && (
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <span className="flex items-center gap-2"><Database className="w-4 h-4 text-blue-500" /> {t('monitor.tab.datasets')} ({dsTotal})</span>
              <div className="flex items-center gap-3">
                {/* Provider type filter */}
                <div className="relative">
                  <select value={dsFilterType} onChange={e => setDsFilterType(e.target.value)}
                    className="px-3 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 appearance-none bg-white pr-7"
                    style={{minWidth: 140}}>
                    <option value="">{lang === 'vi' ? 'Tất cả loại' : 'すべての種類'}</option>
                    {providerTypes.map(pt => (
                      <option key={pt} value={pt}>{pt}</option>
                    ))}
                  </select>
                  <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400 pointer-events-none" />
                </div>
                {/* Card count filter */}
                <div className="flex items-center gap-1.5">
                  <select value={dsFilterCardDir} onChange={e => setDsFilterCardDir(e.target.value)}
                    className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 bg-white"
                    style={{minWidth: 80}}>
                    <option value="">Cards</option>
                    <option value="gte">≥</option>
                    <option value="lt">&lt;</option>
                  </select>
                  {dsFilterCardDir && (
                    <input type="number" value={dsFilterCardVal} onChange={e => setDsFilterCardVal(Number(e.target.value))}
                      className="w-16 px-2 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400"
                      min={0} />
                  )}
                </div>
                <button onClick={crawlDatasets} disabled={loading} className="btn btn-primary" style={{padding:'6px 12px'}}>
                  {crawlType === 'datasets' ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
                  {t('monitor.crawlDatasets')}
                </button>
                <button onClick={loadDatasets} className="btn btn-outline" style={{padding:'6px 12px'}}>
                  <RefreshCw className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>
            <div className="table-wrapper">
              <table className="data-table">
                <thead><tr>
                  <th>{t('common.name')}</th><th>{t('common.status')}</th><th>{t('monitor.rows')}</th><th>{t('monitor.cards')}</th>
                  <th>{t('common.type')}</th><th>{t('common.lastUpdated')}</th><th></th>
                </tr></thead>
                <tbody>
                  {datasets.length === 0 && (
                    <tr><td colSpan={7} className="text-center text-slate-400 py-8">{t('monitor.runHealthCheck')}</td></tr>
                  )}
                  {datasets
                    .filter(ds => !dsFilterType || ds.provider_type === dsFilterType)
                    .filter(ds => {
                      if (!dsFilterCardDir) return true
                      const c = ds.card_count || 0
                      return dsFilterCardDir === 'gte' ? c >= dsFilterCardVal : c < dsFilterCardVal
                    })
                    .map(ds => (
                    <tr key={ds.id}>
                      <td className="font-medium max-w-[300px] truncate">{ds.name}</td>
                      <td>{getStatusBadge(ds.last_execution_state || ds.schedule_state || '')}</td>
                      <td>{ds.row_count?.toLocaleString() || '-'}</td>
                      <td>{ds.card_count || '-'}</td>
                      <td><span className="badge badge-gray">{ds.provider_type || '-'}</span></td>
                      <td className="text-sm">
                        <div className="text-slate-700">{fmtTime(ds.last_updated)}</div>
                        {ds.updated_at && <div className="text-slate-400 text-xs mt-0.5" title={lang === 'vi' ? 'Thời gian cào' : 'クロール時刻'}>⟳ {fmtTimeShort(ds.updated_at)}</div>}
                      </td>
                      <td>
                        <a href={`${DOMO_BASE}/datasources/${ds.id}/details/overview`} target="_blank" rel="noopener noreferrer"
                          className="text-slate-400 hover:text-blue-500 transition-colors" title="Open in Domo">
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ─── Tab: Dataflows ─── */}
        {tab === 'dataflows' && (
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <span className="flex items-center gap-2"><GitBranch className="w-4 h-4 text-purple-500" /> {t('monitor.tab.dataflows')} ({dfTotal})</span>
              <div className="flex gap-2">
                <button onClick={crawlDataflows} disabled={loading} className="btn btn-primary" style={{padding:'6px 12px'}}>
                  {crawlType === 'dataflows' ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
                  {t('monitor.crawlDataflows')}
                </button>
                <button onClick={loadDataflows} className="btn btn-outline" style={{padding:'6px 12px'}}>
                  <RefreshCw className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>
            <div className="table-wrapper">
              <table className="data-table">
                <thead><tr>
                  <th>{t('common.name')}</th><th>{t('common.status')}</th><th>{t('monitor.lastExec')}</th>
                  <th>{t('monitor.executions')}</th><th>{t('monitor.owner')}</th><th></th>
                </tr></thead>
                <tbody>
                  {dataflows.length === 0 && (
                    <tr><td colSpan={6} className="text-center text-slate-400 py-8">{t('monitor.runHealthCheck')}</td></tr>
                  )}
                  {dataflows.map(df => (
                    <tr key={df.id}>
                      <td className="font-medium max-w-[300px] truncate">{df.name}</td>
                      <td>
                        {getStatusBadge(df.last_execution_state)}
                        {df.paused && <span className="badge badge-gray ml-1">{t('monitor.paused')}</span>}
                      </td>
                      <td className="text-sm">
                        <div className="text-slate-700">{fmtTime(df.last_execution_time)}</div>
                        {df.updated_at && <div className="text-slate-400 text-xs mt-0.5" title={lang === 'vi' ? 'Thời gian cào' : 'クロール時刻'}>⟳ {fmtTimeShort(df.updated_at)}</div>}
                      </td>
                      <td>{df.execution_count || '-'}</td>
                      <td className="text-slate-500 text-sm truncate max-w-[150px]">{df.owner || '-'}</td>
                      <td>
                        <a href={`${DOMO_BASE}/datacenter/dataflows/${df.id}/details#settings`} target="_blank" rel="noopener noreferrer"
                          className="text-slate-400 hover:text-purple-500 transition-colors" title="Open in Domo">
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
