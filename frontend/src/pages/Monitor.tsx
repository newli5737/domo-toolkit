import {
  Activity, Play, AlertTriangle, CheckCircle, Clock, Database, GitBranch,
  RefreshCw, Filter, Loader, Download, Search, FileDown,
} from 'lucide-react'
import { ChevronDown, ExternalLink } from 'lucide-react'
import { monitorService } from '../services/monitor.service'
import type { DatasetRow, DataflowRow } from '../services/monitor.service'
import { useMonitor } from '../hooks/useMonitor'
import { useI18n } from '../i18n'

export default function Monitor() {
  const { t, lang } = useI18n()
  const {
    tab, setTab, domoBase,
    crawl, filters, updateFilters, providerTypes,
    datasets, dataflows, dsTotal, dfTotal,
    dsSort, dfSort, handleDsSort, handleDfSort,
    triggerCheck, crawlDs, crawlDf, refresh,
    loadDatasets, loadDataflows,
  } = useMonitor()

  // ─── Sort icons ────────────────────────────────────

  const DsSortIcon = ({ field }: { field: keyof DatasetRow }) => (
    <span className="ml-0.5 text-[10px]">
      {dsSort.sortBy === field ? (dsSort.sortOrder === 'ASC' ? '▲' : '▼') : <span className="text-slate-300">⇅</span>}
    </span>
  )
  const DfSortIcon = ({ field }: { field: keyof DataflowRow }) => (
    <span className="ml-0.5 text-[10px]">
      {dfSort.sortBy === field ? (dfSort.sortOrder === 'ASC' ? '▲' : '▼') : <span className="text-slate-300">⇅</span>}
    </span>
  )

  // ─── Helpers ───────────────────────────────────────

  const getStatusBadge = (status: string) => {
    const upper = (status || '').toUpperCase()
    if (upper.includes('FAILED') || upper === 'ERROR')
      return <span className="badge badge-failed"><AlertTriangle className="w-3 h-3" /> {status}</span>
    switch (status) {
      case 'stale': return <span className="badge badge-stale"><Clock className="w-3 h-3" /> {t('common.stale')}</span>
      case 'no_update': return <span className="badge badge-gray"><Clock className="w-3 h-3" /> {t('common.noUpdate')}</span>
      case 'SUCCESS': return <span className="badge badge-success"><CheckCircle className="w-3 h-3" /> {t('common.ok')}</span>
      default: return <span className="badge badge-info">{status || '-'}</span>
    }
  }

  const fmtTime = (s: string | null | number) => {
    if (!s) return '-'
    try {
      const d = typeof s === 'number' ? new Date(s) : new Date(s)
      if (isNaN(d.getTime())) return String(s)
      const tz = lang === 'ja' ? 'Asia/Tokyo' : 'Asia/Ho_Chi_Minh'
      const locale = lang === 'ja' ? 'ja-JP' : 'vi-VN'
      return d.toLocaleString(locale, {
        timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
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
      return d.toLocaleString(locale, { timeZone: tz, month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
    } catch { return String(s) }
  }

  const tabs: { key: typeof tab; label: string }[] = [
    { key: 'overview', label: t('monitor.tab.overview') },
    { key: 'datasets', label: `${t('monitor.tab.datasets')} (${dsTotal})` },
    { key: 'dataflows', label: `${t('monitor.tab.dataflows')} (${dfTotal})` },
  ]

  const { result } = crawl

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
            <button onClick={refresh} className="btn btn-outline">
              <RefreshCw className="w-4 h-4" /> {t('common.refresh')}
            </button>
            <button onClick={crawlDs} disabled={crawl.loading} className="btn btn-outline">
              {crawl.crawlType === 'datasets' ? <Loader className="w-4 h-4 animate-spin" /> : <Database className="w-4 h-4" />}
              {t('monitor.crawlDatasets')}
            </button>
            <button onClick={crawlDf} disabled={crawl.loading} className="btn btn-outline">
              {crawl.crawlType === 'dataflows' ? <Loader className="w-4 h-4 animate-spin" /> : <GitBranch className="w-4 h-4" />}
              {t('monitor.crawlDataflows')}
            </button>
            <button onClick={triggerCheck} disabled={crawl.loading} className="btn btn-primary">
              {crawl.crawlType === 'health' ? <Loader className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              {crawl.loading ? t('common.running') : t('monitor.runHealthCheck')}
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
        {crawl.error && (
          <div className="mb-4 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm">{crawl.error}</div>
        )}

        {crawl.crawling && (
          <div className="card mb-6">
            <div className="card-body flex flex-col items-center justify-center gap-3 py-10">
              <div className="spinner" />
              <span className="text-slate-500 text-sm">
                {crawl.crawlType === 'datasets' ? t('monitor.crawlDatasets') : crawl.crawlType === 'dataflows' ? t('monitor.crawlDataflows') : t('monitor.crawling')}
                ... {lang === 'vi' ? 'Đang xử lý...' : '処理中...'}
                {crawl.progress && (
                  <div className="text-xs text-blue-400 mt-1">
                    {crawl.progress.step} ({crawl.progress.percent}%)
                  </div>
                )}
              </span>
              {crawl.progress && (
                <div className="w-full bg-blue-100 rounded-full h-2 mt-3 overflow-hidden">
                  <div className="bg-blue-500 h-2 rounded-full transition-all duration-500" style={{ width: `${crawl.progress.percent}%` }} />
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
                    <input type="number" value={filters.staleHours} onChange={e => updateFilters({ staleHours: Number(e.target.value) })}
                      className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100" />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('monitor.minCardCount')}</label>
                    <input type="number" value={filters.minCardCount} onChange={e => updateFilters({ minCardCount: Number(e.target.value) })}
                      className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100" />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('monitor.providerType')}</label>
                    <div className="relative">
                      <select value={filters.providerType} onChange={e => updateFilters({ providerType: e.target.value })}
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
            {result && !crawl.crawling && (
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
                              <td className="text-slate-500 text-sm">{fmtTime(alert.last_updated || alert.last_execution_time || null)}</td>
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

            {!result && !crawl.crawling && (
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
                <div className="relative">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
                  <input type="text" value={filters.dsSearch}
                    onChange={e => updateFilters({ dsSearch: e.target.value })}
                    placeholder={lang === 'vi' ? 'Tìm theo tên...' : '名前で検索...'}
                    className="pl-8 pr-3 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 w-48" />
                </div>
                <div className="relative">
                  <select value={filters.dsFilterType} onChange={e => updateFilters({ dsFilterType: e.target.value })}
                    className="px-3 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 appearance-none bg-white pr-7"
                    style={{minWidth: 140}}>
                    <option value="">{lang === 'vi' ? 'Tất cả loại' : 'すべての種類'}</option>
                    {providerTypes.map(pt => (<option key={pt} value={pt}>{pt}</option>))}
                  </select>
                  <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400 pointer-events-none" />
                </div>
                <div className="flex items-center gap-1.5">
                  <select value={filters.dsFilterCardDir} onChange={e => updateFilters({ dsFilterCardDir: e.target.value })}
                    className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 bg-white" style={{minWidth: 80}}>
                    <option value="">Cards</option>
                    <option value="gte">≥</option>
                    <option value="lt">&lt;</option>
                  </select>
                  {filters.dsFilterCardDir && (
                    <input type="number" value={filters.dsFilterCardVal} onChange={e => updateFilters({ dsFilterCardVal: Number(e.target.value) })}
                      className="w-16 px-2 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" min={0} />
                  )}
                </div>
                <button onClick={() => {
                  const params = new URLSearchParams()
                  if (filters.dsSearch.trim()) params.set('search', filters.dsSearch.trim())
                  if (filters.dsFilterType) params.set('provider_type', filters.dsFilterType)
                  if (filters.dsFilterCardDir === 'gte') params.set('min_card_count', String(filters.dsFilterCardVal))
                  monitorService.exportDatasetsCsv(params.toString())
                }} className="btn btn-outline" style={{padding:'6px 12px'}} title="Export CSV">
                  <FileDown className="w-3.5 h-3.5" /> CSV
                </button>
                <button onClick={crawlDs} disabled={crawl.loading} className="btn btn-primary" style={{padding:'6px 12px'}}>
                  {crawl.crawlType === 'datasets' ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
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
                  <th className="w-12 text-center">#</th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('name')}>
                    {t('common.name')}<DsSortIcon field="name" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('dataset_status')}>
                    {t('monitor.datasetStatus')}<DsSortIcon field="dataset_status" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('last_execution_state')}>
                    {t('monitor.lastExecState')}<DsSortIcon field="last_execution_state" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('row_count')}>
                    {t('monitor.rows')}<DsSortIcon field="row_count" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('card_count')}>
                    {t('monitor.cards')}<DsSortIcon field="card_count" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('provider_type')}>
                    {t('common.type')}<DsSortIcon field="provider_type" />
                  </th>
                  <th className="cursor-pointer hover:text-blue-500 select-none" onClick={() => handleDsSort('last_updated')}>
                    {t('common.lastUpdated')}<DsSortIcon field="last_updated" />
                  </th>
                  <th></th>
                </tr></thead>
                <tbody>
                  {datasets.length === 0 && (
                    <tr><td colSpan={9} className="text-center text-slate-400 py-8">{t('monitor.runHealthCheck')}</td></tr>
                  )}
                  {datasets
                    .filter(ds => !filters.dsSearch.trim() || ds.name?.toLowerCase().includes(filters.dsSearch.trim().toLowerCase()))
                    .filter(ds => !filters.dsFilterType || ds.provider_type === filters.dsFilterType)
                    .filter(ds => {
                      if (!filters.dsFilterCardDir) return true
                      const c = ds.card_count || 0
                      return filters.dsFilterCardDir === 'gte' ? c >= filters.dsFilterCardVal : c < filters.dsFilterCardVal
                    })
                    .sort((a, b) => {
                      const statusPri = (s: string) => s.toUpperCase().includes('FAILED') || ['ERROR'].includes(s) ? 0 : ['stale'].includes(s) ? 1 : 2
                      const aPri = statusPri(a.last_execution_state || '')
                      const bPri = statusPri(b.last_execution_state || '')
                      if (aPri !== bPri) return aPri - bPri
                      const av = a[dsSort.sortBy] ?? ''
                      const bv = b[dsSort.sortBy] ?? ''
                      const cmp = typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv))
                      return dsSort.sortOrder === 'ASC' ? cmp : -cmp
                    })
                    .map((ds, idx) => (
                    <tr key={ds.id}>
                      <td className="text-center text-slate-400 text-xs">{idx + 1}</td>
                      <td className="font-medium max-w-[300px] truncate">{ds.name}</td>
                      <td>{ds.dataset_status ? <span className="badge badge-gray">{ds.dataset_status}</span> : <span className="text-slate-300">-</span>}</td>
                      <td>{ds.last_execution_state ? getStatusBadge(ds.last_execution_state) : <span className="text-slate-300">-</span>}</td>
                      <td>{ds.row_count?.toLocaleString() || '-'}</td>
                      <td>{ds.card_count || '-'}</td>
                      <td><span className="badge badge-gray">{ds.provider_type || '-'}</span></td>
                      <td className="text-sm">
                        <div className="text-slate-700">{fmtTime(ds.last_updated)}</div>
                        {ds.updated_at && <div className="text-slate-400 text-xs mt-0.5" title={lang === 'vi' ? 'Thời gian cào' : 'クロール時刻'}>⟳ {fmtTimeShort(ds.updated_at)}</div>}
                      </td>
                      <td>
                        <a href={`${domoBase}/datasources/${ds.id}/details/overview`} target="_blank" rel="noopener noreferrer"
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
              <div className="flex items-center gap-3">
                <div className="relative">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
                  <input type="text" value={filters.dfSearch}
                    onChange={e => updateFilters({ dfSearch: e.target.value })}
                    placeholder={lang === 'vi' ? 'Tìm theo tên...' : '名前で検索...'}
                    className="pl-8 pr-3 py-1.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 w-48" />
                </div>
                <button onClick={() => {
                  const params = new URLSearchParams()
                  if (filters.dfSearch.trim()) params.set('search', filters.dfSearch.trim())
                  monitorService.exportDataflowsCsv(params.toString())
                }} className="btn btn-outline" style={{padding:'6px 12px'}} title="Export CSV">
                  <FileDown className="w-3.5 h-3.5" /> CSV
                </button>
                <button onClick={crawlDf} disabled={crawl.loading} className="btn btn-primary" style={{padding:'6px 12px'}}>
                  {crawl.crawlType === 'dataflows' ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
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
                  <th className="w-12 text-center">#</th>
                  <th>{t('common.name')}</th>
                  <th className="cursor-pointer hover:text-purple-500 select-none" onClick={() => handleDfSort('status')}>
                    {t('monitor.datasetStatus')}<DfSortIcon field="status" />
                  </th>
                  <th className="cursor-pointer hover:text-purple-500 select-none" onClick={() => handleDfSort('last_execution_state')}>
                    {t('monitor.lastExecState')}<DfSortIcon field="last_execution_state" />
                  </th>
                  <th className="cursor-pointer hover:text-purple-500 select-none" onClick={() => handleDfSort('last_execution_time')}>
                    {t('monitor.lastExec')}<DfSortIcon field="last_execution_time" />
                  </th>
                  <th>{t('monitor.executions')}</th><th>{t('monitor.owner')}</th><th></th>
                </tr></thead>
                <tbody>
                  {dataflows.length === 0 && (
                    <tr><td colSpan={8} className="text-center text-slate-400 py-8">{t('monitor.runHealthCheck')}</td></tr>
                  )}
                  {dataflows
                    .filter(df => !filters.dfSearch.trim() || df.name?.toLowerCase().includes(filters.dfSearch.trim().toLowerCase()))
                    .sort((a, b) => {
                      const statusPri = (s: string) => ['FAILED','ERROR','failed'].includes(s) ? 0 : ['stale'].includes(s) ? 1 : 2
                      const aPri = statusPri(a.last_execution_state || '')
                      const bPri = statusPri(b.last_execution_state || '')
                      if (aPri !== bPri) return aPri - bPri
                      const av = a[dfSort.sortBy] ?? ''
                      const bv = b[dfSort.sortBy] ?? ''
                      const cmp = typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv))
                      return dfSort.sortOrder === 'ASC' ? cmp : -cmp
                    })
                    .map((df, idx) => (
                    <tr key={df.id}>
                      <td className="text-center text-slate-400 text-xs">{idx + 1}</td>
                      <td className="font-medium max-w-[300px] truncate">{df.name}</td>
                      <td>
                        {df.status ? <span className="badge badge-gray">{df.status}</span> : <span className="text-slate-300">-</span>}
                        {df.paused && <span className="badge badge-gray ml-1">{t('monitor.paused')}</span>}
                      </td>
                      <td>
                        {df.last_execution_state ? getStatusBadge(df.last_execution_state) : <span className="text-slate-300">-</span>}
                      </td>
                      <td className="text-sm">
                        <div className="text-slate-700">{fmtTime(df.last_execution_time)}</div>
                        {df.updated_at && <div className="text-slate-400 text-xs mt-0.5" title={lang === 'vi' ? 'Thời gian cào' : 'クロール時刻'}>⟳ {fmtTimeShort(df.updated_at)}</div>}
                      </td>
                      <td>{df.execution_count || '-'}</td>
                      <td className="text-slate-500 text-sm truncate max-w-[150px]">{df.owner || '-'}</td>
                      <td>
                        <a href={`${domoBase}/datacenter/dataflows/${df.id}/details#settings`} target="_blank" rel="noopener noreferrer"
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
