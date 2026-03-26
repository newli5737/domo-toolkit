import { useState, useEffect, useCallback } from 'react'
import { apiGet } from '../api'
import { useI18n } from '../i18n'
import { BarChart3, Eye, Search, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react'

interface Card {
  id: string
  title: string
  card_type: string
  view_count: number
  owner_name: string
  page_id: string | null
  page_title: string | null
  last_viewed_at: string | null
}

interface PagedResponse<T> {
  data: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

interface DashboardRow {
  page_id: string
  page_title: string
  card_count: number
  total_views: number
}

interface CardStats {
  total_cards: number
  total_dashboards: number
  total_views: number
  total_types: number
  zero_view_cards: number
  type_distribution: { card_type: string; count: number; views: number }[]
  top_dashboards: { page_title: string; card_count: number; total_views: number }[]
}

export default function CardDashboard() {
  const { lang } = useI18n()

  const [stats, setStats] = useState<CardStats | null>(null)
  const [types, setTypes] = useState<string[]>([])
  const [domoBase, setDomoBase] = useState('')
  const [activeTab, setActiveTab] = useState<'cards' | 'dashboards'>('cards')

  // ─── Cards state ───
  const [cards, setCards] = useState<Card[]>([])
  const [cTotal, setCTotal] = useState(0)
  const [cPage, setCPage] = useState(1)
  const [cTotalPages, setCTotalPages] = useState(0)
  const [cLoading, setCLoading] = useState(false)
  const [cSearch, setCSearch] = useState('')
  const [cFilterType, setCFilterType] = useState('')
  const [cFilterDash, setCFilterDash] = useState('')
  const [cFilterOwner, setCFilterOwner] = useState('')
  const [cSortBy, setCSortBy] = useState('view_count')
  const [cSortOrder, setCSortOrder] = useState('DESC')

  // ─── Dashboards state ───
  const [dashes, setDashes] = useState<DashboardRow[]>([])
  const [dTotal, setDTotal] = useState(0)
  const [dPage, setDPage] = useState(1)
  const [dTotalPages, setDTotalPages] = useState(0)
  const [dLoading, setDLoading] = useState(false)
  const [dSearch, setDSearch] = useState('')
  const [dSortBy, setDSortBy] = useState('total_views')
  const [dSortOrder, setDSortOrder] = useState('DESC')

  // Init
  useEffect(() => {
    apiGet<any>('/api/auth/status').then(d => {
      if (d?.instance) setDomoBase(`https://${d.instance}`)
    }).catch(() => {})
    apiGet<CardStats>('/api/cards/stats').then(setStats).catch(() => {})
    apiGet<string[]>('/api/cards/types').then(setTypes).catch(() => {})
  }, [])

  // Fetch cards
  const fetchCards = useCallback(() => {
    setCLoading(true)
    const p = new URLSearchParams({ page: String(cPage), page_size: '50', sort_by: cSortBy, sort_order: cSortOrder })
    if (cSearch) p.set('search', cSearch)
    if (cFilterType) p.set('card_type', cFilterType)
    if (cFilterDash) p.set('page_title', cFilterDash)
    if (cFilterOwner) p.set('owner', cFilterOwner)
    apiGet<PagedResponse<Card>>(`/api/cards/list?${p}`)
      .then(d => { setCards(d.data); setCTotal(d.total); setCTotalPages(d.total_pages) })
      .catch(() => {})
      .finally(() => setCLoading(false))
  }, [cPage, cSortBy, cSortOrder, cSearch, cFilterType, cFilterDash, cFilterOwner])

  useEffect(() => { fetchCards() }, [fetchCards])

  // Fetch dashboards
  const fetchDashboards = useCallback(() => {
    setDLoading(true)
    const p = new URLSearchParams({ page: String(dPage), page_size: '50', sort_by: dSortBy, sort_order: dSortOrder })
    if (dSearch) p.set('search', dSearch)
    apiGet<PagedResponse<DashboardRow>>(`/api/cards/dashboards?${p}`)
      .then(d => { setDashes(d.data); setDTotal(d.total); setDTotalPages(d.total_pages) })
      .catch(() => {})
      .finally(() => setDLoading(false))
  }, [dPage, dSortBy, dSortOrder, dSearch])

  useEffect(() => { if (activeTab === 'dashboards') fetchDashboards() }, [fetchDashboards, activeTab])

  // Sort handlers
  const handleCardSort = (field: string) => {
    if (cSortBy === field) setCSortOrder(o => o === 'ASC' ? 'DESC' : 'ASC')
    else { setCSortBy(field); setCSortOrder('DESC') }
    setCPage(1)
  }
  const handleDashSort = (field: string) => {
    if (dSortBy === field) setDSortOrder(o => o === 'ASC' ? 'DESC' : 'ASC')
    else { setDSortBy(field); setDSortOrder('DESC') }
    setDPage(1)
  }

  const SortIcon = ({ field, activeSort, activeOrder }: { field: string; activeSort: string; activeOrder: string }) => (
    <span className="text-[10px] ml-0.5 text-slate-400">
      {activeSort === field ? (activeOrder === 'ASC' ? '▲' : '▼') : ''}
    </span>
  )

  const formatNumber = (n: number) => n?.toLocaleString() ?? '0'

  // Pagination component
  const Pagination = ({ pg, total, totalPg, onPage }: { pg: number; total: number; totalPg: number; onPage: (p: number) => void }) => (
    totalPg > 1 ? (
      <div className="flex items-center justify-between pt-2">
        <span className="text-xs text-slate-400">
          {lang === 'vi' ? 'Trang' : 'ページ'} {pg}/{totalPg} ({formatNumber(total)})
        </span>
        <div className="flex gap-1">
          <button onClick={() => onPage(Math.max(1, pg - 1))} disabled={pg <= 1}
            className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-30">
            <ChevronLeft className="w-4 h-4" />
          </button>
          {Array.from({ length: Math.min(5, totalPg) }, (_, i) => {
            const start = Math.max(1, Math.min(pg - 2, totalPg - 4))
            const p = start + i
            if (p > totalPg) return null
            return (
              <button key={p} onClick={() => onPage(p)}
                className={`w-8 h-8 rounded-lg text-xs font-semibold ${
                  p === pg ? 'bg-blue-500 text-white' : 'border border-slate-200 hover:bg-slate-100'
                }`}>
                {p}
              </button>
            )
          })}
          <button onClick={() => onPage(Math.min(totalPg, pg + 1))} disabled={pg >= totalPg}
            className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-30">
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    ) : null
  )

  return (
    <div>
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <BarChart3 className="w-6 h-6 text-purple-500" />
          {lang === 'vi' ? 'Card & Dashboard' : 'カード＆ダッシュボード'}
        </h1>
        <p>{lang === 'vi' ? 'Thống kê lượt xem các Card và Dashboard' : 'カードとダッシュボードの閲覧統計'}</p>
      </div>

      <div className="page-body space-y-5 animate-fadein">
        {/* Stats overview */}
        {stats && (
          <div className="grid grid-cols-5 gap-3">
            {[
              { label: lang === 'vi' ? 'Tổng Card' : '合計カード', value: formatNumber(stats.total_cards), color: 'blue' },
              { label: lang === 'vi' ? 'Dashboard' : 'ダッシュボード', value: formatNumber(stats.total_dashboards), color: 'purple' },
              { label: lang === 'vi' ? 'Tổng lượt xem' : '合計閲覧数', value: formatNumber(stats.total_views), color: 'green' },
              { label: lang === 'vi' ? 'Loại Card' : 'カードタイプ', value: stats.total_types, color: 'orange' },
              { label: lang === 'vi' ? 'Card 0 view' : '0閲覧カード', value: formatNumber(stats.zero_view_cards), color: 'red' },
            ].map((s, i) => (
              <div key={i} className="card p-3">
                <div className="text-[11px] font-semibold text-slate-400 uppercase mb-1">{s.label}</div>
                <div className={`text-xl font-bold text-${s.color}-500`}>{s.value}</div>
              </div>
            ))}
          </div>
        )}

        {/* Tabs */}
        <div className="flex gap-2">
          <button onClick={() => setActiveTab('cards')}
            className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeTab === 'cards' ? 'bg-blue-500 text-white shadow-md' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
            }`}>
            📋 {lang === 'vi' ? 'Danh sách Card' : 'カード一覧'}
          </button>
          <button onClick={() => setActiveTab('dashboards')}
            className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeTab === 'dashboards' ? 'bg-purple-500 text-white shadow-md' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
            }`}>
            📊 {lang === 'vi' ? 'Dashboard' : 'ダッシュボード'}
          </button>
        </div>

        {/* ═══ Cards Tab ═══ */}
        {activeTab === 'cards' && (
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <span>{lang === 'vi' ? 'Danh sách Card' : 'カード一覧'} ({formatNumber(cTotal)})</span>
              <button onClick={() => { setCSearch(''); setCFilterType(''); setCFilterDash(''); setCFilterOwner(''); setCPage(1) }}
                className="text-xs text-blue-500 hover:underline">
                {lang === 'vi' ? 'Xóa bộ lọc' : 'フィルターリセット'}
              </button>
            </div>
            <div className="card-body space-y-3">
              <div className="grid grid-cols-4 gap-3">
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 w-3.5 h-3.5 text-slate-400" />
                  <input type="text" value={cSearch}
                    onChange={e => { setCSearch(e.target.value); setCPage(1) }}
                    placeholder={lang === 'vi' ? 'Tìm theo tên...' : '名前で検索...'}
                    className="w-full pl-8 pr-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
                </div>
                <select value={cFilterType} onChange={e => { setCFilterType(e.target.value); setCPage(1) }}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400">
                  <option value="">{lang === 'vi' ? 'Tất cả loại' : '全タイプ'}</option>
                  {types.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                <input type="text" value={cFilterDash}
                  onChange={e => { setCFilterDash(e.target.value); setCPage(1) }}
                  placeholder={lang === 'vi' ? 'Lọc dashboard...' : 'ダッシュボード...'}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
                <input type="text" value={cFilterOwner}
                  onChange={e => { setCFilterOwner(e.target.value); setCPage(1) }}
                  placeholder={lang === 'vi' ? 'Lọc owner...' : 'オーナー...'}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-200 text-left">
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleCardSort('title')}>
                        {lang === 'vi' ? 'Tên Card' : 'カード名'}<SortIcon field="title" activeSort={cSortBy} activeOrder={cSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleCardSort('card_type')}>
                        {lang === 'vi' ? 'Loại' : 'タイプ'}<SortIcon field="card_type" activeSort={cSortBy} activeOrder={cSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleCardSort('view_count')}>
                        <Eye className="w-3.5 h-3.5 inline mr-1" />
                        {lang === 'vi' ? 'Lượt xem' : '閲覧数'}<SortIcon field="view_count" activeSort={cSortBy} activeOrder={cSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleCardSort('page_title')}>
                        {lang === 'vi' ? 'Dashboard' : 'ダッシュボード'}<SortIcon field="page_title" activeSort={cSortBy} activeOrder={cSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleCardSort('owner_name')}>
                        Owner<SortIcon field="owner_name" activeSort={cSortBy} activeOrder={cSortOrder} />
                      </th>
                      <th className="p-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {cLoading ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">Loading...</td></tr>
                    ) : cards.length === 0 ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">
                        {lang === 'vi' ? 'Chưa có dữ liệu. Hãy crawl Beast Mode trước.' : 'データなし。先にBeast Modeをクロールしてください。'}
                      </td></tr>
                    ) : cards.map(c => (
                      <tr key={c.id} className="border-b border-slate-50 hover:bg-slate-50/50 transition-colors">
                        <td className="p-2">
                          <div className="font-medium text-slate-700 text-xs truncate max-w-[250px]" title={c.title}>{c.title || '—'}</div>
                        </td>
                        <td className="p-2"><span className="badge badge-info text-[10px]">{c.card_type || '—'}</span></td>
                        <td className="p-2">
                          <span className={`font-semibold text-xs ${c.view_count > 0 ? 'text-green-600' : 'text-slate-400'}`}>
                            {formatNumber(c.view_count)}
                          </span>
                        </td>
                        <td className="p-2">
                          <span className="text-xs text-slate-500 truncate max-w-[200px] block" title={c.page_title || ''}>{c.page_title || '—'}</span>
                        </td>
                        <td className="p-2 text-xs text-slate-500">{c.owner_name || '—'}</td>
                        <td className="p-2">
                          {domoBase && c.page_id && (
                            <a href={`${domoBase}/page/${c.page_id}`} target="_blank" rel="noopener noreferrer"
                              className="text-blue-500 hover:text-blue-700" title="Open in Domo">
                              <ExternalLink className="w-3.5 h-3.5" />
                            </a>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <Pagination pg={cPage} total={cTotal} totalPg={cTotalPages} onPage={setCPage} />
            </div>
          </div>
        )}

        {/* ═══ Dashboards Tab ═══ */}
        {activeTab === 'dashboards' && (
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <span>{lang === 'vi' ? 'Tất cả Dashboard' : '全ダッシュボード'} ({formatNumber(dTotal)})</span>
              <button onClick={() => { setDSearch(''); setDPage(1) }}
                className="text-xs text-blue-500 hover:underline">
                {lang === 'vi' ? 'Xóa bộ lọc' : 'フィルターリセット'}
              </button>
            </div>
            <div className="card-body space-y-3">
              <div className="relative w-72">
                <Search className="absolute left-2.5 top-2.5 w-3.5 h-3.5 text-slate-400" />
                <input type="text" value={dSearch}
                  onChange={e => { setDSearch(e.target.value); setDPage(1) }}
                  placeholder={lang === 'vi' ? 'Tìm dashboard...' : 'ダッシュボード検索...'}
                  className="w-full pl-8 pr-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-200 text-left">
                      <th className="p-2 w-10">#</th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleDashSort('page_title')}>
                        {lang === 'vi' ? 'Dashboard' : 'ダッシュボード'}
                        <SortIcon field="page_title" activeSort={dSortBy} activeOrder={dSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleDashSort('card_count')}>
                        {lang === 'vi' ? 'Số card' : 'カード数'}
                        <SortIcon field="card_count" activeSort={dSortBy} activeOrder={dSortOrder} />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleDashSort('total_views')}>
                        <Eye className="w-3.5 h-3.5 inline mr-1" />
                        {lang === 'vi' ? 'Tổng lượt xem' : '合計閲覧数'}
                        <SortIcon field="total_views" activeSort={dSortBy} activeOrder={dSortOrder} />
                      </th>
                      <th className="p-2 w-40">{lang === 'vi' ? 'Phân bổ' : '分布'}</th>
                      <th className="p-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {dLoading ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">Loading...</td></tr>
                    ) : dashes.length === 0 ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">
                        {lang === 'vi' ? 'Chưa có dữ liệu' : 'データなし'}
                      </td></tr>
                    ) : dashes.map((d, i) => {
                      const maxViews = dashes[0]?.total_views || 1
                      const pct = Math.round((d.total_views / maxViews) * 100)
                      const rank = (dPage - 1) * 50 + i + 1
                      return (
                        <tr key={d.page_id} className={`border-b border-slate-50 hover:bg-slate-50/50 transition-colors ${
                          d.total_views === 0 ? 'bg-red-50/30' : ''
                        }`}>
                          <td className="p-2 font-semibold text-slate-400 text-xs">{rank}</td>
                          <td className="p-2 font-medium text-slate-700 text-xs">{d.page_title}</td>
                          <td className="p-2 text-xs">{d.card_count}</td>
                          <td className="p-2">
                            <span className={`text-xs font-semibold ${d.total_views > 0 ? 'text-green-600' : 'text-red-400'}`}>
                              {formatNumber(d.total_views)}
                            </span>
                          </td>
                          <td className="p-2">
                            <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                              <div className="h-full bg-gradient-to-r from-purple-400 to-blue-400 rounded-full transition-all"
                                style={{ width: `${pct}%` }} />
                            </div>
                          </td>
                          <td className="p-2">
                            {domoBase && d.page_id && (
                              <a href={`${domoBase}/page/${d.page_id}`} target="_blank" rel="noopener noreferrer"
                                className="text-blue-500 hover:text-blue-700" title="Open in Domo">
                                <ExternalLink className="w-3.5 h-3.5" />
                              </a>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
              <Pagination pg={dPage} total={dTotal} totalPg={dTotalPages} onPage={setDPage} />
            </div>
          </div>
        )}

        {/* Type distribution */}
        {activeTab === 'dashboards' && stats && (
          <div className="card">
            <div className="card-header">{lang === 'vi' ? 'Phân bổ loại Card' : 'カードタイプ分布'}</div>
            <div className="card-body">
              <div className="grid grid-cols-2 gap-2">
                {stats.type_distribution.map((t, i) => (
                  <div key={i} className="flex items-center gap-3 p-2 rounded-lg bg-slate-50 border border-slate-100">
                    <span className="text-xs font-semibold text-slate-700 w-24 truncate">{t.card_type}</span>
                    <div className="flex-1 h-2 bg-slate-200 rounded-full overflow-hidden">
                      <div className="h-full bg-gradient-to-r from-blue-400 to-cyan-400 rounded-full"
                        style={{ width: `${Math.round((t.count / (stats.total_cards || 1)) * 100)}%` }} />
                    </div>
                    <span className="text-[10px] text-slate-400 w-16 text-right">{t.count} cards</span>
                    <span className="text-[10px] text-green-500 w-16 text-right">{formatNumber(t.views)} views</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
