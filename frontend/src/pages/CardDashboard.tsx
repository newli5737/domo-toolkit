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

interface CardResponse {
  data: Card[]
  total: number
  page: number
  page_size: number
  total_pages: number
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

  const [cards, setCards] = useState<Card[]>([])
  const [stats, setStats] = useState<CardStats | null>(null)
  const [types, setTypes] = useState<string[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [totalPages, setTotalPages] = useState(0)
  const [loading, setLoading] = useState(false)
  const [domoBase, setDomoBase] = useState('')

  // Filters
  const [search, setSearch] = useState('')
  const [filterType, setFilterType] = useState('')
  const [filterDashboard, setFilterDashboard] = useState('')
  const [filterOwner, setFilterOwner] = useState('')
  const [sortBy, setSortBy] = useState('view_count')
  const [sortOrder, setSortOrder] = useState('DESC')

  const [activeTab, setActiveTab] = useState<'cards' | 'dashboards'>('cards')

  // Fetch Domo base URL
  useEffect(() => {
    apiGet<any>('/api/auth/status').then(d => {
      if (d?.instance) setDomoBase(`https://${d.instance}`)
    }).catch(() => {})
  }, [])

  // Fetch stats + types on mount
  useEffect(() => {
    apiGet<CardStats>('/api/cards/stats').then(setStats).catch(() => {})
    apiGet<string[]>('/api/cards/types').then(setTypes).catch(() => {})
  }, [])

  // Fetch cards
  const fetchCards = useCallback(() => {
    setLoading(true)
    const params = new URLSearchParams({
      page: String(page),
      page_size: '50',
      sort_by: sortBy,
      sort_order: sortOrder,
    })
    if (search) params.set('search', search)
    if (filterType) params.set('card_type', filterType)
    if (filterDashboard) params.set('page_title', filterDashboard)
    if (filterOwner) params.set('owner', filterOwner)

    apiGet<CardResponse>(`/api/cards/list?${params}`)
      .then(d => {
        setCards(d.data)
        setTotal(d.total)
        setTotalPages(d.total_pages)
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [page, sortBy, sortOrder, search, filterType, filterDashboard, filterOwner])

  useEffect(() => { fetchCards() }, [fetchCards])

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setSortOrder(o => o === 'ASC' ? 'DESC' : 'ASC')
    } else {
      setSortBy(field)
      setSortOrder('DESC')
    }
    setPage(1)
  }

  const SortIcon = ({ field }: { field: string }) => (
    <span className="text-[10px] ml-0.5 text-slate-400">
      {sortBy === field ? (sortOrder === 'ASC' ? '▲' : '▼') : ''}
    </span>
  )

  const resetFilters = () => {
    setSearch(''); setFilterType(''); setFilterDashboard(''); setFilterOwner('')
    setPage(1)
  }

  const formatNumber = (n: number) => n?.toLocaleString() ?? '0'

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
          <button
            onClick={() => setActiveTab('cards')}
            className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeTab === 'cards'
                ? 'bg-blue-500 text-white shadow-md'
                : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
            }`}
          >
            📋 {lang === 'vi' ? 'Danh sách Card' : 'カード一覧'}
          </button>
          <button
            onClick={() => setActiveTab('dashboards')}
            className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeTab === 'dashboards'
                ? 'bg-purple-500 text-white shadow-md'
                : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
            }`}
          >
            📊 {lang === 'vi' ? 'Top Dashboard' : 'トップダッシュボード'}
          </button>
        </div>

        {/* Cards Tab */}
        {activeTab === 'cards' && (
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <span>{lang === 'vi' ? 'Danh sách Card' : 'カード一覧'} ({formatNumber(total)})</span>
              <button onClick={resetFilters} className="text-xs text-blue-500 hover:underline">
                {lang === 'vi' ? 'Xóa bộ lọc' : 'フィルターリセット'}
              </button>
            </div>
            <div className="card-body space-y-3">
              {/* Filters */}
              <div className="grid grid-cols-4 gap-3">
                <div className="relative">
                  <Search className="absolute left-2.5 top-2.5 w-3.5 h-3.5 text-slate-400" />
                  <input
                    type="text" value={search}
                    onChange={e => { setSearch(e.target.value); setPage(1) }}
                    placeholder={lang === 'vi' ? 'Tìm theo tên...' : '名前で検索...'}
                    className="w-full pl-8 pr-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400"
                  />
                </div>
                <select
                  value={filterType}
                  onChange={e => { setFilterType(e.target.value); setPage(1) }}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400"
                >
                  <option value="">{lang === 'vi' ? 'Tất cả loại' : '全タイプ'}</option>
                  {types.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                <input
                  type="text" value={filterDashboard}
                  onChange={e => { setFilterDashboard(e.target.value); setPage(1) }}
                  placeholder={lang === 'vi' ? 'Lọc dashboard...' : 'ダッシュボード...'}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400"
                />
                <input
                  type="text" value={filterOwner}
                  onChange={e => { setFilterOwner(e.target.value); setPage(1) }}
                  placeholder={lang === 'vi' ? 'Lọc owner...' : 'オーナー...'}
                  className="px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400"
                />
              </div>

              {/* Table */}
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-200 text-left">
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleSort('title')}>
                        {lang === 'vi' ? 'Tên Card' : 'カード名'}<SortIcon field="title" />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleSort('card_type')}>
                        {lang === 'vi' ? 'Loại' : 'タイプ'}<SortIcon field="card_type" />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleSort('view_count')}>
                        <Eye className="w-3.5 h-3.5 inline mr-1" />
                        {lang === 'vi' ? 'Lượt xem' : '閲覧数'}<SortIcon field="view_count" />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleSort('page_title')}>
                        {lang === 'vi' ? 'Dashboard' : 'ダッシュボード'}<SortIcon field="page_title" />
                      </th>
                      <th className="p-2 cursor-pointer hover:text-blue-500" onClick={() => handleSort('owner_name')}>
                        {lang === 'vi' ? 'Owner' : 'オーナー'}<SortIcon field="owner_name" />
                      </th>
                      <th className="p-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">Loading...</td></tr>
                    ) : cards.length === 0 ? (
                      <tr><td colSpan={6} className="text-center p-8 text-slate-400">
                        {lang === 'vi' ? 'Chưa có dữ liệu. Hãy crawl Beast Mode trước.' : 'データなし。先にBeast Modeをクロールしてください。'}
                      </td></tr>
                    ) : cards.map(card => (
                      <tr key={card.id} className="border-b border-slate-50 hover:bg-slate-50/50 transition-colors">
                        <td className="p-2">
                          <div className="font-medium text-slate-700 text-xs truncate max-w-[250px]" title={card.title}>
                            {card.title || '—'}
                          </div>
                        </td>
                        <td className="p-2">
                          <span className="badge badge-info text-[10px]">{card.card_type || '—'}</span>
                        </td>
                        <td className="p-2">
                          <span className={`font-semibold text-xs ${card.view_count > 0 ? 'text-green-600' : 'text-slate-400'}`}>
                            {formatNumber(card.view_count)}
                          </span>
                        </td>
                        <td className="p-2">
                          <span className="text-xs text-slate-500 truncate max-w-[200px] block" title={card.page_title || ''}>
                            {card.page_title || '—'}
                          </span>
                        </td>
                        <td className="p-2 text-xs text-slate-500">{card.owner_name || '—'}</td>
                        <td className="p-2">
                          {domoBase && card.page_id && (
                            <a href={`${domoBase}/page/${card.page_id}`} target="_blank" rel="noopener noreferrer"
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

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between pt-2">
                  <span className="text-xs text-slate-400">
                    {lang === 'vi' ? 'Trang' : 'ページ'} {page}/{totalPages}
                  </span>
                  <div className="flex gap-1">
                    <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1}
                      className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-30">
                      <ChevronLeft className="w-4 h-4" />
                    </button>
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      const start = Math.max(1, Math.min(page - 2, totalPages - 4))
                      const p = start + i
                      if (p > totalPages) return null
                      return (
                        <button key={p} onClick={() => setPage(p)}
                          className={`w-8 h-8 rounded-lg text-xs font-semibold ${
                            p === page ? 'bg-blue-500 text-white' : 'border border-slate-200 hover:bg-slate-100'
                          }`}>
                          {p}
                        </button>
                      )
                    })}
                    <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
                      className="p-1.5 rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-30">
                      <ChevronRight className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Dashboards Tab */}
        {activeTab === 'dashboards' && stats && (
          <div className="card">
            <div className="card-header">
              {lang === 'vi' ? 'Top Dashboard theo lượt xem' : 'トップダッシュボード（閲覧数順）'}
            </div>
            <div className="card-body">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-200 text-left">
                    <th className="p-2">#</th>
                    <th className="p-2">{lang === 'vi' ? 'Dashboard' : 'ダッシュボード'}</th>
                    <th className="p-2">{lang === 'vi' ? 'Số card' : 'カード数'}</th>
                    <th className="p-2">{lang === 'vi' ? 'Tổng lượt xem' : '合計閲覧数'}</th>
                    <th className="p-2">{lang === 'vi' ? 'Phân bổ' : '分布'}</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.top_dashboards.map((d, i) => {
                    const maxViews = stats.top_dashboards[0]?.total_views || 1
                    const pct = Math.round((d.total_views / maxViews) * 100)
                    return (
                      <tr key={i} className="border-b border-slate-50 hover:bg-slate-50/50">
                        <td className="p-2 font-semibold text-slate-400">{i + 1}</td>
                        <td className="p-2 font-medium text-slate-700 text-xs">{d.page_title}</td>
                        <td className="p-2 text-xs">{d.card_count}</td>
                        <td className="p-2 text-xs font-semibold text-green-600">{formatNumber(d.total_views)}</td>
                        <td className="p-2 w-40">
                          <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                            <div className="h-full bg-gradient-to-r from-purple-400 to-blue-400 rounded-full transition-all"
                              style={{ width: `${pct}%` }} />
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
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
                      <div
                        className="h-full bg-gradient-to-r from-blue-400 to-cyan-400 rounded-full"
                        style={{ width: `${Math.round((t.count / (stats.total_cards || 1)) * 100)}%` }}
                      />
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
