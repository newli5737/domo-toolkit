import { useState } from 'react'
import { BmRow } from './types'
import { GROUP_CONFIG_JA, GROUP_CONFIG_VI, BADGE_BG } from './constants'
import { useI18n } from '../../i18n'

interface Props {
  groupData: BmRow[]
  searchResults: BmRow[] | null
  loadingGroup: boolean
  searching: boolean
  searchQuery: string
  handleSearch: (value: string) => void
  activeTab: number
  setActiveTab: (tab: number) => void
  getGroupCount: (num: number) => number
  setDeleteTarget: (target: BmRow) => void
}

export default function BeastModeTable({
  groupData,
  searchResults,
  loadingGroup,
  searching,
  searchQuery,
  handleSearch,
  activeTab,
  setActiveTab,
  getGroupCount,
  setDeleteTarget,
}: Props) {
  const { lang } = useI18n()
  const GROUP_CONFIG = lang === 'ja' ? GROUP_CONFIG_JA : GROUP_CONFIG_VI
  const [currentPage, setCurrentPage] = useState(1)
  const PAGE_SIZE = 50

  const allData = searchResults ?? groupData
  const totalPages = Math.ceil(allData.length / PAGE_SIZE)
  const pageData = allData.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE)

  return (
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

      {/* Tabs */}
      {!searchResults && (
      <div className="flex gap-1 p-1 bg-[var(--color-bg-secondary)] rounded-lg w-fit mb-5">
        {GROUP_CONFIG.map(g => (
          <button
            key={g.num}
            onClick={() => { setActiveTab(g.num); setCurrentPage(1); }}
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
        ) : allData.length === 0 ? (
          <div className="p-12 text-center text-gray-500 text-sm">
            {searchResults !== null
              ? (lang === 'ja' ? 'Beast Modeが見つかりません' : 'Không tìm thấy Beast Mode nào')
              : (lang === 'ja' ? 'このグループにBeast Modeはありません' : 'Không có Beast Mode nào trong nhóm này')}
          </div>
        ) : (
          <>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-[var(--color-border)]">
                  <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">ID</th>
                  <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">Legacy ID</th>
                  <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? '名前' : 'Tên'}</th>
                  <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? 'グループ' : 'Nhóm'}</th>
                  <th className="px-4 py-3.5 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? 'オーナー' : 'Owner'}</th>
                  <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Cards</th>
                  <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Views</th>
                  <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Refs</th>
                  <th className="px-4 py-3.5 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">Complexity</th>
                  <th className="px-4 py-3.5 text-center text-[10px] font-semibold uppercase tracking-wider text-gray-500">{lang === 'ja' ? '操作' : 'Hành động'}</th>
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
                          {gcfg?.label ?? bm.group_label}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-400 max-w-[120px] truncate" title={bm.owner_name || '—'}>{bm.owner_name || '—'}</td>
                      <td className="px-4 py-3 text-right">
                        {bm.card_ids ? (
                          <div className="flex flex-col items-end gap-0.5">
                            {bm.card_ids.split('\n').filter(Boolean).map((cid, ci) => (
                              <a
                                key={ci}
                                href={`https://astecpaints-co-jp.domo.com/page/kpis/details/${cid.trim()}`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-[10px] text-[var(--color-accent-cyan)] hover:underline font-mono"
                              >
                                #{cid.trim()}
                              </a>
                            ))}
                          </div>
                        ) : (
                          <span className="text-sm text-gray-500">{bm.active_cards_count}</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.total_views.toLocaleString()}</td>
                      <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.referenced_by_count}</td>
                      <td className="px-4 py-3 text-right text-sm text-gray-400">{bm.complexity_score}</td>
                      <td className="px-4 py-3 text-center">
                        <button
                          onClick={() => setDeleteTarget(bm)}
                          className="w-7 h-7 rounded bg-[var(--color-accent-red)]/10 text-[var(--color-accent-red)] hover:bg-[var(--color-accent-red)] hover:text-white transition-colors"
                          title={lang === 'ja' ? 'BMを削除' : 'Xóa BM'}
                        >
                          🗑️
                        </button>
                      </td>
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
        )}
      </div>
    </div>
  )
}
