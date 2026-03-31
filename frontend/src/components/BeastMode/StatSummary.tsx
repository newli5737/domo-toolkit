import { Summary } from './types'
import { COLOR_MAP, TEXT_COLOR, GROUP_CONFIG_JA, GROUP_CONFIG_VI } from './constants'
import { useI18n } from '../../i18n'

interface Props {
  summary: Summary
  setActiveTab: (tab: number) => void
}

export default function StatSummary({ summary, setActiveTab }: Props) {
  const { lang } = useI18n()
  const GROUP_CONFIG = lang === 'ja' ? GROUP_CONFIG_JA : GROUP_CONFIG_VI

  const getGroupCount = (num: number) => {
    if (!summary) return 0
    const g = summary.groups.find(g => g.group_number === num)
    return g?.count ?? 0
  }

  return (
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
    </>
  )
}
