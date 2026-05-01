import { useState, useEffect } from 'react'
import { Filter, X, CreditCard } from 'lucide-react'
import { apiGet } from '../../api'

interface FilterOption { column: string; values: { value: string; count: number }[] }
interface CardViewerProps {
  dataflowId: string
  cardEndpoint: string // "yoy" | "revenue-by-year"
  cardTitle: string
  onClose: () => void
}

export default function CardViewer({ dataflowId, cardEndpoint, cardTitle, onClose }: CardViewerProps) {
  const [filters, setFilters] = useState<FilterOption[]>([])
  const [activeFilters, setActiveFilters] = useState<Record<string, string>>({})
  const [cardData, setCardData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  // Load filter options once
  useEffect(() => {
    apiGet<{ filters: FilterOption[] }>(`/api/pipeline/card/filters?dataflow_id=${dataflowId}`)
      .then(d => setFilters(d.filters)).catch(() => {})
  }, [dataflowId])

  // Load card data when filters change
  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams({ dataflow_id: dataflowId })
    if (activeFilters['BLカテゴリ']) params.set('bl_category', activeFilters['BLカテゴリ'])
    if (activeFilters['ステータス名']) params.set('status_name', activeFilters['ステータス名'])
    if (activeFilters['ERAWANコード']) params.set('erawan', activeFilters['ERAWANコード'])
    if (activeFilters['プロジェクト名']) params.set('project', activeFilters['プロジェクト名'])
    apiGet(`/api/pipeline/card/${cardEndpoint}?${params}`)
      .then(setCardData).catch(() => {}).finally(() => setLoading(false))
  }, [dataflowId, cardEndpoint, activeFilters])

  const setFilter = (col: string, val: string) => {
    setActiveFilters(prev => val ? { ...prev, [col]: val } : Object.fromEntries(Object.entries(prev).filter(([k]) => k !== col)))
  }

  const activeCount = Object.keys(activeFilters).length

  return (
    <div className="card animate-fadein" style={{ marginTop: 16 }}>
      <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8, background: '#D9EBFD' }}>
        <CreditCard style={{ width: 16, height: 16 }} />
        <span style={{ flex: 1, fontWeight: 700 }}>{cardTitle}</span>
        <button onClick={onClose} className="btn btn-outline" style={{ padding: '4px 8px', fontSize: 12 }}>✕ Close</button>
      </div>

      {/* Filter Bar */}
      <div style={{ padding: '12px 20px', borderBottom: '1px solid #f1f5f9', display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
        <Filter style={{ width: 14, height: 14, color: '#64748b' }} />
        {filters.slice(0, 4).map(f => (
          <select key={f.column} value={activeFilters[f.column] || ''}
            onChange={e => setFilter(f.column, e.target.value)}
            style={{ padding: '5px 8px', borderRadius: 6, border: '1px solid #e2e8f0', fontSize: 12, minWidth: 100,
              background: activeFilters[f.column] ? '#dbeafe' : '#fff' }}>
            <option value="">{f.column}</option>
            {f.values.map(v => <option key={v.value} value={v.value}>{v.value} ({v.count})</option>)}
          </select>
        ))}
        {/* Active filter tags */}
        {activeCount > 0 && (
          <div style={{ display: 'flex', gap: 4, marginLeft: 8 }}>
            {Object.entries(activeFilters).map(([col, val]) => (
              <span key={col} style={{
                display: 'inline-flex', alignItems: 'center', gap: 4,
                padding: '3px 8px', borderRadius: 12, background: '#dbeafe', color: '#1e40af',
                fontSize: 11, fontWeight: 600,
              }}>
                {val}
                <X style={{ width: 12, height: 12, cursor: 'pointer' }} onClick={() => setFilter(col, '')} />
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Card Content */}
      <div className="card-body table-wrapper" style={{ padding: 0 }}>
        {loading ? (
          <div style={{ padding: 40, textAlign: 'center' }}><div className="spinner" style={{ margin: '0 auto' }} /></div>
        ) : cardEndpoint === 'yoy' ? (
          <YoYTable data={cardData} />
        ) : cardEndpoint === 'revenue-by-year' ? (
          <RevenueByYearTable data={cardData} />
        ) : <div style={{ padding: 20, color: '#94a3b8' }}>Unknown card type</div>}
      </div>
    </div>
  )
}

function YoYTable({ data }: { data: any }) {
  if (!data?.exists) return <div style={{ padding: 20, color: '#94a3b8' }}>No data</div>
  return (
    <table className="data-table">
      <thead>
        <tr style={{ background: '#D9EBFD' }}>
          <th>請求月</th><th style={{ textAlign: 'right' }}>当年</th>
          <th style={{ textAlign: 'right' }}>前年</th><th style={{ textAlign: 'right' }}>昨対比</th>
        </tr>
      </thead>
      <tbody>
        {data.rows.map((r: any, i: number) => {
          const bg = r.yoy_ratio == null ? '#fff' : r.yoy_ratio >= 1 ? '#D9EBFD' : '#FDDDDD'
          return (
            <tr key={i}>
              <td style={{ fontWeight: 600 }}>{r.month}</td>
              <td style={{ textAlign: 'right' }}>¥{r.current_year.toLocaleString()}</td>
              <td style={{ textAlign: 'right' }}>¥{r.prev_year.toLocaleString()}</td>
              <td style={{ textAlign: 'right', background: bg, fontWeight: 700 }}>
                {r.yoy_ratio != null ? `${(r.yoy_ratio * 100).toFixed(2)}%` : '—'}
              </td>
            </tr>
          )
        })}
        <tr style={{ fontWeight: 800, borderTop: '2px solid #e2e8f0' }}>
          <td>合計</td>
          <td style={{ textAlign: 'right' }}>¥{data.totals.current_year.toLocaleString()}</td>
          <td style={{ textAlign: 'right' }}>¥{data.totals.prev_year.toLocaleString()}</td>
          <td style={{ textAlign: 'right', background: data.totals.yoy_ratio != null && data.totals.yoy_ratio >= 1 ? '#D9EBFD' : '#FDDDDD', fontWeight: 700 }}>
            {data.totals.yoy_ratio != null ? `${(data.totals.yoy_ratio * 100).toFixed(2)}%` : '—'}
          </td>
        </tr>
      </tbody>
    </table>
  )
}

function RevenueByYearTable({ data }: { data: any }) {
  if (!data?.exists) return <div style={{ padding: 20, color: '#94a3b8' }}>No data</div>
  const years: number[] = data.years
  return (
    <table className="data-table">
      <thead>
        <tr style={{ background: '#D9EBFD' }}>
          <th>請求月</th>
          {years.map(y => <th key={y} style={{ textAlign: 'right' }}>{y}</th>)}
        </tr>
      </thead>
      <tbody>
        {data.rows.map((r: any, i: number) => (
          <tr key={i}>
            <td style={{ fontWeight: 600 }}>{r.month}</td>
            {years.map(y => <td key={y} style={{ textAlign: 'right' }}>¥{(r[String(y)] || 0).toLocaleString()}</td>)}
          </tr>
        ))}
        <tr style={{ fontWeight: 800, borderTop: '2px solid #e2e8f0' }}>
          <td>合計</td>
          {years.map(y => <td key={y} style={{ textAlign: 'right' }}>¥{(data.totals[String(y)] || 0).toLocaleString()}</td>)}
        </tr>
      </tbody>
    </table>
  )
}
