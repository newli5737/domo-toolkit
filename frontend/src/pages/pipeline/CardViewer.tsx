import { useState, useEffect } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { ChevronLeft, Filter, X, CreditCard } from 'lucide-react'
import { apiGet } from '../../api'
import { useI18n } from '../../i18n'

interface FilterOption { column: string; values: { value: string; count: number }[] }

export default function CardDetailPage() {
  const { dataflowId, cardEndpoint } = useParams<{ dataflowId: string; cardEndpoint: string }>()
  const navigate = useNavigate()
  const { lang } = useI18n()
  const dfId = dataflowId || '215'
  const endpoint = cardEndpoint || 'yoy'

  const [filters, setFilters] = useState<FilterOption[]>([])
  const [active, setActive] = useState<Record<string, string>>({})
  const [cardData, setCardData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    apiGet<{ filters: FilterOption[] }>(`/api/pipeline/card/filters?dataflow_id=${dfId}`)
      .then(d => setFilters(d.filters)).catch(() => {})
  }, [dfId])

  useEffect(() => {
    setLoading(true)
    const p = new URLSearchParams({ dataflow_id: dfId })
    if (active['BLカテゴリ']) p.set('bl_category', active['BLカテゴリ'])
    if (active['ステータス名']) p.set('status_name', active['ステータス名'])
    if (active['ERAWANコード']) p.set('erawan', active['ERAWANコード'])
    if (active['プロジェクト名']) p.set('project', active['プロジェクト名'])
    apiGet(`/api/pipeline/card/${endpoint}?${p}`)
      .then(setCardData).catch(() => {}).finally(() => setLoading(false))
  }, [dfId, endpoint, active])

  const toggle = (col: string, val: string) => {
    setActive(prev => val ? { ...prev, [col]: val } : Object.fromEntries(Object.entries(prev).filter(([k]) => k !== col)))
  }

  const title = cardData?.title || endpoint

  return (
    <div className="animate-fadein">
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <button onClick={() => navigate(`/datasets/${dfId}`)} className="btn btn-outline" style={{ padding: '4px 8px', fontSize: 12 }}>
            <ChevronLeft style={{ width: 14, height: 14 }} />
          </button>
          <span style={{ fontSize: 12, color: '#64748b' }}>
            <Link to="/pipeline" style={{ color: '#3b82f6', textDecoration: 'none' }}>Pipeline</Link>
            {' / '}<Link to={`/datasets/${dfId}`} style={{ color: '#3b82f6', textDecoration: 'none' }}>Dataset</Link>
            {' / '}Card
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <CreditCard style={{ width: 28, height: 28, color: '#3b82f6' }} />
          <div>
            <h1 style={{ margin: 0, fontSize: 20 }}>{title}</h1>
            <div style={{ fontSize: 12, color: '#64748b' }}>
              Card #{cardData?.card_id} · {cardData?.chart_type || 'pivot_table'} · Dataflow {dfId}
            </div>
          </div>
        </div>
      </div>

      <div className="page-body">
        {/* Filter bar */}
        <div className="card" style={{ marginBottom: 16 }}>
          <div className="card-body" style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
            <Filter style={{ width: 16, height: 16, color: '#64748b' }} />
            <span style={{ fontSize: 12, color: '#64748b', fontWeight: 600 }}>{lang === 'vi' ? 'Bộ lọc:' : 'フィルター:'}</span>
            {filters.slice(0, 4).map(f => (
              <select key={f.column} value={active[f.column] || ''} onChange={e => toggle(f.column, e.target.value)}
                style={{
                  padding: '6px 10px', borderRadius: 6, border: '1px solid #e2e8f0', fontSize: 12,
                  minWidth: 120, background: active[f.column] ? '#dbeafe' : '#fff', cursor: 'pointer',
                }}>
                <option value="">{f.column}</option>
                {f.values.map(v => <option key={v.value} value={v.value}>{v.value} ({v.count})</option>)}
              </select>
            ))}
            {/* Active filter tags */}
            {Object.entries(active).map(([col, val]) => (
              <span key={col} style={{
                display: 'inline-flex', alignItems: 'center', gap: 4,
                padding: '4px 10px', borderRadius: 20, background: '#dbeafe', color: '#1e40af',
                fontSize: 11, fontWeight: 600,
              }}>
                {col}: {val}
                <X style={{ width: 12, height: 12, cursor: 'pointer' }} onClick={() => toggle(col, '')} />
              </span>
            ))}
            {Object.keys(active).length > 0 && (
              <button onClick={() => setActive({})} style={{
                padding: '4px 10px', borderRadius: 20, background: '#fee2e2', color: '#dc2626',
                fontSize: 11, fontWeight: 600, border: 'none', cursor: 'pointer',
              }}>
                {lang === 'vi' ? 'Xóa tất cả' : 'クリア'}
              </button>
            )}
          </div>
        </div>

        {/* Card content */}
        <div className="card">
          <div className="card-body table-wrapper" style={{ padding: 0 }}>
            {loading ? (
              <div style={{ padding: 60, textAlign: 'center' }}><div className="spinner" style={{ margin: '0 auto' }} /></div>
            ) : endpoint === 'yoy' ? (
              <YoYTable data={cardData} lang={lang} />
            ) : endpoint === 'revenue-by-year' ? (
              <RevenueByYearTable data={cardData} lang={lang} />
            ) : (
              <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8' }}>Unknown card type</div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function YoYTable({ data, lang }: { data: any; lang: string }) {
  if (!data?.exists || !data.rows?.length) return <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8' }}>{lang === 'vi' ? 'Không có dữ liệu' : 'データなし'}</div>
  return (
    <table className="data-table">
      <thead>
        <tr style={{ background: '#D9EBFD' }}>
          <th>請求月</th><th style={{ textAlign: 'right' }}>当年</th>
          <th style={{ textAlign: 'right' }}>前年</th><th style={{ textAlign: 'right' }}>昨対比</th>
        </tr>
      </thead>
      <tbody>
        {data.rows.map((r: any, i: number) => (
          <tr key={i}>
            <td style={{ fontWeight: 600 }}>{r.month}</td>
            <td style={{ textAlign: 'right' }}>¥{r.current_year.toLocaleString()}</td>
            <td style={{ textAlign: 'right' }}>¥{r.prev_year.toLocaleString()}</td>
            <td style={{ textAlign: 'right', fontWeight: 700, background: r.yoy_ratio == null ? '#fff' : r.yoy_ratio >= 1 ? '#D9EBFD' : '#FDDDDD' }}>
              {r.yoy_ratio != null ? `${(r.yoy_ratio * 100).toFixed(2)}%` : '—'}
            </td>
          </tr>
        ))}
        <tr style={{ fontWeight: 800, borderTop: '2px solid #e2e8f0' }}>
          <td>合計</td>
          <td style={{ textAlign: 'right' }}>¥{data.totals.current_year.toLocaleString()}</td>
          <td style={{ textAlign: 'right' }}>¥{data.totals.prev_year.toLocaleString()}</td>
          <td style={{ textAlign: 'right', fontWeight: 700, background: data.totals.yoy_ratio != null && data.totals.yoy_ratio >= 1 ? '#D9EBFD' : '#FDDDDD' }}>
            {data.totals.yoy_ratio != null ? `${(data.totals.yoy_ratio * 100).toFixed(2)}%` : '—'}
          </td>
        </tr>
      </tbody>
    </table>
  )
}

function RevenueByYearTable({ data, lang }: { data: any; lang: string }) {
  if (!data?.exists || !data.rows?.length) return <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8' }}>{lang === 'vi' ? 'Không có dữ liệu' : 'データなし'}</div>
  const years: number[] = data.years || []
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
          {years.map(y => <td key={y} style={{ textAlign: 'right' }}>¥{(data.totals?.[String(y)] || 0).toLocaleString()}</td>)}
        </tr>
      </tbody>
    </table>
  )
}
