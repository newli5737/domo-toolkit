import React, { useState, useEffect } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { Filter, X } from 'lucide-react'
import { apiGet } from '../../api'

interface FilterOption { column: string; values: { value: string; count: number }[] }

const tableStyle: React.CSSProperties = {
  width: '100%', borderCollapse: 'separate', borderSpacing: 0, fontSize: 13, fontFamily: "'Inter', -apple-system, sans-serif",
}
const thBase: React.CSSProperties = {
  padding: '12px 16px', fontWeight: 700, fontSize: 12, letterSpacing: 0.5, textTransform: 'uppercase',
  position: 'sticky', top: 0, zIndex: 1, borderBottom: '2px solid #c7d2fe',
  background: 'linear-gradient(135deg, #dbeafe 0%, #e0e7ff 100%)', color: '#1e3a5f',
}
const tdBase: React.CSSProperties = {
  padding: '10px 16px', borderBottom: '1px solid #f1f5f9', transition: 'background 0.15s ease',
}

function fmt(v: number): string {
  return '¥' + v.toLocaleString('ja-JP')
}

function ratioColor(r: number | null): string {
  if (r == null) return 'transparent'
  if (r >= 1.1) return '#dcfce7'
  if (r >= 1.0) return '#dbeafe'
  if (r >= 0.9) return '#fef9c3'
  return '#fee2e2'
}

function ratioText(r: number | null): string {
  if (r == null) return '—'
  return `${(r * 100).toFixed(1)}%`
}

export default function EmbedCard() {
  const { dataflowId, cardEndpoint } = useParams<{ dataflowId: string; cardEndpoint: string }>()
  const [searchParams] = useSearchParams()
  const dfId = dataflowId || '215'
  const endpoint = cardEndpoint || 'yoy'
  const lang = searchParams.get('lang') || 'ja'
  const customTitle = searchParams.get('title')

  const [filters, setFilters] = useState<FilterOption[]>([])
  const [active, setActive] = useState<Record<string, string>>({})
  const [cardData, setCardData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    apiGet<{ filters: FilterOption[] }>(`/api/pipeline/card/filters?dataflow_id=${dfId}`)
      .then(d => setFilters(d.filters)).catch(() => { })
  }, [dfId])

  useEffect(() => {
    setLoading(true)
    const p = new URLSearchParams({ dataflow_id: dfId })
    if (active['BLカテゴリ']) p.set('bl_category', active['BLカテゴリ'])
    if (active['ステータス名']) p.set('status_name', active['ステータス名'])
    if (active['ERAWANコード']) p.set('erawan', active['ERAWANコード'])
    if (active['プロジェクト名']) p.set('project', active['プロジェクト名'])
    apiGet(`/api/pipeline/card/${endpoint}?${p}`)
      .then(setCardData).catch(() => { }).finally(() => setLoading(false))
  }, [dfId, endpoint, active])

  const toggle = (col: string, val: string) => {
    setActive(prev => val ? { ...prev, [col]: val } : Object.fromEntries(Object.entries(prev).filter(([k]) => k !== col)))
  }

  const title = customTitle || cardData?.title || endpoint

  return (
    <div style={{
      minHeight: '100vh', background: '#fff',
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
    }}>
      {/* Compact header */}
      <div style={{
        padding: '14px 20px', borderBottom: '1px solid #e2e8f0',
        background: 'linear-gradient(135deg, #f8fafc 0%, #f0f4ff 100%)',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8,
            background: 'linear-gradient(135deg, #3b82f6, #6366f1)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: '#fff', fontSize: 14, fontWeight: 800,
          }}>D</div>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: '#0f172a' }}>{title}</div>
            <div style={{ fontSize: 11, color: '#94a3b8' }}>
              Dataflow {dfId} · {cardData?.chart_type || 'pivot_table'}
            </div>
          </div>
        </div>
        <div style={{
          fontSize: 10, color: '#94a3b8', padding: '3px 8px',
          background: '#f1f5f9', borderRadius: 4,
        }}>
          DOMO Toolkit
        </div>
      </div>

      {/* Compact filter bar */}
      <div style={{
        padding: '10px 20px', borderBottom: '1px solid #f1f5f9',
        display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center',
        background: '#fafbfc',
      }}>
        <Filter style={{ width: 14, height: 14, color: '#94a3b8' }} />
        {filters.slice(0, 4).map(f => (
          <select key={f.column} value={active[f.column] || ''} onChange={e => toggle(f.column, e.target.value)}
            style={{
              padding: '5px 8px', borderRadius: 6, border: '1px solid #e2e8f0', fontSize: 11,
              minWidth: 100, background: active[f.column] ? '#dbeafe' : '#fff', cursor: 'pointer',
              outline: 'none',
            }}>
            <option value="">{f.column}</option>
            {f.values.map(v => <option key={v.value} value={v.value}>{v.value} ({v.count})</option>)}
          </select>
        ))}
        {Object.entries(active).map(([col, val]) => (
          <span key={col} style={{
            display: 'inline-flex', alignItems: 'center', gap: 3,
            padding: '3px 8px', borderRadius: 12, background: '#dbeafe', color: '#1e40af',
            fontSize: 10, fontWeight: 600,
          }}>
            {col}: {val}
            <X style={{ width: 10, height: 10, cursor: 'pointer' }} onClick={() => toggle(col, '')} />
          </span>
        ))}
        {Object.keys(active).length > 0 && (
          <button onClick={() => setActive({})} style={{
            padding: '3px 8px', borderRadius: 12, background: '#fee2e2', color: '#dc2626',
            fontSize: 10, fontWeight: 600, border: 'none', cursor: 'pointer',
          }}>
            {lang === 'vi' ? 'Xóa' : 'クリア'}
          </button>
        )}
      </div>

      {/* Card content — full width */}
      <div style={{ padding: 0 }}>
        {loading ? (
          <div style={{ padding: 80, textAlign: 'center' }}>
            <div style={{
              width: 32, height: 32, border: '3px solid #e2e8f0', borderTopColor: '#3b82f6',
              borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto',
            }} />
          </div>
        ) : endpoint === 'yoy' ? (
          <EmbedYoYTable data={cardData} lang={lang} />
        ) : endpoint === 'revenue-by-year' ? (
          <EmbedRevenueTable data={cardData} lang={lang} />
        ) : (
          <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8' }}>Unknown card</div>
        )}
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  )
}

function EmbedYoYTable({ data, lang }: { data: any; lang: string }) {
  if (!data?.exists || !data.rows?.length) return <div style={{ padding: 60, textAlign: 'center', color: '#94a3b8' }}>{lang === 'vi' ? 'Không có dữ liệu' : 'データなし'}</div>
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={tableStyle}>
        <thead>
          <tr>
            <th style={{ ...thBase, textAlign: 'left' }}>{lang === 'vi' ? 'Tháng' : '請求月'}</th>
            <th style={{ ...thBase, textAlign: 'right' }}>{lang === 'vi' ? 'Năm nay' : '当年'}</th>
            <th style={{ ...thBase, textAlign: 'right' }}>{lang === 'vi' ? 'Năm trước' : '前年'}</th>
            <th style={{ ...thBase, textAlign: 'right' }}>{lang === 'vi' ? 'So sánh YoY' : '昨対比'}</th>
          </tr>
        </thead>
        <tbody>
          {data.rows.map((r: any, i: number) => (
            <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#f8fafc' }}>
              <td style={{ ...tdBase, fontWeight: 700, color: '#334155' }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#3b82f6' }} />
                  {r.month}
                </span>
              </td>
              <td style={{ ...tdBase, textAlign: 'right', fontFamily: "'JetBrains Mono', monospace", fontWeight: 600 }}>{fmt(r.current_year)}</td>
              <td style={{ ...tdBase, textAlign: 'right', fontFamily: "'JetBrains Mono', monospace", color: '#64748b' }}>{fmt(r.prev_year)}</td>
              <td style={{
                ...tdBase, textAlign: 'right', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace",
                background: ratioColor(r.yoy_ratio),
                color: r.yoy_ratio == null ? '#94a3b8' : r.yoy_ratio >= 1 ? '#15803d' : '#dc2626',
              }}>
                <span style={{ padding: '2px 8px', borderRadius: 6, background: r.yoy_ratio != null ? 'rgba(255,255,255,0.6)' : 'transparent' }}>
                  {r.yoy_ratio != null && r.yoy_ratio >= 1 && '▲ '}
                  {r.yoy_ratio != null && r.yoy_ratio < 1 && '▼ '}
                  {ratioText(r.yoy_ratio)}
                </span>
              </td>
            </tr>
          ))}
          <tr style={{ background: 'linear-gradient(135deg, #f0f4ff 0%, #e8eeff 100%)' }}>
            <td style={{ ...tdBase, fontWeight: 800, color: '#1e3a5f', borderTop: '2px solid #c7d2fe', borderBottom: 'none' }}>{lang === 'vi' ? 'TỔNG CỘNG' : '合計'}</td>
            <td style={{ ...tdBase, textAlign: 'right', fontWeight: 800, fontFamily: "'JetBrains Mono', monospace", fontSize: 14, borderTop: '2px solid #c7d2fe', borderBottom: 'none' }}>{fmt(data.totals.current_year)}</td>
            <td style={{ ...tdBase, textAlign: 'right', fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: '#64748b', borderTop: '2px solid #c7d2fe', borderBottom: 'none' }}>{fmt(data.totals.prev_year)}</td>
            <td style={{
              ...tdBase, textAlign: 'right', fontWeight: 800, fontFamily: "'JetBrains Mono', monospace", fontSize: 14,
              borderTop: '2px solid #c7d2fe', borderBottom: 'none', background: ratioColor(data.totals.yoy_ratio),
              color: data.totals.yoy_ratio != null && data.totals.yoy_ratio >= 1 ? '#15803d' : '#dc2626',
            }}>
              <span style={{ padding: '3px 10px', borderRadius: 8, background: 'rgba(255,255,255,0.7)' }}>
                {data.totals.yoy_ratio != null && data.totals.yoy_ratio >= 1 && '▲ '}
                {data.totals.yoy_ratio != null && data.totals.yoy_ratio < 1 && '▼ '}
                {ratioText(data.totals.yoy_ratio)}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  )
}

function EmbedRevenueTable({ data, lang }: { data: any; lang: string }) {
  if (!data?.exists || !data.rows?.length) return <div style={{ padding: 60, textAlign: 'center', color: '#94a3b8' }}>{lang === 'vi' ? 'Không có dữ liệu' : 'データなし'}</div>
  const years: number[] = data.years || []
  const yearColors = ['#6366f1', '#3b82f6', '#06b6d4', '#10b981', '#f59e0b']
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={tableStyle}>
        <thead>
          <tr>
            <th style={{ ...thBase, textAlign: 'left', minWidth: 80 }}>{lang === 'vi' ? 'Tháng' : '請求月'}</th>
            {years.map((y, idx) => (
              <th key={y} style={{ ...thBase, textAlign: 'right', minWidth: 110 }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: yearColors[idx % yearColors.length] }} />
                  {y}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((r: any, i: number) => {
            const vals = years.map(y => r[String(y)] || 0)
            const maxVal = Math.max(...vals, 1)
            return (
              <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#f8fafc' }}>
                <td style={{ ...tdBase, fontWeight: 700, color: '#334155' }}>
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#8b5cf6' }} />
                    {r.month}
                  </span>
                </td>
                {years.map((y, idx) => {
                  const val = r[String(y)] || 0
                  const intensity = maxVal > 0 ? val / maxVal : 0
                  const bgAlpha = Math.round(intensity * 0.12 * 255).toString(16).padStart(2, '0')
                  return (
                    <td key={y} style={{
                      ...tdBase, textAlign: 'right', fontFamily: "'JetBrains Mono', monospace",
                      fontWeight: idx === years.length - 1 ? 700 : 500,
                      color: idx === years.length - 1 ? '#0f172a' : '#475569',
                      background: `${yearColors[idx % yearColors.length]}${bgAlpha}`,
                    }}>{fmt(val)}</td>
                  )
                })}
              </tr>
            )
          })}
          <tr style={{ background: 'linear-gradient(135deg, #f0f4ff 0%, #e8eeff 100%)' }}>
            <td style={{ ...tdBase, fontWeight: 800, color: '#1e3a5f', borderTop: '2px solid #c7d2fe', borderBottom: 'none' }}>{lang === 'vi' ? 'TỔNG CỘNG' : '合計'}</td>
            {years.map((y, idx) => (
              <td key={y} style={{
                ...tdBase, textAlign: 'right', fontWeight: 800, fontFamily: "'JetBrains Mono', monospace",
                fontSize: 14, borderTop: '2px solid #c7d2fe', borderBottom: 'none',
              }}>
                <span style={{ padding: '3px 10px', borderRadius: 8, background: `${yearColors[idx % yearColors.length]}18` }}>
                  {fmt(data.totals?.[String(y)] || 0)}
                </span>
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  )
}
