import { useState, useEffect } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { ChevronLeft, Save, FileSpreadsheet, CreditCard, Columns3, Database, Clock, HardDrive } from 'lucide-react'
import { apiGet, apiPut } from '../../api'
import { useI18n } from '../../i18n'

interface ColumnInfo { name: string; type: string; samples: (string | null)[]; null_count: number; distinct_count: number }
interface CardInfo { id: number; title: string; endpoint: string; chart_type: string; description: string }
interface DetailData {
  exists: boolean; display_name: string; total_rows: number; column_count: number
  columns: ColumnInfo[]; cards: CardInfo[]; last_modified: number; size_bytes: number
}

export default function DatasetDetailPage() {
  const { dataflowId } = useParams<{ dataflowId: string }>()
  const navigate = useNavigate()
  const { lang } = useI18n()
  const dfId = dataflowId || '215'
  const [detail, setDetail] = useState<DetailData | null>(null)
  const [editName, setEditName] = useState('')
  const [saving, setSaving] = useState(false)
  const [activeTab, setActiveTab] = useState<'overview' | 'data' | 'cards'>('overview')

  useEffect(() => {
    apiGet<DetailData>(`/api/pipeline/datasets/detail?dataflow_id=${dfId}`)
      .then(d => { setDetail(d); setEditName(d.display_name) }).catch(() => {})
  }, [dfId])

  const saveName = async () => {
    setSaving(true)
    try {
      await apiPut('/api/pipeline/datasets/rename', { dataflow_id: dfId, display_name: editName })
      setDetail(prev => prev ? { ...prev, display_name: editName } : prev)
    } catch { /* */ }
    setSaving(false)
  }

  if (!detail) return <div style={{ padding: 60, textAlign: 'center' }}><div className="spinner" style={{ margin: '0 auto' }} /></div>

  return (
    <div className="animate-fadein">
      {/* Breadcrumb */}
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <button onClick={() => navigate('/pipeline')} className="btn btn-outline" style={{ padding: '4px 8px', fontSize: 12 }}>
            <ChevronLeft style={{ width: 14, height: 14 }} />
          </button>
          <span style={{ fontSize: 12, color: '#64748b' }}>
            <Link to="/pipeline" style={{ color: '#3b82f6', textDecoration: 'none' }}>Pipeline</Link>
            {' / '}Dataflow {dfId}{' / '}Dataset
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <HardDrive style={{ width: 28, height: 28, color: '#8b5cf6' }} />
          <div style={{ flex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <input value={editName} onChange={e => setEditName(e.target.value)}
                style={{ fontSize: 18, fontWeight: 700, border: '1px solid transparent', borderRadius: 6, padding: '4px 8px', background: 'transparent', outline: 'none' }}
                onFocus={e => e.target.style.borderColor = '#e2e8f0'} onBlur={e => e.target.style.borderColor = 'transparent'} />
              {editName !== detail.display_name && (
                <button onClick={saveName} disabled={saving} className="btn btn-primary" style={{ padding: '4px 12px', fontSize: 12 }}>
                  <Save style={{ width: 14, height: 14 }} /> {lang === 'vi' ? 'Lưu' : '保存'}
                </button>
              )}
            </div>
            <div style={{ fontSize: 12, color: '#64748b', marginTop: 2 }}>
              DataFlow · Domo · {detail.column_count} {lang === 'vi' ? 'cột' : 'columns'} · {detail.total_rows.toLocaleString()} rows
              · {lang === 'vi' ? 'Lần chạy cuối' : '最終実行'}: {new Date(detail.last_modified * 1000).toLocaleString()}
            </div>
          </div>
        </div>
      </div>

      <div className="page-body">
        {/* Tabs — like DOMO */}
        <div style={{ display: 'flex', gap: 0, marginBottom: 20, borderBottom: '2px solid #e2e8f0' }}>
          {([
            { key: 'overview' as const, label: lang === 'vi' ? 'Tổng quan' : 'OVERVIEW' },
            { key: 'data' as const, label: lang === 'vi' ? 'Dữ liệu' : 'DATA' },
            { key: 'cards' as const, label: 'CARDS' },
          ]).map(tab => (
            <button key={tab.key} onClick={() => setActiveTab(tab.key)} style={{
              padding: '10px 24px', border: 'none', cursor: 'pointer', fontSize: 13, fontWeight: 600, letterSpacing: 0.5,
              background: 'transparent', color: activeTab === tab.key ? '#0f172a' : '#94a3b8',
              borderBottom: activeTab === tab.key ? '2px solid #3b82f6' : '2px solid transparent', marginBottom: -2,
            }}>
              {tab.label}
            </button>
          ))}
        </div>

        {/* OVERVIEW tab */}
        {activeTab === 'overview' && (
          <div className="animate-fadein">
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 24 }}>
              <InfoCard icon={<CreditCard style={{ width: 24, height: 24, color: '#3b82f6' }} />}
                value={String(detail.cards.length)} label={lang === 'vi' ? 'Card liên kết' : 'Cards powered'} />
              <InfoCard icon={<Columns3 style={{ width: 24, height: 24, color: '#8b5cf6' }} />}
                value={String(detail.column_count)} label={lang === 'vi' ? 'Số cột' : 'Columns'} />
              <InfoCard icon={<Database style={{ width: 24, height: 24, color: '#10b981' }} />}
                value={`${(detail.size_bytes / 1024 / 1024).toFixed(1)} MB`} label={lang === 'vi' ? 'Dung lượng' : 'Size'} />
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
              <InfoCard icon={<Clock style={{ width: 24, height: 24, color: '#f59e0b' }} />}
                value={new Date(detail.last_modified * 1000).toLocaleDateString()} label={lang === 'vi' ? 'Lần chạy cuối' : '最終実行'} />
              <InfoCard icon={<FileSpreadsheet style={{ width: 24, height: 24, color: '#06b6d4' }} />}
                value={detail.total_rows.toLocaleString()} label={lang === 'vi' ? 'Tổng số dòng' : '行数'} />
              <InfoCard icon={<HardDrive style={{ width: 24, height: 24, color: '#64748b' }} />}
                value="DuckDB" label={lang === 'vi' ? 'Định dạng' : 'Format'} />
            </div>
          </div>
        )}

        {/* DATA tab — column table */}
        {activeTab === 'data' && (
          <div className="card animate-fadein">
            <div className="card-body table-wrapper" style={{ maxHeight: 600, overflowY: 'auto', padding: 0 }}>
              <table className="data-table" style={{ fontSize: 12 }}>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>{lang === 'vi' ? 'Tên cột' : 'カラム名'}</th>
                    <th>{lang === 'vi' ? 'Kiểu' : 'Type'}</th>
                    <th style={{ textAlign: 'right' }}>Distinct</th>
                    <th style={{ textAlign: 'right' }}>Nulls</th>
                    <th>{lang === 'vi' ? 'Mẫu dữ liệu' : 'サンプル'}</th>
                  </tr>
                </thead>
                <tbody>
                  {detail.columns.map((col, i) => (
                    <tr key={i}>
                      <td style={{ color: '#94a3b8', width: 40 }}>{i + 1}</td>
                      <td style={{ fontWeight: 600 }}>{col.name}</td>
                      <td><span style={{ padding: '2px 6px', borderRadius: 4, background: '#f1f5f9', fontSize: 11, fontFamily: 'monospace' }}>{col.type}</span></td>
                      <td style={{ textAlign: 'right' }}>{col.distinct_count.toLocaleString()}</td>
                      <td style={{ textAlign: 'right' }}>{col.null_count.toLocaleString()}</td>
                      <td style={{ color: '#64748b', maxWidth: 250, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {col.samples.filter(Boolean).join(' | ')}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* CARDS tab */}
        {activeTab === 'cards' && (
          <div className="animate-fadein" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 }}>
            {detail.cards.map(card => (
              <Link key={card.id} to={`/card/${dfId}/${card.endpoint}`}
                style={{ textDecoration: 'none', color: 'inherit' }}>
                <div className="card" style={{ cursor: 'pointer', transition: 'box-shadow 0.15s, transform 0.15s' }}
                  onMouseEnter={e => { e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)'; e.currentTarget.style.transform = 'translateY(-2px)' }}
                  onMouseLeave={e => { e.currentTarget.style.boxShadow = ''; e.currentTarget.style.transform = '' }}>
                  <div className="card-body" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <div style={{ width: 40, height: 40, borderRadius: 10, background: '#dbeafe', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                      <CreditCard style={{ width: 20, height: 20, color: '#3b82f6' }} />
                    </div>
                    <div>
                      <div style={{ fontSize: 14, fontWeight: 700, color: '#0f172a' }}>{card.title}</div>
                      <div style={{ fontSize: 11, color: '#64748b' }}>#{card.id} · {card.chart_type}</div>
                    </div>
                  </div>
                </div>
              </Link>
            ))}
            {detail.cards.length === 0 && (
              <div style={{ padding: 40, textAlign: 'center', color: '#94a3b8', gridColumn: '1/-1' }}>
                {lang === 'vi' ? 'Chưa có card nào' : 'カードはありません'}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function InfoCard({ icon, value, label }: { icon: React.ReactNode; value: string; label: string }) {
  return (
    <div className="card">
      <div className="card-body" style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
        {icon}
        <div>
          <div style={{ fontSize: 22, fontWeight: 800, color: '#0f172a' }}>{value}</div>
          <div style={{ fontSize: 12, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.5 }}>{label}</div>
        </div>
      </div>
    </div>
  )
}
