import { useState, useEffect } from 'react'
import { ChevronLeft, Save, FileSpreadsheet, CreditCard, Columns3 } from 'lucide-react'
import { apiGet, apiPut } from '../../api'
import CardViewer from './CardViewer'

interface ColumnInfo { name: string; type: string; samples: (string | null)[]; null_count: number; distinct_count: number }
interface CardInfo { id: number; title: string; endpoint: string; chart_type: string; description: string }
interface DetailData {
  exists: boolean; display_name: string; total_rows: number; column_count: number
  columns: ColumnInfo[]; cards: CardInfo[]; last_modified: number; size_bytes: number
}

export default function DatasetDetail({ dataflowId, onBack }: { dataflowId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<DetailData | null>(null)
  const [editName, setEditName] = useState('')
  const [saving, setSaving] = useState(false)
  const [activeCard, setActiveCard] = useState<CardInfo | null>(null)

  useEffect(() => {
    apiGet<DetailData>(`/api/pipeline/datasets/detail?dataflow_id=${dataflowId}`)
      .then(d => { setDetail(d); setEditName(d.display_name) }).catch(() => {})
  }, [dataflowId])

  const saveName = async () => {
    setSaving(true)
    try {
      await apiPut('/api/pipeline/datasets/rename', { dataflow_id: dataflowId, display_name: editName })
      setDetail(prev => prev ? { ...prev, display_name: editName } : prev)
    } catch { /* ignore */ }
    setSaving(false)
  }

  if (!detail) return <div style={{ padding: 40, textAlign: 'center' }}><div className="spinner" style={{ margin: '0 auto' }} /></div>

  return (
    <div className="animate-fadein">
      {/* Back button */}
      <button onClick={onBack} className="btn btn-outline" style={{ marginBottom: 16, fontSize: 13 }}>
        <ChevronLeft style={{ width: 16, height: 16 }} /> Back to Datasets
      </button>

      {/* Header card */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-body" style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
          <FileSpreadsheet style={{ width: 28, height: 28, color: '#8b5cf6' }} />
          <div style={{ flex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <input value={editName} onChange={e => setEditName(e.target.value)}
                style={{ fontSize: 16, fontWeight: 700, border: '1px solid #e2e8f0', borderRadius: 6, padding: '4px 10px', minWidth: 300 }} />
              {editName !== detail.display_name && (
                <button onClick={saveName} disabled={saving} className="btn btn-primary" style={{ padding: '4px 12px', fontSize: 12 }}>
                  <Save style={{ width: 14, height: 14 }} /> Save
                </button>
              )}
            </div>
            <div style={{ fontSize: 12, color: '#64748b', marginTop: 4 }}>
              {detail.total_rows.toLocaleString()} rows · {detail.column_count} columns · {(detail.size_bytes / 1024 / 1024).toFixed(1)} MB
              · Last run: {new Date(detail.last_modified * 1000).toLocaleString()}
            </div>
          </div>
        </div>
      </div>

      {/* Columns table */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Columns3 style={{ width: 16, height: 16, color: '#3b82f6' }} /> Columns ({detail.column_count})
        </div>
        <div className="card-body table-wrapper" style={{ maxHeight: 400, overflowY: 'auto', padding: 0 }}>
          <table className="data-table" style={{ fontSize: 12 }}>
            <thead>
              <tr><th>#</th><th>Name</th><th>Type</th><th style={{ textAlign: 'right' }}>Distinct</th><th style={{ textAlign: 'right' }}>Nulls</th><th>Sample</th></tr>
            </thead>
            <tbody>
              {detail.columns.map((col, i) => (
                <tr key={i}>
                  <td style={{ color: '#94a3b8' }}>{i + 1}</td>
                  <td style={{ fontWeight: 600 }}>{col.name}</td>
                  <td><span style={{ padding: '2px 6px', borderRadius: 4, background: '#f1f5f9', fontSize: 11 }}>{col.type}</span></td>
                  <td style={{ textAlign: 'right' }}>{col.distinct_count.toLocaleString()}</td>
                  <td style={{ textAlign: 'right' }}>{col.null_count.toLocaleString()}</td>
                  <td style={{ color: '#64748b', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {col.samples.filter(Boolean).join(' | ')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Linked Cards */}
      <div className="card">
        <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <CreditCard style={{ width: 16, height: 16, color: '#8b5cf6' }} /> Linked Cards ({detail.cards.length})
        </div>
        <div className="card-body" style={{ padding: 0 }}>
          {detail.cards.map((card, i) => (
            <div key={card.id} onClick={() => setActiveCard(activeCard?.id === card.id ? null : card)}
              style={{
                display: 'flex', alignItems: 'center', gap: 12, padding: '12px 20px', cursor: 'pointer',
                borderBottom: i < detail.cards.length - 1 ? '1px solid #f1f5f9' : 'none',
                background: activeCard?.id === card.id ? '#f0f4ff' : 'transparent',
                transition: 'background 0.15s',
              }}>
              <CreditCard style={{ width: 18, height: 18, color: activeCard?.id === card.id ? '#3b82f6' : '#94a3b8' }} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a' }}>{card.title}</div>
                <div style={{ fontSize: 11, color: '#64748b' }}>#{card.id} · {card.chart_type} · {card.description}</div>
              </div>
              <span style={{ fontSize: 11, color: '#3b82f6', fontWeight: 600 }}>{activeCard?.id === card.id ? '▲ Close' : '▼ Open'}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Active Card Viewer */}
      {activeCard && (
        <CardViewer
          dataflowId={dataflowId}
          cardEndpoint={activeCard.endpoint}
          cardTitle={activeCard.title}
          onClose={() => setActiveCard(null)}
        />
      )}
    </div>
  )
}
