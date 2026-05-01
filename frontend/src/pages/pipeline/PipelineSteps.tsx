import { BarChart3 } from 'lucide-react'

interface ModelStep {
  name: string
  duration_ms: number
  row_count: number | null
  error: string | null
}

export default function PipelineSteps({ models }: { models?: ModelStep[] }) {
  if (!models || models.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 60, color: '#94a3b8' }}>
        <BarChart3 style={{ width: 48, height: 48, margin: '0 auto 16px', opacity: 0.3 }} />
        <p style={{ fontSize: 15, fontWeight: 600 }}>No pipeline run data</p>
        <p style={{ fontSize: 13 }}>Run the pipeline to see execution steps</p>
      </div>
    )
  }

  const maxDur = Math.max(...models.map(m => m.duration_ms))

  return (
    <div className="card">
      <div className="card-body" style={{ padding: 0 }}>
        {models.map((model, i) => {
          const pct = model.duration_ms / maxDur * 100
          const isErr = !!model.error
          return (
            <div key={i} style={{
              display: 'flex', alignItems: 'center', gap: 14, padding: '14px 20px',
              borderBottom: i < models.length - 1 ? '1px solid #f1f5f9' : 'none',
            }}>
              <div style={{
                width: 28, height: 28, borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center',
                background: isErr ? '#fee2e2' : '#dcfce7', fontSize: 12, fontWeight: 800,
                color: isErr ? '#dc2626' : '#15803d',
              }}>
                {isErr ? '✗' : '✓'}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a' }}>{model.name}</div>
                <div style={{ display: 'flex', gap: 12, fontSize: 11, color: '#64748b', marginTop: 2 }}>
                  <span>{model.duration_ms.toFixed(0)}ms</span>
                  {model.row_count != null && <span>{model.row_count.toLocaleString()} rows</span>}
                </div>
              </div>
              <div style={{ width: 120, height: 6, borderRadius: 3, background: '#f1f5f9', overflow: 'hidden' }}>
                <div style={{
                  width: `${pct}%`, height: '100%', borderRadius: 3,
                  background: isErr ? '#ef4444' : `hsl(${210 - pct * 1.2}, 80%, 55%)`,
                  transition: 'width 0.5s ease',
                }} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
