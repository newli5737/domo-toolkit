import { useState, useEffect } from 'react'
import { AlertTriangle, ExternalLink, RefreshCw, CheckCircle, Database, Workflow } from 'lucide-react'
import { apiGet } from '../api'
import { useI18n } from '../i18n'

const DOMO_BASE = 'https://astecpaints-co-jp.domo.com'

interface AlertData {
  checked_at: string | null
  all_ok: boolean
  failed_datasets: Array<{id: string; name: string; provider_type: string; last_execution_state: string; card_count: number}>
  failed_dataflows: Array<{id: string; name: string; last_execution_state: string}>
}

export default function Alert() {
  const { lang } = useI18n()
  const [data, setData] = useState<AlertData | null>(null)
  const [loading, setLoading] = useState(false)

  const load = () => {
    setLoading(true)
    apiGet<AlertData>('/api/monitor/alerts')
      .then(d => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  return (
    <div>
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <AlertTriangle className="w-6 h-6 text-amber-500" />
          {lang === 'vi' ? 'Cảnh báo' : 'アラート'}
        </h1>
        <p>{lang === 'vi' ? 'Datasets và Dataflows có vấn đề' : '問題のあるDataSetとDataFlow'}</p>
      </div>

      <div className="page-body space-y-6 animate-fadein">
        {/* Status banner */}
        <div className={`card ${data?.all_ok ? '' : ''}`}>
          <div className="card-body flex items-center justify-between">
            <div className="flex items-center gap-3">
              {data?.all_ok
                ? <CheckCircle className="w-5 h-5 text-green-500" />
                : <AlertTriangle className="w-5 h-5 text-amber-500" />}
              <div>
                <div className="font-medium">
                  {data?.all_ok
                    ? (lang === 'vi' ? 'Tất cả bình thường' : 'すべて正常')
                    : (lang === 'vi'
                        ? `${(data?.failed_datasets?.length || 0) + (data?.failed_dataflows?.length || 0)} vấn đề phát hiện`
                        : `${(data?.failed_datasets?.length || 0) + (data?.failed_dataflows?.length || 0)}件の問題を検出`
                    )}
                </div>
                {data?.checked_at && (
                  <div className="text-xs text-slate-400">
                    {lang === 'vi' ? 'Kiểm tra lúc' : '確認時刻'}: {new Date(data.checked_at).toLocaleString(lang === 'vi' ? 'vi-VN' : 'ja-JP')}
                  </div>
                )}
                {!data?.checked_at && (
                  <div className="text-xs text-slate-400">
                    {lang === 'vi' ? 'Chưa kiểm tra. Hãy chạy Auto-Check trong Settings.' : '未確認。設定でAuto-Checkを実行してください。'}
                  </div>
                )}
              </div>
            </div>
            <button onClick={load} disabled={loading} className="btn btn-outline" style={{padding: '6px 12px'}}>
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>

        {/* Failed Datasets */}
        {data && data.failed_datasets && data.failed_datasets.length > 0 && (
          <div className="card">
            <div className="card-header flex items-center gap-2">
              <Database className="w-4 h-4 text-red-500" />
              {lang === 'vi' ? 'Datasets lỗi' : 'エラーDataSet'} ({data.failed_datasets.length})
            </div>
            <div className="table-wrapper">
              <table className="data-table">
                <thead><tr>
                  <th>{lang === 'vi' ? 'Tên' : '名前'}</th>
                  <th>{lang === 'vi' ? 'Loại' : '種類'}</th>
                  <th>{lang === 'vi' ? 'Trạng thái' : 'ステータス'}</th>
                  <th>Cards</th>
                  <th></th>
                </tr></thead>
                <tbody>
                  {data.failed_datasets.map(ds => (
                    <tr key={ds.id}>
                      <td className="font-medium max-w-[300px] truncate">{ds.name}</td>
                      <td><span className="badge badge-neutral">{ds.provider_type}</span></td>
                      <td><span className="badge badge-failed">{ds.last_execution_state}</span></td>
                      <td>{ds.card_count || '-'}</td>
                      <td>
                        <a href={`${DOMO_BASE}/datasources/${ds.id}/details/overview`} target="_blank" rel="noopener noreferrer"
                          className="text-slate-400 hover:text-blue-500 transition-colors">
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Failed Dataflows */}
        {data && data.failed_dataflows && data.failed_dataflows.length > 0 && (
          <div className="card">
            <div className="card-header flex items-center gap-2">
              <Workflow className="w-4 h-4 text-red-500" />
              {lang === 'vi' ? 'Dataflows lỗi' : 'エラーDataFlow'} ({data.failed_dataflows.length})
            </div>
            <div className="table-wrapper">
              <table className="data-table">
                <thead><tr>
                  <th>{lang === 'vi' ? 'Tên' : '名前'}</th>
                  <th>{lang === 'vi' ? 'Trạng thái' : 'ステータス'}</th>
                  <th></th>
                </tr></thead>
                <tbody>
                  {data.failed_dataflows.map(df => (
                    <tr key={df.id}>
                      <td className="font-medium max-w-[300px] truncate">{df.name}</td>
                      <td><span className="badge badge-failed">{df.last_execution_state}</span></td>
                      <td>
                        <a href={`${DOMO_BASE}/datacenter/dataflows/${df.id}/details#settings`} target="_blank" rel="noopener noreferrer"
                          className="text-slate-400 hover:text-purple-500 transition-colors">
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
