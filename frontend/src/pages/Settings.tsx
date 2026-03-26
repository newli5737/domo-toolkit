import { useState, useEffect } from 'react'
import { Settings as SettingsIcon, ExternalLink, CheckCircle, AlertTriangle, PlayCircle, Mail, Loader, Save } from 'lucide-react'
import { apiGet, apiPost } from '../api'
import { useI18n } from '../i18n'

interface AutoCheckConfig {
  backlog_base_url: string
  backlog_issue_id: string
  has_backlog_cookie: boolean
  alert_email_to: string
  min_card_count: number
  has_gmail: boolean
}

export default function Settings() {
  const { lang } = useI18n()

  const [config, setConfig] = useState<AutoCheckConfig | null>(null)
  const [minCards, setMinCards] = useState(40)
  const [alertEmail, setAlertEmail] = useState('')
  const [commentOk, setCommentOk] = useState(
    '【1次データ取得エラー確認結果】\nエラーがなかった旨\n\n【メインDataSetエラー確認結果】\nエラーがなかった旨'
  )
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<any>(null)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    apiGet<AutoCheckConfig>('/api/monitor/auto-check-config')
      .then(d => {
        setConfig(d)
        if (d.alert_email_to) setAlertEmail(d.alert_email_to)
        if (d.min_card_count) setMinCards(d.min_card_count)
      })
      .catch(() => {})
  }, [])

  const saveConfig = async () => {
    setSaving(true)
    setSaved(false)
    try {
      await apiPost('/api/monitor/save-alert-config', {
        alert_email: alertEmail,
        min_card_count: minCards,
      })
      setSaved(true)
      setTimeout(() => setSaved(false), 3000)
    } catch {}
    finally { setSaving(false) }
  }

  const runAutoCheck = async () => {
    setRunning(true)
    setResult(null)
    try {
      const res = await apiPost<any>('/api/monitor/auto-check', {
        min_card_count: minCards,
        comment_ok: commentOk,
        alert_email: alertEmail,
      })
      setResult(res)
    } catch (err) {
      setResult({ error: err instanceof Error ? err.message : 'Error' })
    } finally {
      setRunning(false)
    }
  }

  return (
    <div>
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <SettingsIcon className="w-6 h-6 text-slate-500" />
          {lang === 'vi' ? 'Cài đặt giám sát' : '監視設定'}
        </h1>
        <p>{lang === 'vi' ? 'Cấu hình tự động kiểm tra và thông báo' : '自動チェックと通知の設定'}</p>
      </div>

      <div className="page-body space-y-6 animate-fadein">
        {/* ─── Config Status ─── */}
        <div className="card">
          <div className="card-header">{lang === 'vi' ? 'Trạng thái kết nối' : '接続ステータス'}</div>
          <div className="card-body">
            <div className="grid grid-cols-2 gap-4">
              <div className="p-3 rounded-lg bg-slate-50 border border-slate-100">
                <div className="flex items-center gap-2 mb-1">
                  <ExternalLink className="w-3.5 h-3.5 text-green-500" />
                  <span className="text-xs font-semibold text-slate-500 uppercase">Backlog</span>
                  <span className={`ml-auto badge ${config?.has_backlog_cookie ? 'badge-success' : 'badge-failed'}`} style={{fontSize: '10px'}}>
                    {config?.has_backlog_cookie ? 'OK' : 'N/A'}
                  </span>
                </div>
                <div className="text-xs text-slate-400 space-y-0.5">
                  <div>Issue: <code className="bg-slate-100 px-1 rounded">{config?.backlog_issue_id || '—'}</code></div>
                  <div>URL: <code className="bg-slate-100 px-1 rounded text-[10px]">{config?.backlog_base_url || '—'}</code></div>
                </div>
              </div>
              <div className="p-3 rounded-lg bg-slate-50 border border-slate-100">
                <div className="flex items-center gap-2 mb-1">
                  <Mail className="w-3.5 h-3.5 text-blue-500" />
                  <span className="text-xs font-semibold text-slate-500 uppercase">Email</span>
                  <span className={`ml-auto badge ${config?.has_gmail ? 'badge-success' : 'badge-failed'}`} style={{fontSize: '10px'}}>
                    {config?.has_gmail ? 'OK' : 'N/A'}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* ─── Auto-Check Config ─── */}
        <div className="card">
          <div className="card-header">{lang === 'vi' ? 'Cấu hình Auto-Check' : 'Auto-Check設定'}</div>
          <div className="card-body space-y-4">
            {/* Card threshold + Alert email */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                  {lang === 'vi' ? 'Card tối thiểu (メインDataSet)' : 'メインDataSetの最小カード数'}
                </label>
                <input type="number" value={minCards} onChange={e => setMinCards(Number(e.target.value))}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" min={0} />
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                  {lang === 'vi' ? 'Email nhận cảnh báo' : 'アラート送信先メール'}
                </label>
                <input type="email" value={alertEmail} onChange={e => setAlertEmail(e.target.value)}
                  placeholder="user@example.com"
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
              </div>
            </div>

            {/* Save config button */}
            <div className="flex items-center gap-3">
              <button onClick={saveConfig} disabled={saving} className="btn btn-outline" style={{padding: '6px 16px'}}>
                {saving ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                {lang === 'vi' ? 'Lưu cấu hình' : '設定を保存'}
              </button>
              {saved && <span className="text-xs text-green-600">✓ {lang === 'vi' ? 'Đã lưu!' : '保存しました！'}</span>}
            </div>

            {/* Comment template */}
            <div>
              <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                {lang === 'vi' ? 'Comment khi OK (gửi Backlog)' : 'OK時のコメント（Backlog投稿）'}
              </label>
              <textarea value={commentOk} onChange={e => setCommentOk(e.target.value)}
                rows={5}
                className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 resize-none font-mono" />
            </div>

            {/* Info */}
            <div className="p-3 rounded-lg bg-blue-50 border border-blue-100 text-xs text-blue-700">
              {lang === 'vi'
                ? '• Nếu tất cả OK → tự post comment lên Backlog\n• Nếu có lỗi → hiển thị trang Cảnh báo + gửi email'
                : '• すべてOKの場合 → Backlogにコメントを自動投稿\n• エラーがある場合 → アラートページに表示 + メール送信'}
            </div>

            {/* Result */}
            {result && (
              <div className={`p-3 rounded-lg text-sm flex items-start gap-2 ${
                result.error ? 'bg-red-50 text-red-700' :
                result.all_ok ? 'bg-green-50 text-green-700' : 'bg-amber-50 text-amber-700'
              }`}>
                {result.error
                  ? <><AlertTriangle className="w-4 h-4 mt-0.5" /> {result.error}</>
                  : result.all_ok
                    ? <><CheckCircle className="w-4 h-4 mt-0.5" />
                        <div>
                          {lang === 'vi' ? 'Tất cả OK!' : 'すべてOK！'}
                          {result.backlog_posted && <span className="ml-2 text-xs opacity-70">✓ Backlog posted</span>}
                        </div>
                      </>
                    : <><AlertTriangle className="w-4 h-4 mt-0.5" />
                        <div>
                          {lang === 'vi'
                            ? `${result.failed_dataset_count} dataset + ${result.failed_dataflow_count} dataflow lỗi`
                            : `${result.failed_dataset_count}件のDataSet + ${result.failed_dataflow_count}件のDataFlowエラー`}
                          {result.email_sent && <span className="ml-2 text-xs opacity-70">✓ Email sent</span>}
                        </div>
                      </>
                }
              </div>
            )}

            {/* Run button */}
            <button onClick={runAutoCheck} disabled={running} className="btn btn-primary">
              {running ? <Loader className="w-4 h-4 animate-spin" /> : <PlayCircle className="w-4 h-4" />}
              {running
                ? (lang === 'vi' ? 'Đang kiểm tra...' : 'チェック中...')
                : (lang === 'vi' ? 'Chạy Auto-Check' : 'Auto-Checkを実行')}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
