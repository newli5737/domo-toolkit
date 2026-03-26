import { useState, useEffect } from 'react'
import { Settings as SettingsIcon, ExternalLink, CheckCircle, AlertTriangle, PlayCircle, Mail, Loader, Save, Clock, Calendar } from 'lucide-react'
import { apiGet, apiPost } from '../api'
import { useI18n } from '../i18n'

interface AutoCheckConfig {
  backlog_base_url: string
  backlog_issue_id: string
  has_backlog_cookie: boolean
  alert_email_to: string
  min_card_count: number
  provider_type: string
  has_gmail: boolean
  schedule_enabled: boolean
  schedule_hour: number
  schedule_minute: number
  schedule_days: string
}

const DAYS = [
  { key: 'mon', vi: 'T2', ja: '月' },
  { key: 'tue', vi: 'T3', ja: '火' },
  { key: 'wed', vi: 'T4', ja: '水' },
  { key: 'thu', vi: 'T5', ja: '木' },
  { key: 'fri', vi: 'T6', ja: '金' },
  { key: 'sat', vi: 'T7', ja: '土' },
  { key: 'sun', vi: 'CN', ja: '日' },
]

export default function Settings() {
  const { lang } = useI18n()

  const [config, setConfig] = useState<AutoCheckConfig | null>(null)
  const [minCards, setMinCards] = useState(40)
  const [providerType, setProviderType] = useState('mysql-ssh')
  const [alertEmail, setAlertEmail] = useState('')
  const [commentOk, setCommentOk] = useState(
    '【1次データ取得エラー確認結果】\nエラーがなかった旨\n\n【メインDataSetエラー確認結果】\nエラーがなかった旨'
  )
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<any>(null)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [scheduleEnabled, setScheduleEnabled] = useState(false)
  const [scheduleHour, setScheduleHour] = useState(8)
  const [scheduleMinute, setScheduleMinute] = useState(0)
  const [scheduleDays, setScheduleDays] = useState('mon,tue,wed,thu,fri')

  useEffect(() => {
    apiGet<AutoCheckConfig>('/api/monitor/auto-check-config')
      .then(d => {
        setConfig(d)
        if (d.alert_email_to) setAlertEmail(d.alert_email_to)
        if (d.min_card_count) setMinCards(d.min_card_count)
        if (d.provider_type) setProviderType(d.provider_type)
        setScheduleEnabled(d.schedule_enabled ?? false)
        setScheduleHour(d.schedule_hour ?? 8)
        setScheduleMinute(d.schedule_minute ?? 0)
        setScheduleDays(d.schedule_days ?? 'mon,tue,wed,thu,fri')
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
        provider_type: providerType,
        schedule_enabled: scheduleEnabled,
        schedule_hour: scheduleHour,
        schedule_minute: scheduleMinute,
        schedule_days: scheduleDays,
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
        provider_type: providerType,
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
            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                  {lang === 'vi' ? 'Import Type (điều kiện lọc)' : 'Import Type（フィルター条件）'}
                </label>
                <input type="text" value={providerType} onChange={e => setProviderType(e.target.value)}
                  placeholder="mysql-ssh"
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
              </div>
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
                <input type="text" value={alertEmail} onChange={e => setAlertEmail(e.target.value)}
                  placeholder="user1@example.com, user2@example.com"
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" />
                <p className="text-[10px] text-slate-400 mt-1">{lang === 'vi' ? 'Nhiều email cách nhau bằng dấu phẩy' : '複数のメールはカンマで区切り'}</p>
              </div>
            </div>
            <p className="text-[10px] text-slate-400 mt-1">
              {lang === 'vi'
                ? 'ℹ️ Auto-check sẽ lọc dataset theo Import Type VÀ Card ≥ giá trị trên. Nếu tất cả OK → đăng Backlog. Nếu dataflow lỗi → gửi email.'
                : 'ℹ️ Auto-checkはImport TypeかつCard≥上記値でDataSetをフィルター。全てOK→Backlog投稿。DataFlowエラー→メール送信。'}
            </p>

            {/* ─── Schedule Config ─── */}
            <div className="p-4 rounded-xl bg-slate-50 border border-slate-200 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Clock className="w-4 h-4 text-blue-500" />
                  <span className="text-sm font-semibold text-slate-700">
                    {lang === 'vi' ? 'Lịch Auto-Check tự động' : '自動チェックスケジュール'}
                  </span>
                </div>
                <button
                  onClick={() => setScheduleEnabled(!scheduleEnabled)}
                  className={`relative w-11 h-6 rounded-full transition-colors duration-200 ${
                    scheduleEnabled ? 'bg-blue-500' : 'bg-slate-300'
                  }`}
                >
                  <span className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow-sm transition-transform duration-200 ${
                    scheduleEnabled ? 'translate-x-5' : ''
                  }`} />
                </button>
              </div>

              {scheduleEnabled && (
                <div className="space-y-3 animate-fadein">
                  {/* Time picker */}
                  <div className="flex items-center gap-3">
                    <Calendar className="w-4 h-4 text-slate-400" />
                    <span className="text-xs text-slate-500 w-12">{lang === 'vi' ? 'Giờ:' : '時刻:'}</span>
                    <select
                      value={scheduleHour}
                      onChange={e => setScheduleHour(Number(e.target.value))}
                      className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400"
                    >
                      {Array.from({length: 24}, (_, i) => (
                        <option key={i} value={i}>{String(i).padStart(2, '0')}</option>
                      ))}
                    </select>
                    <span className="text-slate-500">:</span>
                    <select
                      value={scheduleMinute}
                      onChange={e => setScheduleMinute(Number(e.target.value))}
                      className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400"
                    >
                      {[0, 15, 30, 45].map(m => (
                        <option key={m} value={m}>{String(m).padStart(2, '0')}</option>
                      ))}
                    </select>
                    <span className="text-xs text-slate-400">JST</span>
                  </div>

                  {/* Day checkboxes */}
                  <div className="flex items-center gap-3">
                    <Calendar className="w-4 h-4 text-slate-400" />
                    <span className="text-xs text-slate-500 w-12">{lang === 'vi' ? 'Ngày:' : '曜日:'}</span>
                    <div className="flex gap-1">
                      {DAYS.map(d => {
                        const active = scheduleDays.split(',').map(s => s.trim()).includes(d.key)
                        return (
                          <button
                            key={d.key}
                            onClick={() => {
                              const current = scheduleDays.split(',').map(s => s.trim()).filter(Boolean)
                              const next = active ? current.filter(k => k !== d.key) : [...current, d.key]
                              setScheduleDays(next.join(','))
                            }}
                            className={`w-8 h-8 rounded-lg text-xs font-semibold transition-all ${
                              active
                                ? 'bg-blue-500 text-white shadow-sm'
                                : 'bg-white text-slate-400 border border-slate-200 hover:border-blue-300'
                            }`}
                          >
                            {lang === 'vi' ? d.vi : d.ja}
                          </button>
                        )
                      })}
                    </div>
                  </div>

                  <p className="text-[10px] text-slate-400 pl-8">
                    {lang === 'vi'
                      ? `Tự động chạy check Dataset + Dataflow lúc ${String(scheduleHour).padStart(2,'0')}:${String(scheduleMinute).padStart(2,'0')} JST`
                      : `${String(scheduleHour).padStart(2,'0')}:${String(scheduleMinute).padStart(2,'0')} JSTに自動でDataSet + DataFlowをチェック`}
                  </p>
                </div>
              )}
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
