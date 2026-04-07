import { Settings as SettingsIcon, ExternalLink, CheckCircle, AlertTriangle, PlayCircle, Mail, Loader, Save, Clock, Calendar, ChevronDown } from 'lucide-react'
import { useI18n } from '../i18n'
import { useMonitorConfig } from '../hooks/useMonitorConfig'

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

  const {
    config,
    form,
    updateForm,
    providerTypes,
    saveConfig,
    isSaving,
    isSaved,
    runAutoCheck,
    isRunning,
    runResult: result,
  } = useMonitorConfig()

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
                <div className="relative">
                  <select value={form.providerType} onChange={e => updateForm({ providerType: e.target.value })}
                    className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100 appearance-none bg-white pr-8">
                    <option value="">{lang === 'vi' ? 'Tất cả loại' : 'すべての種類'}</option>
                    {providerTypes.map(pt => (
                      <option key={pt} value={pt}>{pt}</option>
                    ))}
                  </select>
                  <ChevronDown className="absolute right-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                  {lang === 'vi' ? 'Card tối thiểu (メインDataSet)' : 'メインDataSetの最小カード数'}
                </label>
                <input type="number" value={form.minCards} onChange={e => updateForm({ minCards: Number(e.target.value) })}
                  className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400" min={0} />
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-500 mb-1 uppercase tracking-wide">
                  {lang === 'vi' ? 'Email nhận cảnh báo' : 'アラート送信先メール'}
                </label>
                <input type="text" value={form.alertEmail} onChange={e => updateForm({ alertEmail: e.target.value })}
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
                  onClick={() => updateForm({ scheduleEnabled: !form.scheduleEnabled })}
                  className={`relative w-11 h-6 rounded-full transition-colors duration-200 ${
                    form.scheduleEnabled ? 'bg-blue-500' : 'bg-slate-300'
                  }`}
                >
                  <span className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow-sm transition-transform duration-200 ${
                    form.scheduleEnabled ? 'translate-x-5' : ''
                  }`} />
                </button>
              </div>

              {form.scheduleEnabled && (
                <div className="space-y-3 animate-fadein">
                  {/* Time picker */}
                  <div className="flex items-center gap-3">
                    <Calendar className="w-4 h-4 text-slate-400" />
                    <span className="text-xs text-slate-500 w-12">{lang === 'vi' ? 'Giờ:' : '時刻:'}</span>
                    <select
                      value={form.scheduleHour}
                      onChange={e => updateForm({ scheduleHour: Number(e.target.value) })}
                      className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400"
                    >
                      {Array.from({length: 24}, (_, i) => (
                        <option key={i} value={i}>{String(i).padStart(2, '0')}</option>
                      ))}
                    </select>
                    <span className="text-slate-500">:</span>
                    <select
                      value={form.scheduleMinute}
                      onChange={e => updateForm({ scheduleMinute: Number(e.target.value) })}
                      className="px-2 py-1.5 rounded-lg border border-slate-200 text-sm bg-white focus:outline-none focus:border-blue-400"
                    >
                      {Array.from({length: 60}, (_, m) => (
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
                        const active = form.scheduleDays.split(',').map(s => s.trim()).includes(d.key)
                        return (
                          <button
                            key={d.key}
                            onClick={() => {
                              const current = form.scheduleDays.split(',').map(s => s.trim()).filter(Boolean)
                              const next = active ? current.filter(k => k !== d.key) : [...current, d.key]
                              updateForm({ scheduleDays: next.join(',') })
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
                      ? `Tự động chạy check Dataset + Dataflow lúc ${String(form.scheduleHour).padStart(2,'0')}:${String(form.scheduleMinute).padStart(2,'0')} JST`
                      : `${String(form.scheduleHour).padStart(2,'0')}:${String(form.scheduleMinute).padStart(2,'0')} JSTに自動でDataSet + DataFlowをチェック`}
                  </p>
                </div>
              )}
            </div>

            {/* Save config button */}
            <div className="flex items-center gap-3">
              <button onClick={saveConfig} disabled={isSaving} className="btn btn-outline" style={{padding: '6px 16px'}}>
                {isSaving ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                {lang === 'vi' ? 'Lưu cấu hình' : '設定を保存'}
              </button>
              {isSaved && <span className="text-xs text-green-600">✓ {lang === 'vi' ? 'Đã lưu!' : '保存しました！'}</span>}
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
                result.error ? 'bg-red-50 text-red-700' : 'bg-blue-50 text-blue-700'
              }`}>
                {result.error
                  ? <><AlertTriangle className="w-4 h-4 mt-0.5" /> {result.error}</>
                  : <><CheckCircle className="w-4 h-4 mt-0.5" />
                      <div>{result.message || (lang === 'vi' ? 'Đã bắt đầu chạy Auto-Check ngầm...' : 'Auto-Checkを開始しました...')}</div>
                    </>
                }
              </div>
            )}

            {/* Run button */}
            <button onClick={runAutoCheck} disabled={isRunning} className="btn btn-primary">
              {isRunning ? <Loader className="w-4 h-4 animate-spin" /> : <PlayCircle className="w-4 h-4" />}
              {isRunning
                ? (lang === 'vi' ? 'Đang kiểm tra...' : 'チェック中...')
                : (lang === 'vi' ? 'Chạy Auto-Check' : 'Auto-Checkを実行')}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
