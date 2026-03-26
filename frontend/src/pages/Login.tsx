import { useState, useRef, useEffect } from 'react'
import { LogIn, Upload, ExternalLink, KeyRound, Loader } from 'lucide-react'
import { apiPost, apiGet } from '../api'
import { useI18n } from '../i18n'

interface LoginProps {
  onLoginSuccess: (username: string) => void
}

export default function Login({ onLoginSuccess }: LoginProps) {
  const { t } = useI18n()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [successMsg, setSuccessMsg] = useState('')
  const [domoUrl, setDomoUrl] = useState('')
  const [tab, setTab] = useState<'auto' | 'cookie'>('auto')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    apiGet<{ domo_url: string }>('/api/auth/status')
      .then(res => setDomoUrl(res.domo_url))
      .catch(() => {})
  }, [])

  const handleAutoLogin = async () => {
    setError(''); setSuccessMsg(''); setLoading(true)
    try {
      const result = await apiPost<{ success: boolean; message: string; username: string }>('/api/auth/login', { username, password })
      if (result.success) { setSuccessMsg(result.message); setTimeout(() => onLoginSuccess(result.username), 1000) }
      else setError(result.message)
    } catch (err) { setError(err instanceof Error ? err.message : 'Login failed') }
    finally { setLoading(false) }
  }

  const handleCookieUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setError(''); setSuccessMsg(''); setLoading(true)
    try {
      const text = await file.text()
      const cookieData = JSON.parse(text)
      const result = await apiPost<{ success: boolean; message: string; username: string }>('/api/auth/upload-cookies', cookieData)
      if (result.success) { setSuccessMsg(result.message); setTimeout(() => onLoginSuccess(result.username), 1000) }
      else setError(result.message)
    } catch (err) { setError(err instanceof Error ? err.message : 'Invalid JSON') }
    finally { setLoading(false); if (fileInputRef.current) fileInputRef.current.value = '' }
  }

  return (
    <div className="animate-fadein">
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <KeyRound className="w-6 h-6 text-blue-500" /> {t('login.title')}
        </h1>
        <p>{t('login.desc')}</p>
      </div>

      <div className="page-body">
        <div className="max-w-lg mx-auto">
          <div className="card">
            <div className="flex border-b border-slate-200">
              <button onClick={() => setTab('auto')}
                className={`flex-1 py-3 text-sm font-semibold text-center transition-all border-b-2 ${
                  tab === 'auto' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-600'
                }`}>
                <LogIn className="w-4 h-4 inline mr-1.5 -mt-0.5" /> {t('login.userPass')}
              </button>
              <button onClick={() => setTab('cookie')}
                className={`flex-1 py-3 text-sm font-semibold text-center transition-all border-b-2 ${
                  tab === 'cookie' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-600'
                }`}>
                <Upload className="w-4 h-4 inline mr-1.5 -mt-0.5" /> {t('login.cookieImport')}
              </button>
            </div>

            <div className="card-body">
              {error && <div className="mb-4 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-600 text-sm">{error}</div>}
              {successMsg && <div className="mb-4 px-4 py-3 rounded-lg bg-green-50 border border-green-200 text-green-600 text-sm">{successMsg}</div>}

              {tab === 'auto' ? (
                <div className="space-y-4">
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('login.email')}</label>
                    <input type="email" value={username} onChange={e => setUsername(e.target.value)} placeholder="your@email.com"
                      className="w-full px-3 py-2.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-100" />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-slate-500 mb-1.5 uppercase tracking-wide">{t('login.password')}</label>
                    <input type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Password"
                      className="w-full px-3 py-2.5 rounded-lg border border-slate-200 text-sm focus:outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-100" />
                  </div>
                  <p className="text-xs text-slate-400">{t('login.useEnv')}</p>
                  <button onClick={handleAutoLogin} disabled={loading} className="btn btn-primary w-full justify-center">
                    {loading ? <Loader className="w-4 h-4 animate-spin" /> : <LogIn className="w-4 h-4" />}
                    {loading ? t('login.loggingIn') : t('common.login')}
                  </button>
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="p-4 rounded-lg bg-slate-50 border border-slate-200 text-sm text-slate-600">
                    <p className="font-semibold text-slate-700 mb-2">{t('login.steps')}:</p>
                    <ol className="list-decimal list-inside space-y-1.5 text-[13px]">
                      <li>{t('login.step1')}</li>
                      <li>{t('login.step2')}</li>
                      <li>{t('login.step3')}</li>
                    </ol>
                  </div>
                  {domoUrl && (
                    <a href={`${domoUrl}/auth/index`} target="_blank" rel="noopener noreferrer"
                      className="btn btn-outline w-full justify-center no-underline">
                      <ExternalLink className="w-4 h-4" /> {t('login.openDomo')}
                    </a>
                  )}
                  <input ref={fileInputRef} type="file" accept=".json" onChange={handleCookieUpload} className="hidden" id="cookie-upload" />
                  <button onClick={() => fileInputRef.current?.click()} disabled={loading} className="btn btn-primary w-full justify-center">
                    {loading ? <Loader className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
                    {loading ? t('login.processing') : t('login.uploadCookie')}
                  </button>
                </div>
              )}
            </div>
          </div>
          <p className="text-xs text-slate-400 text-center mt-4">
            {t('login.connectingTo')} {domoUrl ? new URL(domoUrl).hostname : 'your Domo instance'}
          </p>
        </div>
      </div>
    </div>
  )
}
