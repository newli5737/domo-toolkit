import { useState, useRef, useEffect } from 'react'
import { apiPost, apiGet } from '../api'

interface LoginProps {
  onLoginSuccess: (username: string) => void
}

export default function Login({ onLoginSuccess }: LoginProps) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [successMsg, setSuccessMsg] = useState('')
  const [domoUrl, setDomoUrl] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    apiGet<{ domo_url: string }>('/api/auth/status')
      .then(res => setDomoUrl(res.domo_url))
      .catch(() => {})
  }, [])

  const handleCookieUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    setError('')
    setSuccessMsg('')
    setLoading(true)

    try {
      const text = await file.text()
      const cookieData = JSON.parse(text)

      const result = await apiPost<{ success: boolean; message: string; username: string }>(
        '/api/auth/upload-cookies',
        cookieData
      )

      if (result.success) {
        setSuccessMsg(result.message)
        setTimeout(() => onLoginSuccess(result.username), 1000)
      } else {
        setError(result.message)
      }
    } catch (err) {
      console.error("Lỗi upload cookie:", err)
      setError(err instanceof Error ? err.message : 'File JSON không hợp lệ')
    } finally {
      setLoading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div className="w-full max-w-md">
        {/* Logo / Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-[var(--color-accent-blue)] to-[var(--color-accent-purple)] mb-4 shadow-lg shadow-[var(--color-accent-blue)]/20">
            <svg className="w-8 h-8 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
            </svg>
          </div>
          <h1 className="text-3xl font-extrabold bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)] bg-clip-text text-transparent">
            DOMO Toolkit
          </h1>
          <p className="text-sm text-gray-400 mt-2">Đăng nhập tài khoản Domo để bắt đầu</p>
        </div>

        {/* Card */}
        <div className="bg-[var(--color-bg-card)] border border-[var(--color-border)] rounded-2xl p-10 shadow-2xl shadow-black/30 text-center">
          {error && (
            <div className="mb-6 px-4 py-3 rounded-lg bg-red-500/10 border border-red-500/20 text-[var(--color-accent-red)] text-sm text-left">
              {error}
            </div>
          )}

          {successMsg && (
            <div className="mb-6 px-4 py-3 rounded-lg bg-green-500/10 border border-green-500/20 text-green-400 text-sm text-left">
              ✅ {successMsg}
            </div>
          )}

          {/* Hướng dẫn */}
          <div className="bg-[var(--color-bg-secondary)] border border-[var(--color-border)] rounded-xl p-6 mb-6 text-left">
            <p className="text-sm font-semibold text-gray-200 mb-3">Hướng dẫn:</p>
            <ol className="text-sm text-gray-400 space-y-2 list-decimal list-inside">
              <li>Mở trang Domo và đăng nhập</li>
              <li>Dùng <strong className="text-gray-300">J2 Team Cookie</strong> extension để xuất cookies</li>
              <li>Upload file <code className="text-[var(--color-accent-cyan)]">.json</code> bên dưới</li>
            </ol>
          </div>

          {/* Mở Domo */}
          {domoUrl && (
            <a
              href={`${domoUrl}/auth/index`}
              target="_blank"
              rel="noopener noreferrer"
              className="w-full py-3.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-purple)] text-white font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-blue)]/30 hover:-translate-y-0.5 flex items-center justify-center gap-2 mb-4 no-underline"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
              </svg>
              Mở trang Domo Login
            </a>
          )}

          {/* Cookie Upload */}
          <input
            ref={fileInputRef}
            type="file"
            accept=".json"
            onChange={handleCookieUpload}
            className="hidden"
            id="cookie-upload"
          />
          <button
            onClick={() => fileInputRef.current?.click()}
            disabled={loading}
            className="w-full py-3.5 rounded-lg bg-[var(--color-bg-secondary)] border border-dashed border-[var(--color-border)] text-gray-300 font-semibold text-sm transition-all hover:border-[var(--color-accent-cyan)]/50 hover:text-white hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:transform-none flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Đang xử lý...
              </>
            ) : (
              <>
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                </svg>
                Upload Cookie JSON
              </>
            )}
          </button>

          <p className="text-xs text-gray-500 text-center mt-5">
            Kết nối tới {domoUrl ? new URL(domoUrl).hostname : 'hệ thống nội bộ'}
          </p>
        </div>
      </div>
    </div>
  )
}
