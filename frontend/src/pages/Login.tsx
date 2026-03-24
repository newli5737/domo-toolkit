import { useState } from 'react'
import { apiPost } from '../api'

interface LoginProps {
  onLoginSuccess: (username: string) => void
}

export default function Login({ onLoginSuccess }: LoginProps) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleInteractiveLogin = async () => {
    setError('')
    setLoading(true)

    try {
      // Backend sẽ mở một cửa sổ Chrome
      const result = await apiPost<{ success: boolean; message: string; username: string }>('/api/auth/login')

      if (result.success) {
        onLoginSuccess(result.username)
      } else {
        setError(result.message)
      }
    } catch (err) {
      console.error("Lỗi khi gọi API login:", err);
      setError(err instanceof Error ? err.message : 'Lỗi kết nối server')
    } finally {
      setLoading(false)
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

          <div className="bg-[var(--color-bg-secondary)] border border-[var(--color-border)] rounded-xl p-6 mb-8">
            <p className="text-sm text-gray-300 leading-relaxed">
              Bạn sẽ được cấp quyền truy cập bằng cách đăng nhập trực tiếp tại trang web của Domo.
              Hệ thống sẽ mở một cửa sổ trình duyệt an toàn.
            </p>
          </div>

          <button
            onClick={handleInteractiveLogin}
            disabled={loading}
            className="w-full py-3.5 rounded-lg bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-purple)] text-white font-semibold text-sm transition-all hover:shadow-lg hover:shadow-[var(--color-accent-blue)]/30 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:transform-none disabled:hover:shadow-none flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Vui lòng thao tác trên cửa sổ mới...
              </>
            ) : (
              'Mở cửa sổ Đăng nhập Domo'
            )}
          </button>

          <p className="text-xs text-gray-500 text-center mt-5">
            Kết nối tới hệ thống nội bộ của công ty
          </p>
        </div>
      </div>
    </div>
  )
}
