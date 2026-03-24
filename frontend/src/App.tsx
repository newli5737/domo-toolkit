import { useState, useEffect } from 'react'
import { apiGet } from './api'
import Login from './pages/Login'
import BeastModeCleanup from './pages/BeastModeCleanup'

export default function App() {
  const [loggedIn, setLoggedIn] = useState(false)
  const [username, setUsername] = useState('')
  const [checking, setChecking] = useState(true)

  useEffect(() => {
    apiGet<{ logged_in: boolean; username: string }>('/api/auth/status')
      .then(data => {
        if (data.logged_in) {
          setLoggedIn(true)
          setUsername(data.username)
        }
      })
      .catch(() => {})
      .finally(() => setChecking(false))
  }, [])

  const handleLogin = (user: string) => {
    setLoggedIn(true)
    setUsername(user)
  }

  if (checking) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-white/20 border-t-[var(--color-accent-cyan)] rounded-full animate-spin" />
      </div>
    )
  }

  if (!loggedIn) {
    return <Login onLoginSuccess={handleLogin} />
  }

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="sticky top-0 z-50 backdrop-blur-xl bg-[var(--color-bg-primary)]/85 border-b border-[var(--color-border)] px-8 h-16 flex items-center justify-between">
        <h1 className="text-lg font-bold bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)] bg-clip-text text-transparent tracking-tight">
          DOMO Toolkit
        </h1>

        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2.5 px-4 py-1.5 rounded-full bg-white/[0.03] border border-[var(--color-border)] text-sm text-gray-400">
            <span className="w-2 h-2 rounded-full bg-[var(--color-accent-green)] shadow-[0_0_8px] shadow-[var(--color-accent-green)]" />
            {username}
          </div>
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 p-8 max-w-[1400px] mx-auto w-full">
        <BeastModeCleanup />
      </main>
    </div>
  )
}
