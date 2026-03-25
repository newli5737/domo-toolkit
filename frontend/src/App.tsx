import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom'
import { apiGet } from './api'
import Login from './pages/Login'
import BeastModeCleanup from './pages/BeastModeCleanup'

function NavBar({ loggedIn, username }: { loggedIn: boolean; username: string }) {
  const location = useLocation()
  const isAdmin = location.pathname === '/admin'

  return (
    <header className="sticky top-0 z-50 backdrop-blur-xl bg-[var(--color-bg-primary)]/85 border-b border-[var(--color-border)] px-8 h-16 flex items-center justify-between">
      <div className="flex items-center gap-6">
        <h1 className="text-lg font-bold bg-gradient-to-r from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)] bg-clip-text text-transparent tracking-tight">
          DOMO Toolkit
        </h1>
        <nav className="flex gap-1 p-1 bg-white/[0.03] rounded-lg border border-[var(--color-border)]">
          <Link
            to="/"
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
              !isAdmin
                ? 'bg-[var(--color-accent-blue)] text-white shadow-md'
                : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            📊 Dashboard
          </Link>
          <Link
            to="/admin"
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
              isAdmin
                ? 'bg-[var(--color-accent-blue)] text-white shadow-md'
                : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            ⚙️ Admin
          </Link>
        </nav>
      </div>

      <div className="flex items-center gap-4">
        {loggedIn ? (
          <div className="flex items-center gap-2.5 px-4 py-1.5 rounded-full bg-white/[0.03] border border-[var(--color-border)] text-sm text-gray-400">
            <span className="w-2 h-2 rounded-full bg-[var(--color-accent-green)] shadow-[0_0_8px] shadow-[var(--color-accent-green)]" />
            {username}
          </div>
        ) : (
          <div className="flex items-center gap-2.5 px-4 py-1.5 rounded-full bg-white/[0.03] border border-[var(--color-border)] text-sm text-gray-600">
            <span className="w-2 h-2 rounded-full bg-gray-600" />
            Chưa login
          </div>
        )}
      </div>
    </header>
  )
}

export default function App() {
  const [loggedIn, setLoggedIn] = useState(false)
  const [username, setUsername] = useState('')

  useEffect(() => {
    apiGet<{ logged_in: boolean; username: string }>('/api/auth/status')
      .then(data => {
        if (data.logged_in) {
          setLoggedIn(true)
          setUsername(data.username)
        }
      })
      .catch(() => {})
  }, [])

  const handleLogin = (user: string) => {
    setLoggedIn(true)
    setUsername(user)
  }

  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col">
        <NavBar loggedIn={loggedIn} username={username} />

        <main className="flex-1 p-8 max-w-[1400px] mx-auto w-full">
          <Routes>
            {/* Route / — Chỉ đọc, không cần login */}
            <Route path="/" element={<BeastModeCleanup readOnly />} />

            {/* Route /admin — Đầy đủ, cần login để crawl */}
            <Route path="/admin" element={
              loggedIn ? (
                <BeastModeCleanup readOnly={false} />
              ) : (
                <Login onLoginSuccess={handleLogin} />
              )
            } />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
