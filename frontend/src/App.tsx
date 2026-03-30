import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { apiGet, apiPost } from './api'
import { I18nProvider } from './i18n'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import BeastModeCleanup from './pages/BeastModeCleanup'
import Monitor from './pages/Monitor'
import Alert from './pages/Alert'
import Settings from './pages/Settings'
import Guide from './pages/Guide'
import CardDashboard from './pages/CardDashboard'
import Login from './pages/Login'

export default function App() {
  const [loggedIn, setLoggedIn] = useState(false)
  const [username, setUsername] = useState('')
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    return localStorage.getItem('sidebar-collapsed') === 'true'
  })

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

  const handleLogout = async () => {
    try {
      await apiPost('/api/auth/logout')
    } catch { /* ignore */ }
    setLoggedIn(false)
    setUsername('')
  }

  const toggleSidebar = () => {
    setSidebarCollapsed(prev => {
      const next = !prev
      localStorage.setItem('sidebar-collapsed', String(next))
      return next
    })
  }

  return (
    <I18nProvider>
      <BrowserRouter>
        <div className="flex min-h-screen">
          <Sidebar
            loggedIn={loggedIn}
            username={username}
            collapsed={sidebarCollapsed}
            onToggle={toggleSidebar}
            onLogout={handleLogout}
          />

          <div className={`main-content flex-1 ${sidebarCollapsed ? 'sidebar-collapsed' : ''}`}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/beastmode" element={<BeastModeCleanup readOnly={!loggedIn} />} />
              <Route path="/monitor" element={
                loggedIn ? <Monitor /> : <Login onLoginSuccess={handleLogin} />
              } />
              <Route path="/settings" element={<Settings />} />
              <Route path="/guide" element={<Guide />} />
              <Route path="/cards" element={<CardDashboard />} />
              <Route path="/alert" element={<Alert />} />
              <Route path="/login" element={<Login onLoginSuccess={handleLogin} />} />
            </Routes>
          </div>
        </div>
      </BrowserRouter>
    </I18nProvider>
  )
}
