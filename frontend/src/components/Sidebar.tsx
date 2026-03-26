import { Link, useLocation } from 'react-router-dom'
import {
  LayoutDashboard,
  Zap,
  Activity,
  Settings,
  LogIn,
  User,
  CircleDot,
  Languages,
  ChevronsLeft,
  ChevronsRight,
  AlertTriangle,
  BookOpen,
  BarChart3,
} from 'lucide-react'
import { useI18n } from '../i18n'

interface SidebarProps {
  loggedIn: boolean
  username: string
  collapsed: boolean
  onToggle: () => void
}

export default function Sidebar({ loggedIn, username, collapsed, onToggle }: SidebarProps) {
  const location = useLocation()
  const { t, lang, setLang } = useI18n()

  const isActive = (path: string) => {
    if (path === '/') return location.pathname === '/'
    return location.pathname.startsWith(path)
  }

  const navItems = [
    {
      section: t('nav.main'),
      items: [
        { path: '/', label: t('nav.dashboard'), icon: LayoutDashboard },
        { path: '/beastmode', label: lang === 'vi' ? 'Giám sát Beast Mode' : 'Beast Mode監視', icon: Zap },
        { path: '/cards', label: lang === 'vi' ? 'Card & Dashboard' : 'カード＆ダッシュボード', icon: BarChart3 },
        { path: '/monitor', label: lang === 'vi' ? 'Giám sát DataSet' : 'DataSet監視', icon: Activity },
        { path: '/alert', label: lang === 'vi' ? 'Cảnh báo' : 'アラート', icon: AlertTriangle },
      ]
    },
    {
      section: t('nav.system'),
      items: [
        { path: '/settings', label: t('nav.settings'), icon: Settings },
        { path: '/guide', label: lang === 'vi' ? 'Hướng dẫn' : 'ガイド', icon: BookOpen },
      ]
    }
  ]

  const toggleLang = () => {
    setLang(lang === 'vi' ? 'ja' : 'vi')
  }

  return (
    <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
      {/* Logo */}
      <div className="sidebar-logo">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center flex-shrink-0">
            <CircleDot className="w-5 h-5 text-white" />
          </div>
          {!collapsed && (
            <div className="sidebar-text-fade">
              <div className="text-[15px] font-bold text-white tracking-tight">{t('app.name')}</div>
              <div className="text-[11px] text-slate-400">{t('app.subtitle')}</div>
            </div>
          )}
        </div>
      </div>

      {/* Toggle button */}
      <button onClick={onToggle} className="sidebar-toggle" title={collapsed ? 'Expand' : 'Collapse'}>
        {collapsed ? <ChevronsRight className="w-4 h-4" /> : <ChevronsLeft className="w-4 h-4" />}
      </button>

      {/* Navigation */}
      <nav className="sidebar-nav">
        {navItems.map(section => (
          <div key={section.section}>
            {!collapsed && <div className="sidebar-section-label">{section.section}</div>}
            {section.items.map(item => {
              const Icon = item.icon
              return (
                <Link key={item.path} to={item.path}
                  className={`sidebar-link ${isActive(item.path) ? 'active' : ''}`}
                  title={collapsed ? item.label : undefined}>
                  <Icon className="icon" />
                  {!collapsed && <span className="sidebar-text-fade">{item.label}</span>}
                </Link>
              )
            })}
          </div>
        ))}
      </nav>

      {/* Language + Auth footer */}
      <div className="sidebar-footer">
        {/* Language switcher */}
        <button onClick={toggleLang}
          className="sidebar-link"
          style={{ border: 'none', background: 'transparent', cursor: 'pointer', width: '100%', marginBottom: '12px' }}
          title={collapsed ? (lang === 'vi' ? 'Tiếng Việt → 日本語' : '日本語 → Tiếng Việt') : undefined}>
          <Languages className="icon" />
          {!collapsed && (
            <>
              <span className="sidebar-text-fade">{lang === 'vi' ? 'Tiếng Việt' : '日本語'}</span>
              <span className="ml-auto text-xs px-1.5 py-0.5 rounded bg-slate-700 text-slate-300 sidebar-text-fade">
                {lang === 'vi' ? 'JA' : 'VI'}
              </span>
            </>
          )}
        </button>

        {loggedIn ? (
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-full bg-slate-600 flex items-center justify-center flex-shrink-0">
              <User className="w-4 h-4 text-slate-300" />
            </div>
            {!collapsed && (
              <div className="flex-1 min-w-0 sidebar-text-fade">
                <div className="text-[13px] font-medium text-slate-200 truncate">{username}</div>
                <div className="flex items-center gap-1.5 text-[11px] text-green-400">
                  <span className="w-1.5 h-1.5 rounded-full bg-green-400 pulse-dot" />
                  {t('common.connected')}
                </div>
              </div>
            )}
          </div>
        ) : (
          <Link to="/login" className="sidebar-link" title={collapsed ? t('common.login') : undefined}>
            <LogIn className="icon" />
            {!collapsed && <span className="sidebar-text-fade">{t('common.login')}</span>}
          </Link>
        )}
      </div>
    </aside>
  )
}
