import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'

export type Lang = 'vi' | 'ja'

const translations = {
  // Common
  'app.name': { vi: 'DOMO Toolkit', ja: 'DOMO Toolkit' },
  'app.subtitle': { vi: 'Quản lý dữ liệu', ja: 'データ管理' },
  'common.refresh': { vi: 'Làm mới', ja: '更新' },
  'common.loading': { vi: 'Đang tải...', ja: '読み込み中...' },
  'common.cancel': { vi: 'Hủy', ja: 'キャンセル' },
  'common.connected': { vi: 'Đã kết nối', ja: '接続済み' },
  'common.login': { vi: 'Đăng nhập', ja: 'ログイン' },
  'common.name': { vi: 'Tên', ja: '名前' },
  'common.status': { vi: 'Trạng thái', ja: 'ステータス' },
  'common.type': { vi: 'Loại', ja: '種類' },
  'common.details': { vi: 'Chi tiết', ja: '詳細' },
  'common.actions': { vi: 'Thao tác', ja: 'アクション' },
  'common.total': { vi: 'Tổng', ja: '合計' },
  'common.ok': { vi: 'OK', ja: 'OK' },
  'common.failed': { vi: 'Thất bại', ja: '失敗' },
  'common.stale': { vi: 'Quá hạn', ja: '期限切れ' },
  'common.running': { vi: 'Đang chạy...', ja: '実行中...' },
  'common.noUpdate': { vi: 'Chưa cập nhật', ja: '未更新' },
  'common.lastUpdated': { vi: 'Cập nhật lần cuối', ja: '最終更新' },

  // Sidebar
  'nav.main': { vi: 'Chính', ja: 'メイン' },
  'nav.system': { vi: 'Hệ thống', ja: 'システム' },
  'nav.dashboard': { vi: 'Tổng quan', ja: 'ダッシュボード' },
  'nav.beastmode': { vi: 'Beast Mode', ja: 'Beast Mode' },
  'nav.monitor': { vi: 'Giám sát', ja: 'モニター' },
  'nav.settings': { vi: 'Cài đặt', ja: '設定' },

  // Dashboard
  'dashboard.title': { vi: 'Tổng quan', ja: 'ダッシュボード' },
  'dashboard.desc': { vi: 'Tổng quan instance Domo', ja: 'Domoインスタンスの概要' },
  'dashboard.beastmodes': { vi: 'Beast Modes', ja: 'Beast Modes' },
  'dashboard.datasets': { vi: 'Datasets', ja: 'データセット' },
  'dashboard.dataflows': { vi: 'Dataflows', ja: 'データフロー' },
  'dashboard.bmGroups': { vi: 'Nhóm BM', ja: 'BMグループ' },
  'dashboard.quickActions': { vi: 'Thao tác nhanh', ja: 'クイックアクション' },
  'dashboard.bmCleanup': { vi: 'Dọn Beast Mode', ja: 'Beast Modeクリーンアップ' },
  'dashboard.bmCleanupDesc': { vi: 'Phân tích & xóa formula không dùng', ja: '未使用の数式を分析・削除' },
  'dashboard.monitor': { vi: 'Giám sát', ja: 'モニター' },
  'dashboard.monitorDesc': { vi: 'Kiểm tra trạng thái cập nhật', ja: '更新ステータスの確認' },
  'dashboard.session': { vi: 'Đăng nhập / Phiên', ja: 'ログイン / セッション' },
  'dashboard.sessionDesc': { vi: 'Quản lý kết nối Domo', ja: 'Domo接続の管理' },

  // Monitor
  'monitor.title': { vi: 'Giám sát', ja: 'モニター' },
  'monitor.desc': { vi: 'Kiểm tra trạng thái cập nhật dataset & dataflow', ja: 'データセットとデータフローの更新ステータス確認' },
  'monitor.crawl': { vi: 'Cào dữ liệu', ja: 'クロール' },
  'monitor.crawlDatasets': { vi: 'Cào Datasets', ja: 'データセットクロール' },
  'monitor.crawlDataflows': { vi: 'Cào Dataflows', ja: 'データフロークロール' },
  'monitor.crawlAll': { vi: 'Cào tất cả', ja: '全てクロール' },
  'monitor.healthCheck': { vi: 'Kiểm tra sức khỏe', ja: 'ヘルスチェック' },
  'monitor.runHealthCheck': { vi: 'Chạy Health Check', ja: 'ヘルスチェック実行' },
  'monitor.filters': { vi: 'Bộ lọc', ja: 'フィルター' },
  'monitor.staleThreshold': { vi: 'Ngưỡng quá hạn (giờ)', ja: '期限切れ閾値（時間）' },
  'monitor.minCardCount': { vi: 'Số card tối thiểu', ja: '最小カード数' },
  'monitor.providerType': { vi: 'Loại nguồn dữ liệu', ja: 'プロバイダー種類' },
  'monitor.crawling': { vi: 'Đang cào dữ liệu...', ja: 'クロール中...' },
  'monitor.datasetsCrawled': { vi: 'Datasets đã cào', ja: 'クロール済みデータセット' },
  'monitor.dataflowsCrawled': { vi: 'Dataflows đã cào', ja: 'クロール済みデータフロー' },
  'monitor.alerts': { vi: 'Cảnh báo', ja: 'アラート' },
  'monitor.allClear': { vi: 'Tất cả bình thường', ja: '問題なし' },
  'monitor.noAlerts': { vi: 'Không có cảnh báo với bộ lọc hiện tại', ja: '現在のフィルターでアラートはありません' },
  'monitor.lastCheck': { vi: 'Lần kiểm tra gần nhất', ja: '最終チェック' },
  'monitor.tab.overview': { vi: 'Tổng quan', ja: '概要' },
  'monitor.tab.datasets': { vi: 'Datasets', ja: 'データセット' },
  'monitor.tab.dataflows': { vi: 'Dataflows', ja: 'データフロー' },
  'monitor.rows': { vi: 'Dòng', ja: '行数' },
  'monitor.cards': { vi: 'Cards', ja: 'カード' },
  'monitor.owner': { vi: 'Chủ sở hữu', ja: 'オーナー' },
  'monitor.executions': { vi: 'Lần chạy', ja: '実行回数' },
  'monitor.lastExec': { vi: 'Lần chạy cuối', ja: '最終実行' },
  'monitor.paused': { vi: 'Tạm dừng', ja: '一時停止' },

  // Login
  'login.title': { vi: 'Đăng nhập', ja: 'ログイン' },
  'login.desc': { vi: 'Kết nối đến Domo instance', ja: 'Domoインスタンスに接続' },
  'login.userPass': { vi: 'Tài khoản / Mật khẩu', ja: 'ユーザー名 / パスワード' },
  'login.cookieImport': { vi: 'Cookie Import', ja: 'Cookie インポート' },
  'login.email': { vi: 'Email', ja: 'メールアドレス' },
  'login.password': { vi: 'Mật khẩu', ja: 'パスワード' },
  'login.useEnv': { vi: 'Để trống để dùng credentials từ .env', ja: '.envの認証情報を使用するには空のままに' },
  'login.loggingIn': { vi: 'Đang đăng nhập...', ja: 'ログイン中...' },
  'login.steps': { vi: 'Các bước', ja: '手順' },
  'login.step1': { vi: 'Mở Domo và đăng nhập', ja: 'Domoを開いてログイン' },
  'login.step2': { vi: 'Dùng J2 Team Cookie extension để export cookies', ja: 'J2 Team Cookie拡張機能でcookieをエクスポート' },
  'login.step3': { vi: 'Upload file .json bên dưới', ja: '下記に.jsonファイルをアップロード' },
  'login.openDomo': { vi: 'Mở Domo Login', ja: 'Domoログインを開く' },
  'login.uploadCookie': { vi: 'Upload Cookie JSON', ja: 'Cookie JSONアップロード' },
  'login.processing': { vi: 'Đang xử lý...', ja: '処理中...' },
  'login.connectingTo': { vi: 'Đang kết nối tới', ja: '接続先' },
} as const

export type TranslationKey = keyof typeof translations

interface I18nContextType {
  lang: Lang
  setLang: (lang: Lang) => void
  t: (key: TranslationKey) => string
}

const I18nContext = createContext<I18nContextType>({
  lang: 'vi',
  setLang: () => {},
  t: (key: string) => key,
})

/** Resolve initial language: URL param `?lang=ja` > localStorage > default 'vi' */
function getInitialLang(): Lang {
  // Check URL param
  const urlParams = new URLSearchParams(window.location.search)
  const urlLang = urlParams.get('lang')
  if (urlLang === 'ja' || urlLang === 'vi') return urlLang

  // Check localStorage
  const saved = localStorage.getItem('domo-lang')
  return (saved === 'ja' ? 'ja' : 'vi') as Lang
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [lang, setLang] = useState<Lang>(getInitialLang)

  // Sync URL param changes (e.g. user edits URL manually)
  useEffect(() => {
    const handlePopState = () => {
      const urlParams = new URLSearchParams(window.location.search)
      const urlLang = urlParams.get('lang')
      if (urlLang === 'ja' || urlLang === 'vi') {
        setLang(urlLang)
        localStorage.setItem('domo-lang', urlLang)
      }
    }
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  const changeLang = (newLang: Lang) => {
    setLang(newLang)
    localStorage.setItem('domo-lang', newLang)
    // Update URL param without page reload
    const url = new URL(window.location.href)
    url.searchParams.set('lang', newLang)
    window.history.replaceState({}, '', url.toString())
  }

  const t = (key: TranslationKey): string => {
    const entry = translations[key]
    return entry ? entry[lang] : key
  }

  return (
    <I18nContext.Provider value={{ lang, setLang: changeLang, t }}>
      {children}
    </I18nContext.Provider>
  )
}

export function useI18n() {
  return useContext(I18nContext)
}
