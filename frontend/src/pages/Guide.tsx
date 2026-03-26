import { useI18n } from '../i18n'
import { BookOpen } from 'lucide-react'

export default function Guide() {
  const { lang } = useI18n()

  return (
    <div>
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <BookOpen className="w-6 h-6 text-blue-500" />
          {lang === 'vi' ? 'Hướng dẫn sử dụng' : '使い方ガイド'}
        </h1>
        <p>{lang === 'vi' ? 'Hướng dẫn sử dụng các chức năng' : '各機能の使い方ガイド'}</p>
      </div>

      <div className="page-body space-y-6 animate-fadein">

        {/* Intro */}
        <div className="card">
          <div className="card-header">{lang === 'vi' ? '📖 Công cụ này làm gì?' : '📖 このツールについて'}</div>
          <div className="card-body">
            <ul className="space-y-2 text-sm text-slate-600">
              <li className="flex items-start gap-2">
                <span className="text-blue-500 mt-0.5">•</span>
                {lang === 'vi'
                  ? 'Kiểm tra Dataset và Dataflow trên Domo có đang chạy bình thường không'
                  : 'DomoのDataSetとDataFlowが正常に動作しているかチェック'}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-500 mt-0.5">•</span>
                {lang === 'vi'
                  ? 'Nhận cảnh báo email khi có lỗi xảy ra'
                  : 'エラー発生時にメールで通知'}
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-500 mt-0.5">•</span>
                {lang === 'vi'
                  ? 'Dọn dẹp Beast Mode dư thừa, không cần dùng'
                  : '不要なBeast Modeを整理・削除'}
              </li>
            </ul>
          </div>
        </div>

        {/* Dashboard */}
        <div className="card">
          <div className="card-header">📊 Dashboard</div>
          <div className="card-body text-sm text-slate-600 space-y-2">
            <p>{lang === 'vi'
              ? 'Trang chủ hiển thị tổng quan: tổng số Beast Mode, Dataset, Dataflow. Bấm Quick Actions để chuyển nhanh đến các chức năng.'
              : 'ホーム画面で全体概要を表示：Beast Mode、DataSet、DataFlowの合計数。Quick Actionsボタンで各機能に素早くアクセス。'}</p>
          </div>
        </div>

        {/* Monitor */}
        <div className="card">
          <div className="card-header">🔍 {lang === 'vi' ? 'Monitor — Giám sát DataSet' : 'Monitor — DataSet監視'}</div>
          <div className="card-body text-sm text-slate-600 space-y-3">
            <p className="font-medium text-slate-700">{lang === 'vi' ? 'Kiểm tra Dataset/Dataflow nào đang bị lỗi hoặc ngưng hoạt động.' : 'DataSet/DataFlowのエラーや停止状態を確認。'}</p>
            <div className="p-3 rounded-lg bg-slate-50 border border-slate-100 space-y-2">
              <p><span className="font-semibold text-slate-700">1.</span> {lang === 'vi' ? 'Bấm Crawl → hệ thống quét toàn bộ từ Domo (chờ vài phút)' : 'Crawlボタン → Domoから全データをスキャン（数分お待ちください）'}</p>
              <p><span className="font-semibold text-slate-700">2.</span> {lang === 'vi' ? 'Chuyển tab Datasets hoặc Dataflows để xem kết quả' : 'DatasetsまたはDataflowsタブで結果を確認'}</p>
              <p><span className="font-semibold text-slate-700">3.</span> {lang === 'vi' ? 'Dùng bộ lọc để tìm nhanh (loại, trạng thái, số card...)' : 'フィルターで素早く検索（タイプ、状態、カード数など）'}</p>
              <p><span className="font-semibold text-slate-700">4.</span> {lang === 'vi' ? 'Bấm icon 🔗 → mở trực tiếp trên Domo để kiểm tra chi tiết' : '🔗アイコン → Domoで直接確認'}</p>
            </div>
          </div>
        </div>

        {/* Beast Mode */}
        <div className="card">
          <div className="card-header">🐉 {lang === 'vi' ? 'Beast Mode — Dọn dẹp' : 'Beast Mode — クリーンアップ'}</div>
          <div className="card-body text-sm text-slate-600 space-y-3">
            <p className="font-medium text-slate-700">{lang === 'vi' ? 'Tìm Beast Mode dư thừa để dọn dẹp, giảm tải hệ thống.' : '不要なBeast Modeを見つけてシステムを整理。'}</p>
            <div className="p-3 rounded-lg bg-slate-50 border border-slate-100 space-y-2">
              <p><span className="font-semibold text-slate-700">1.</span> {lang === 'vi' ? 'Bấm Crawl → quét toàn bộ Beast Mode' : 'Crawlボタン → 全Beast Modeをスキャン'}</p>
              <p><span className="font-semibold text-slate-700">2.</span> {lang === 'vi' ? 'Xem kết quả chia thành 4 nhóm:' : '結果は4グループに分類：'}</p>
            </div>

            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="p-2 text-left">{lang === 'vi' ? 'Nhóm' : 'グループ'}</th>
                  <th className="p-2 text-left">{lang === 'vi' ? 'Ý nghĩa' : '意味'}</th>
                </tr>
              </thead>
              <tbody>
                <tr className="border-b border-slate-100">
                  <td className="p-2"><span className="inline-block w-3 h-3 rounded-full bg-red-400 mr-1.5"></span>{lang === 'vi' ? 'Nhóm 1' : 'G1'}</td>
                  <td className="p-2">{lang === 'vi' ? 'Không được dùng → nên xóa' : '未使用 → 削除推奨'}</td>
                </tr>
                <tr className="border-b border-slate-100">
                  <td className="p-2"><span className="inline-block w-3 h-3 rounded-full bg-yellow-400 mr-1.5"></span>{lang === 'vi' ? 'Nhóm 2' : 'G2'}</td>
                  <td className="p-2">{lang === 'vi' ? 'Có người xem nhưng không dùng' : '閲覧あり、カード未使用'}</td>
                </tr>
                <tr className="border-b border-slate-100">
                  <td className="p-2"><span className="inline-block w-3 h-3 rounded-full bg-orange-400 mr-1.5"></span>{lang === 'vi' ? 'Nhóm 3' : 'G3'}</td>
                  <td className="p-2">{lang === 'vi' ? 'Được dùng ít' : '少数のカードで使用'}</td>
                </tr>
                <tr>
                  <td className="p-2"><span className="inline-block w-3 h-3 rounded-full bg-green-400 mr-1.5"></span>{lang === 'vi' ? 'Nhóm 4' : 'G4'}</td>
                  <td className="p-2">{lang === 'vi' ? 'Đang dùng bình thường' : '正常に使用中'}</td>
                </tr>
              </tbody>
            </table>

            <div className="p-3 rounded-lg bg-blue-50 border border-blue-100 text-xs text-blue-700">
              {lang === 'vi'
                ? '💡 Bấm Re-analyze để tính toán lại mà không cào dữ liệu mới — tiết kiệm thời gian.'
                : '💡 Re-analyzeボタンで新しいデータを取得せずに再計算 — 時間を節約。'}
            </div>
          </div>
        </div>

        {/* Alert */}
        <div className="card">
          <div className="card-header">🚨 {lang === 'vi' ? 'Alert — Cảnh báo' : 'Alert — アラート'}</div>
          <div className="card-body text-sm text-slate-600 space-y-2">
            <p>{lang === 'vi'
              ? 'Xem nhanh danh sách Dataset/Dataflow đang bị lỗi. Bấm icon 🔗 để mở trên Domo, bấm 🔄 để refresh.'
              : 'エラー状態のDataSet/DataFlowを一覧表示。🔗でDomoを開き、🔄で更新。'}</p>
          </div>
        </div>

        {/* Settings */}
        <div className="card">
          <div className="card-header">⚙️ {lang === 'vi' ? 'Settings — Cài đặt' : 'Settings — 設定'}</div>
          <div className="card-body text-sm text-slate-600 space-y-3">
            <div>
              <p className="font-semibold text-slate-700 mb-1">{lang === 'vi' ? 'Email cảnh báo' : 'アラートメール'}</p>
              <p>{lang === 'vi'
                ? 'Nhập email nhận cảnh báo (nhiều email cách nhau bằng dấu phẩy). Khi phát hiện lỗi → tự động gửi email.'
                : 'アラートメールアドレスを入力（複数はカンマ区切り）。エラー検出時に自動送信。'}</p>
            </div>
            <div>
              <p className="font-semibold text-slate-700 mb-1">{lang === 'vi' ? 'Lịch Auto-Check' : '自動チェックスケジュール'}</p>
              <div className="p-3 rounded-lg bg-slate-50 border border-slate-100 space-y-1.5">
                <p><span className="font-semibold">1.</span> {lang === 'vi' ? 'Bật toggle để kích hoạt' : 'トグルをONにして有効化'}</p>
                <p><span className="font-semibold">2.</span> {lang === 'vi' ? 'Chọn giờ, phút muốn chạy (mặc định 8:00)' : '実行時刻を選択（デフォルト8:00）'}</p>
                <p><span className="font-semibold">3.</span> {lang === 'vi' ? 'Chọn ngày trong tuần (mặc định T2→T6)' : '実行曜日を選択（デフォルト月〜金）'}</p>
                <p><span className="font-semibold">4.</span> {lang === 'vi' ? 'Bấm Lưu cấu hình' : '設定を保存ボタン'}</p>
              </div>
            </div>
            <div className="p-3 rounded-lg bg-green-50 border border-green-100 text-xs text-green-700">
              {lang === 'vi'
                ? '✅ Bấm "Chạy Auto-Check" để kiểm tra ngay lập tức mà không cần chờ lịch.'
                : '✅ 「Auto-Checkを実行」ボタンでスケジュールを待たずにすぐチェック。'}
            </div>
          </div>
        </div>

      </div>
    </div>
  )
}
