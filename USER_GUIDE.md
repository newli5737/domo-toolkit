# 📖 Domo Toolkit — Hướng dẫn sử dụng

## Công cụ này làm gì?
Domo Toolkit giúp bạn:
- **Kiểm tra** Dataset và Dataflow trên Domo có đang chạy bình thường không
- **Nhận cảnh báo email** khi có lỗi xảy ra
- **Tự động kiểm tra theo lịch** (ví dụ: 8h sáng T2-T6)

---

## 🔍 Trang Monitor (`/monitor`)

**Mục đích**: Kiểm tra xem Dataset/Dataflow nào đang bị lỗi hoặc ngưng hoạt động.

### Cách dùng:
1. Bấm **Crawl** → hệ thống sẽ quét toàn bộ Dataset/Dataflow từ Domo (chờ vài phút)
2. Khi xong, chuyển tab **Datasets** hoặc **Dataflows** để xem kết quả
3. Dùng bộ lọc phía trên bảng để tìm nhanh (theo loại, trạng thái, số card...)
4. Bấm icon 🔗 bên phải mỗi dòng → **mở trực tiếp trên Domo** để kiểm tra chi tiết

### Ý nghĩa trạng thái:
| Trạng thái | Ý nghĩa |
|------------|---------|
| ✅ OK | Đang chạy bình thường |
| ⚠️ Stale | Không cập nhật quá lâu |
| ❌ Failed | Lần chạy gần nhất bị lỗi |

---

## 🚨 Trang Alert (`/alert`)

Xem nhanh danh sách Dataset/Dataflow đang **bị lỗi**. Bấm icon 🔗 → mở trên Domo, bấm 🔄 để refresh.

---

## ⚙️ Trang Settings (`/settings`)

### Email cảnh báo
- Nhập email nhận cảnh báo (nhiều email cách nhau bằng dấu phẩy)
- Khi hệ thống phát hiện lỗi → tự động gửi email đến các địa chỉ này

### Lịch Auto-Check
1. Bật **toggle** để kích hoạt kiểm tra tự động
2. Chọn **giờ** và **phút** muốn chạy (mặc định: 8:00 sáng)
3. Chọn **ngày trong tuần** (mặc định: Thứ 2 → Thứ 6)
4. Bấm **Lưu cấu hình**

→ Hệ thống sẽ tự kiểm tra vào đúng lịch đã đặt, nếu có lỗi sẽ gửi email ngay.

### Chạy thủ công
- Bấm **Chạy Auto-Check** → kiểm tra ngay lập tức mà không cần chờ lịch
