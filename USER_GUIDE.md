# 📖 Domo Toolkit — Hướng dẫn sử dụng

## Công cụ này làm gì?
Domo Toolkit giúp bạn:
- **Kiểm tra** Dataset và Dataflow trên Domo có đang chạy bình thường không
- **Nhận cảnh báo email** khi có lỗi xảy ra
- **Dọn dẹp Beast Mode** dư thừa, không cần dùng

---

## 📊 Trang Dashboard (`/`)

Đây là trang chủ, hiển thị tổng quan:
- Tổng số Beast Mode, Dataset, Dataflow đang có
- **Quick Actions**: bấm vào để chuyển nhanh đến các chức năng

---

## 🔍 Trang Monitor (`/monitor`)

**Mục đích**: Kiểm tra xem Dataset/Dataflow nào đang bị lỗi hoặc ngưng hoạt động.

### Cách dùng:
1. Bấm **Crawl** → hệ thống sẽ quét toàn bộ Dataset/Dataflow từ Domo (chờ vài phút)
2. Khi xong, chuyển tab **Datasets** hoặc **Dataflows** để xem kết quả
3. Dùng bộ lọc phía trên bảng để tìm nhanh (theo loại, trạng thái, số card...)
4. Bấm icon 🔗 bên phải mỗi dòng → **mở trực tiếp trên Domo** để kiểm tra chi tiết

---

## 🐉 Trang Beast Mode (`/beastmode`)

**Mục đích**: Tìm Beast Mode dư thừa để dọn dẹp, giảm tải hệ thống.

### Cách dùng:
1. Bấm **Crawl** → quét toàn bộ Beast Mode (chờ vài phút, xem tiến trình realtime)
2. Khi xong, xem bảng kết quả chia thành **4 nhóm**:

| Nhóm | Màu | Ý nghĩa |
|------|-----|---------|
| Nhóm 1 | 🔴 | Không được dùng → **nên xóa** |
| Nhóm 2 | 🟡 | Có người xem nhưng không dùng |
| Nhóm 3 | 🟠 | Được dùng ít |
| Nhóm 4 | 🟢 | Đang dùng bình thường |

3. Kéo xuống xem **Dataset cần dọn dẹp nhất** — bấm ID để mở trên Domo
4. Bấm **Re-analyze** nếu muốn tính toán lại mà không cào dữ liệu mới

---

## 🚨 Trang Alert (`/alert`)

**Mục đích**: Xem nhanh danh sách Dataset/Dataflow đang **bị lỗi**.

- Bấm icon 🔗 → mở trực tiếp trên Domo để kiểm tra
- Bấm 🔄 để refresh dữ liệu mới nhất

---

## ⚙️ Trang Settings (`/settings`)

**Mục đích**: Cài đặt email cảnh báo và lịch kiểm tra tự động.

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
