# Domo 定期MTG - Nội dung tài liệu (Loop)

---

## ① Tổng quan dự án (10 phút)  
**Speaker: Sakamoto**

### 🎯 Mục tiêu đến cuối tháng 06/2026

Khắc phục tình trạng vận hành Domo không ổn định phát sinh trong kỳ 39:
- Chậm xử lý sự cố
- Trễ deadline
- Tăng credit

→ Chuyển đổi sang nền tảng dữ liệu có thể vận hành ổn định toàn công ty, không phụ thuộc từng bộ phận.

---

### 🛠 Phương hướng

Chuyển từ phụ thuộc cá nhân → quản lý bằng:
- Quy trình
- Cấu trúc hệ thống

Thực hiện qua:
- Xây dựng hệ thống vận hành
- Thiết kế lại DataFlow
- Thiết lập quản lý credit

---

### ✅ KPI (Hiện tại: OK)

- 100% sự cố có báo cáo trong 72h (ngày làm việc)
- ≥50% sự cố được xử lý hoặc có plan trong 72h
- Tỷ lệ đúng deadline ≥80%
- Credit 2025 ≤ 11,000 (kế hoạch 12,000)
- Dự báo credit 2026 = 0

---

## ① Xây dựng hệ thống xử lý request & sự cố

- Chuẩn hóa flow xử lý:
  - Initial response
  - Phân tích nguyên nhân
  - Báo cáo phòng ngừa

- Quản lý task trên Backlog
- Thiết lập deadline + theo dõi định kỳ

→ Hiện vận hành tốt

---

## ② DataFlow: Hiển thị & tối ưu

- Đã triển khai ~30% (114 DataFlow)
- Tối ưu:
  - Logic phân loại
  - Phân công team + NDIS
  - Loại bỏ default xử lý

---

## ③ Quản lý chi phí (Credit)

- Hiện còn phụ thuộc cá nhân
- Cần:
  - Hiển thị usage
  - Rule kiểm soát
  - Training team

---

# 📊 Tình hình hiện tại

## Task

- Done: **409** (+14)
- Còn lại: **63** (-2)

- ~10 task liên quan ReCover (Sakamoto)

---

## 💰 Credit (Căng)

| Thời điểm | Credit |
|----------|--------|
| 15/3     | 1901.83 |
| 22/3     | 2498.83 |

### Dự kiến tháng 3

- 16–22: 597 (actual)
- 23–29: 597 (estimate)
- 30–31: 260 (estimate)

➡ Tổng:
- Plan: 3528  
- Actual: **3355**

---

### ❗ Credit cần mua
3355 - 235.37 = 120
→ Mua theo block 1000 → cần 4000 credit

👉 Mục tiêu: giảm bằng mọi cách

---

# 🚧 Các việc cần làm

## 1. Điều chỉnh tần suất MySQL

- Tối ưu theo hệ thống gốc
- File Excel: lịch update (bắt buộc bookmark)

### Phân công

- Yamaryo → PB / 現ポ + ETL → ✅ Done
- Nakamura → Account / Product / Order / Mothers  
  → ❌ Chưa làm → làm sáng mai

---

## 2. Tổng hợp data ngoài MySQL

- DOMO-381  
→ ✅ Done

---

## 3. Giảm tần suất DataFlow

→ ✅ Done

---

# ⚠️ Công việc cuối tháng / đầu tháng

- Update ERAWAN đầu tháng
- Re-fetch data + check shipment vs sales  
→ DOMO-425

- 2 ngày đầu/cuối tháng:
  → Ưu tiên xử lý sự cố

---

## Issue

### ❗ Chưa xử lý
- DOMO-396: lệch số liệu tháng 1

### ✅ Đã xử lý
- Issue 18,000 yen

---

# ② Kế hoạch 5 ngày tới (10–20 phút)

## Sakamoto
- Tuần trước: 5–10h
- Tuần tới: tương tự + xử lý project khác
- Issue: 없음

---

## Nakamura
- Tuần trước: 5h

### Ưu tiên:
- Task test (high)
- Data maintenance (high)

### Rule:
- Mỗi ngày check 1 issue + report

- Issue: 없음
- Support: 없음

### Task lớn
- DOMO-396 → deadline: 15/4  
→ Nguyên nhân: uncheck → mất data

---

## Yamaryo
- Tuần trước: ~2h
- Tuần tới: ≥6h (DataFlow)
- Issue: 없음

---

## Hanh
- Tuần trước: ~30h
- Tuần tới: ~25h
- Nghỉ: 10/4
- Issue: 없음

---

## Ngọc
- Tuần trước: ~20h
- Tuần tới: ≥30h
- Issue: 없음

---

# ③ Thảo luận chung

- Credit
- DataFlow
- Task lớn
- Blocker

→ Trao đổi toàn team