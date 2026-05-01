# Hướng dẫn Giảm Credit DOMO bằng DOMO Toolkit & DOMO App

Tài liệu này mô tả chi tiết phương pháp sử dụng **DOMO Toolkit** kết hợp với tính năng **DOMO App (iframe)** để giảm thiểu đáng kể chi phí (credit) tiêu thụ trên nền tảng DOMO, trong khi vẫn giữ nguyên trải nghiệm người dùng cuối.

---

## 1. Vấn đề hiện tại & Ý tưởng giải quyết

**Vấn đề:** 
DOMO tính phí (credit) dựa trên:
1. Số lần chạy Dataflow (xử lý dữ liệu).
2. Lượng dữ liệu lưu trữ.
3. Số lượng query khi người dùng xem/tương tác với Card trên Dashboard.
Với các Dataflow phức tạp chạy liên tục và nhiều người dùng truy cập Dashboard, lượng credit tiêu thụ hàng tháng là rất lớn.

**Ý tưởng giải quyết (Hybrid Approach):**
Thay vì để DOMO làm tất cả mọi việc (từ kéo data, xử lý ETL, lưu trữ output, đến render biểu đồ), chúng ta sẽ **"offload" (giảm tải)** phần xử lý nặng và phần render biểu đồ ra bên ngoài (DOMO Toolkit), sau đó nhúng ngược kết quả vào DOMO.

**Luồng hoạt động mới:**
1. **Sync Data:** DOMO Toolkit tải dữ liệu gốc (raw input datasets) từ DOMO về qua Export API (không tốn credit tính toán).
2. **Local Processing:** DOMO Toolkit sử dụng **DuckDB** (engine siêu tốc local) để chạy Dataflow (Join, Filter, Group By, v.v.). Việc này diễn ra trên server nội bộ, **chi phí = 0 credit**.
3. **Rendering:** DOMO Toolkit vẽ các Card (biểu đồ, bảng biểu) dựa trên dữ liệu đã xử lý.
4. **Embedding (Nhúng ngược):** Sử dụng tính năng **DOMO App** để tạo một thẻ iframe trên DOMO Dashboard. Thẻ này sẽ nhúng đường link (Embed Link) chứa biểu đồ từ DOMO Toolkit.
   
👉 **Kết quả:** Người dùng vẫn vào DOMO để xem Dashboard như bình thường. Tuy nhiên, đằng sau đó, DOMO không hề chạy Dataflow hay truy vấn Database cho các card đó. Trải nghiệm không đổi, nhưng tiết kiệm credit tối đa.

---

## 2. Bằng chứng khái niệm (Proof of Concept - PoC)

Phương pháp này đã được thử nghiệm và chứng minh thành công với quy trình sau:

### 2.1. Thử nghiệm Dataflow
- **Dataflow đích:** `Backlog結合 _運用課アカウント` (ID: 215)
- **Quá trình:** DOMO Toolkit đã tải 8 dataset đầu vào, sau đó chạy toàn bộ chuỗi xử lý bằng DuckDB (SQL).
- **Kết quả:** Output Dataset tạo ra từ DuckDB **khớp hoàn toàn 100%** về số dòng, số cột và giá trị so với Output Dataset chạy trên DOMO Magic ETL.

### 2.2. Thử nghiệm Visualization (Card)
Đã tái hiện thành công 2 Card quan trọng và phức tạp nhất (dạng Pivot Table) từ output của Dataflow trên:

1. **月別・ERAWANコード別売上額 (Doanh thu theo tháng và mã ERAWAN)**
   - Thể hiện Heatmap doanh thu chéo giữa các Tháng và Năm.
2. **売上昨対比 (So sánh doanh thu YoY)**
   - Thể hiện bảng so sánh doanh thu Năm nay vs Năm trước, kèm tỷ lệ tăng trưởng (tô màu highlight tự động).

Hai thẻ này được render với giao diện cao cấp (Premium UI), hỗ trợ song ngữ (Việt/Nhật), filter mượt mà và đặc biệt là có sẵn **Embed Link**.

---

## 3. Hướng dẫn các bước nhúng vào DOMO (Sử dụng DOMO App)

Để đưa các Card đã được tối ưu này lên Dashboard cho end-user, thực hiện các bước sau:

**Bước 1: Lấy Embed Link từ DOMO Toolkit**
1. Mở DOMO Toolkit, vào mục **Pipeline** -> Chọn Dataflow tương ứng (VD: 215).
2. Vào tab **Datasets**, click **Xem chi tiết** ở Output Dataset (VD: `pipeline_output.duckdb`).
3. Trong danh sách Card, chọn Card muốn nhúng (VD: `売上昨対比`).
4. Ở góc trên bên phải, click nút **[📋 Embed Link]**. Hệ thống sẽ copy một URL dạng:
   `https://[domain-cua-toolkit]/embed/card/215/yoy?lang=ja&title=売上昨対比`

**Bước 2: Tạo DOMO App (iframe) trên DOMO**
1. Đăng nhập vào DOMO, đi đến trang Dashboard mong muốn.
2. Thêm một thẻ mới (Add Card) và chọn loại **Custom App** hoặc **Webpage/Iframe Card** (tùy thuộc vào cấu hình instance DOMO của bạn).
3. Trong phần thiết lập nguồn (Source URL), dán **Embed Link** vừa copy ở Bước 1 vào.
4. Điều chỉnh kích thước khung nhìn (Width/Height) cho phù hợp với bố cục Dashboard.
5. Lưu lại.

**Trải nghiệm người dùng:**
Khi user vào Dashboard trên DOMO, iframe sẽ tự động load giao diện của DOMO Toolkit. Giao diện Embed đã được tối giản hoàn toàn (ẩn sidebar, menu), chỉ giữ lại tiêu đề, bộ lọc (filter) và bảng dữ liệu, khiến nó trông giống hệt một native card của DOMO.

---

## 4. Lợi ích và Kế hoạch mở rộng (Next Steps)

**Lợi ích trực tiếp:**
- **Tiết kiệm Dataflow Credit:** Tắt các schedule Dataflow nặng trên DOMO, chuyển sang chạy định kỳ trên Toolkit.
- **Tiết kiệm Query Credit:** Số lượng người dùng view/filter card không còn làm tăng load trên database của DOMO.

**Kế hoạch tiếp theo:**
Việc tái tạo bằng tay (code SQL & code React cho từng card) đã thành công ở Phase 1. Ở các phase tiếp theo, chúng tôi sẽ triển khai **AI Card Builder** để tự động hóa quá trình "dịch" cấu hình Card từ DOMO sang Toolkit, giúp mở rộng mô hình tiết kiệm này cho toàn bộ hàng trăm Dashboard hiện có của công ty một cách nhanh chóng.
