# Chiến lược Tối ưu hóa chi phí và Chuyển đổi hệ thống BI (Từ DOMO sang In-house)

## 1. Bối cảnh và Mục tiêu
Hệ thống hiện tại đang phụ thuộc hoàn toàn vào DOMO cho toàn bộ vòng đời dữ liệu (Connectors, ETL/Dataflows, Data Warehouse, và Visualization/Dashboards). Do mô hình tính phí của DOMO dựa trên Credit (số lần chạy dataflow, dung lượng lưu trữ, lượng user truy cập), chi phí vận hành hàng tháng là rất lớn.

**Mục tiêu cốt lõi:**
- Giảm thiểu số lần chạy Dataflow và giảm lượng lưu trữ trên DOMO.
- Từng bước chuyển đổi hệ thống BI sang các công nghệ mã nguồn mở / cloud-native có chi phí tối ưu hơn nhưng vẫn đảm bảo sức mạnh xử lý.
- **Mục tiêu cuối cùng (Phase 4):** Đưa chi phí vận hành toàn bộ hệ thống dữ liệu về mức cực kỳ tối ưu, ước tính chỉ khoảng **7.5 man (75,000 JPY) / tháng**.

---

## 2. Lộ trình triển khai (4 Phases Roadmap)

### Phase 1: Hybrid Processing & Custom Dashboard (Hiện tại)
*Mục tiêu: Giảm tải tính toán cho DOMO, tiết kiệm khoảng **10%** tổng số credit đang sử dụng.*

- **Data Sync (Export API):** Tự động tải dữ liệu gốc (raw data) từ DOMO về hệ thống local/server thông qua cơ chế đồng bộ (Sync DOMO endpoint).
- **Local ETL (DuckDB):** Sử dụng **DuckDB** kết hợp với SQL để xử lý, join, và transform hàng triệu dòng dữ liệu ngay trên hệ thống nội bộ thay vì chạy Dataflow trên DOMO.
- **Custom UI / Visualization:** Tự xây dựng Frontend (React) để trực quan hóa dữ liệu (Pivot tables, YoY Revenue, Cards) thay vì bắt user truy cập vào DOMO để xem dashboard, giảm số lượng license và credit query.
- **Trạng thái:** Đã hoàn thành và tích hợp thành công.

### Phase 2: AI-Powered Analytics & Visualization
*Mục tiêu: Mang lại trải nghiệm Self-service BI thông minh không cần phụ thuộc vào DOMO Analyzer.*

- **Module AI Card Builder:** 
  - Cho phép người dùng tự tạo biểu đồ (Pivot, Bar, Line, Pie...) thông qua giao diện trực quan (Visual Builder).
  - **Tích hợp AI (LLM):** Phân tích ngôn ngữ tự nhiên. Người dùng chỉ cần gõ "Hiển thị doanh thu theo tháng so sánh YoY", AI sẽ tự động sinh SQL chạy qua DuckDB và vẽ Card tương ứng.
- **Smart Insights:** Phân tích dữ liệu tự động, đưa ra các cảnh báo hoặc insight kinh doanh mà không cần setup phức tạp.

### Phase 3: Xây dựng In-house Data Stack (Tách rời DOMO Connectors & ETL)
*Mục tiêu: Xây dựng nền tảng hạ tầng dữ liệu độc lập, chuẩn bị cho việc rời bỏ hoàn toàn lưu trữ trên DOMO.*

- **Data Ingestion (Airbyte):** Thay thế DOMO Connectors bằng Airbyte để kéo dữ liệu tự động từ các nguồn (CRM, ERP, Ads Platforms, APIs...) về kho lưu trữ nội bộ.
- **Data Warehouse (ClickHouse):** Thay thế DOMO Storage bằng **ClickHouse** — CSDL phân tích (OLAP) cực kỳ mạnh mẽ, siêu tốc độ và chi phí lưu trữ cực rẻ.
- **Data Transformation (dbt):** Quản lý toàn bộ pipeline SQL, version control cho các dataflow thay cho Magic ETL của DOMO. Giúp data team dễ dàng maintain và scale.

### Phase 4: Hoàn tất Migration (Full Independence)
*Mục tiêu: Ngừng sử dụng hoàn toàn DOMO (hoặc giữ lại ở mức tối thiểu), đưa chi phí vận hành về **7.5 man/tháng**.*

- **1-to-1 Mapping:** Chuyển đổi và map toàn bộ các DOMO Datasets, Dataflows, Cards, và Dashboards còn lại sang hệ thống mới (Airbyte → dbt → ClickHouse → In-house Visualization / BI Tool nguồn mở như Superset/Metabase).
- **Rollout & Phân quyền:** Chuyển toàn bộ người dùng sang hệ thống mới với cơ chế phân quyền (Row-level security) tự quản lý.
- **Hiệu quả kinh tế:** Hệ thống công nghệ hiện đại, khả năng scale không giới hạn, nhưng chi phí hạ tầng (Server, ClickHouse cloud, Airbyte) được tối ưu triệt để.

---

## 3. Tóm tắt Lợi ích
1. **Kiểm soát chi phí:** Không còn nỗi lo "đốt credit" mỗi khi refresh dataflow hay thêm user.
2. **Làm chủ công nghệ:** Nắm giữ hoàn toàn data pipeline, dễ dàng debug và mở rộng.
3. **Trải nghiệm người dùng:** UI/UX được may đo (custom-built) phù hợp với quy trình nghiệp vụ cụ thể của công ty (kết hợp AI), thay vì dùng UI chung chung của nền tảng bên thứ ba.
