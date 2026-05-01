# AI Card Builder — Tự động tạo Card từ mô tả

Module cho phép user **mô tả card bằng ngôn ngữ tự nhiên** hoặc **chọn cấu hình trực quan** → AI sinh SQL query + chọn chart renderer phù hợp → hiển thị card real-time từ DuckDB data.

## Tổng quan kiến trúc

```
User Input (chart type, columns, logic)
    ↓
AI Engine (LLM / Rule-based)
    ↓
SQL Generator + Chart Config
    ↓
Backend (DuckDB Execute)
    ↓
Frontend (Dynamic Renderer)
    ↓
Card hiển thị
```

## Câu hỏi cần quyết định

### 1. Chọn AI engine
- **Rule-based (Phase 1)**: User chọn từ UI form → hệ thống ghép SQL từ template. Không cần API, miễn phí, deterministic.
- **LLM-based (Phase 2)**: User mô tả bằng ngôn ngữ tự nhiên → AI sinh SQL. Linh hoạt nhưng cần API key + chi phí.
- **Đề xuất**: Bắt đầu với Rule-based, sau đó thêm LLM.

### 2. Chart types hỗ trợ
- `pivot_table` — Bảng pivot (giống YoY, Revenue hiện tại)
- `bar_chart` — Biểu đồ cột
- `line_chart` — Biểu đồ đường (time series)
- `badge_number` — Số lớn hiển thị KPI
- `pie_chart` — Biểu đồ tròn

### 3. Dữ liệu & lưu trữ
- Dùng cho `pipeline_output` (DuckDB) hay cũng hỗ trợ query trực tiếp từ DOMO API?
- Card do user tạo lưu vào `config.json` hay riêng?

---

## Phase 1: Rule-based Card Builder (Core)

### Backend

#### [NEW] `backend/app/services/card_builder.py`
Service xử lý logic sinh SQL từ card config:

```python
class CardSpec:
    chart_type: str        # pivot_table, bar_chart, line_chart, badge_number, pie_chart
    row_column: str        # Cột dùng làm ROW (dimension)
    value_column: str      # Cột dùng làm VALUE (measure)
    aggregation: str       # SUM, COUNT, AVG, MIN, MAX
    pivot_column: str      # (optional) Cột dùng làm COLUMN header trong pivot
    filters: list          # WHERE conditions
    sort: str              # ASC/DESC
    limit: int             # Row limit
    title: str             # Tên card
```

Chức năng chính:
- `build_sql(spec: CardSpec, schema: list) -> str` — Sinh SQL query từ spec
- `validate_spec(spec: CardSpec, schema: list) -> list[str]` — Kiểm tra cột có tồn tại
- Schema-aware: đọc column types từ DuckDB để cast đúng kiểu

#### [MODIFY] `backend/app/routers/pipeline.py`
Thêm 3 endpoints:

```
GET  /api/pipeline/card-builder/schema    → Trả column list + types từ DuckDB
POST /api/pipeline/card-builder/preview   → Nhận CardSpec, trả data preview
POST /api/pipeline/card-builder/save      → Lưu card vào config.json
```

### Frontend

#### [NEW] `frontend/src/pages/pipeline/CardBuilder.tsx`
Trang tạo card mới, giao diện split-panel:

**Left Panel — Configuration Form:**
| Field | UI Element | Mô tả |
|---|---|---|
| Chart Type | Icon selector grid | Chọn loại chart (pivot, bar, line, badge, pie) |
| Row Column | Dropdown (từ schema) | Cột dimension (trục X / hàng) |
| Value Column | Dropdown (numeric only) | Cột measure (giá trị) |
| Aggregation | Button group | SUM / COUNT / AVG / MIN / MAX |
| Pivot Column | Dropdown (optional) | Cột pivot (header cột trong pivot table) |
| Filters | Dynamic filter builder | Thêm WHERE conditions |
| Title | Text input | Tên card |

**Right Panel — Live Preview:**
- Real-time preview khi user thay đổi config
- Debounce 500ms trước khi gọi API
- Hiển thị SQL query đã sinh (collapsible)

#### [NEW] `frontend/src/components/DynamicChart.tsx`
Universal chart renderer — nhận `chart_type` + `data` → render component phù hợp:

```tsx
function DynamicChart({ type, data, config }) {
  switch(type) {
    case 'pivot_table':   return <PivotTable data={data} config={config} />
    case 'bar_chart':     return <BarChart data={data} config={config} />
    case 'line_chart':    return <LineChart data={data} config={config} />
    case 'badge_number':  return <BadgeNumber data={data} config={config} />
    case 'pie_chart':     return <PieChart data={data} config={config} />
  }
}
```

Charts render bằng **SVG thuần** (không dependency ngoài).

#### [MODIFY] `frontend/src/App.tsx`
Thêm route: `/card-builder/:dataflowId`

#### [MODIFY] `frontend/src/pages/pipeline/DatasetDetail.tsx`
Thêm nút "＋ Create Card" ở tab Cards → navigate đến Card Builder

---

## Phase 2: LLM Natural Language (Extension)

#### [NEW] `backend/app/services/ai_card_generator.py`
- Nhận mô tả tiếng Việt/Nhật/Anh
- Gửi schema + prompt đến LLM API
- Parse response → `CardSpec`
- Fallback: nếu LLM fail → hiển thị form rule-based

**Prompt template:**
```
Given dataset with columns: {schema}
User request: "{user_input}"
Generate a CardSpec JSON with: chart_type, row_column, value_column, aggregation, filters, title
```

#### [MODIFY] `frontend/src/pages/pipeline/CardBuilder.tsx`
Thêm mode toggle: **Visual Builder** ↔ **AI Description**
- AI mode: textarea + "Generate" button
- AI response auto-fills form fields
- User có thể edit lại trước khi preview

---

## UI Flow

```
Dataset Detail Page
  → Click "＋ Create Card"
    → Card Builder Page
      → Chọn mode: Visual / AI
        → Visual: Form chọn chart type, columns, agg, filters
        → AI: Nhập mô tả tự nhiên → LLM parse → fill form
      → Live Preview (debounce 500ms) → POST /card-builder/preview
      → Click Save → POST /card-builder/save → config.json
      → Card xuất hiện trong Dataset Detail
```

---

## Verification Plan

### Test Cases
| # | Input | Expected Output |
|---|---|---|
| 1 | pivot_table + 請求月 + 税抜費用 + SUM + pivot=Year | Giống card YoY hiện tại |
| 2 | bar_chart + ERAWANコード + COUNT(*) | Top ERAWAN bar chart |
| 3 | badge_number + 税抜費用 + SUM | Hiển thị ¥xxx tổng |
| 4 | line_chart + 請求月 + 税抜費用 + SUM | Monthly revenue line |
| 5 | Cột không tồn tại | Validation error message |

### Commands
```bash
# Backend
cd backend && python -m pytest tests/test_card_builder.py -v

# Frontend build
cd frontend && npx tsc --noEmit
```
