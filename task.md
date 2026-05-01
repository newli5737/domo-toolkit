# Frontend Refactor — Multi-Dataflow + Dataset Detail + Card Filters

## ✅ Backend (DONE)
- [x] `dataFlow/215/config.json` — name, display_name, linked cards
- [x] `GET /api/pipeline/list` — list dataflows
- [x] `GET /api/pipeline/datasets` — input/output with display_name, cards
- [x] `GET /api/pipeline/datasets/detail` — columns, types, samples, null/distinct counts
- [x] `PUT /api/pipeline/datasets/rename` — update display name
- [x] `GET /api/pipeline/card/filters` — smart filter values (columns with <200 distinct)
- [x] `GET /api/pipeline/card/yoy` — card 186671670 with filter params
- [x] `GET /api/pipeline/card/revenue-by-year` — card 258978026 with filter params

---

## 🔧 Frontend (TODO)

### File structure — tách component
Hiện tại `PipelineManager.tsx` là 597 dòng monolith. Cần tách:

```
frontend/src/pages/
  PipelineManager.tsx       ← main page (header, tabs, dataflow selector)
  pipeline/
    DatasetDetail.tsx       ← output dataset click → detail panel  
    CardViewer.tsx          ← render card pivot table + smart filters
    PipelineSteps.tsx       ← move pipeline steps tab here
```

### Task 1: `PipelineManager.tsx` refactor
- [ ] Add dataflow selector dropdown (calls `/api/pipeline/list`)
- [ ] Pass `dataflowId` to all child components
- [ ] Remove standalone `Cards` tab
- [ ] Keep: Overview, Datasets, Data Explorer, Pipeline Steps (4 tabs)

### Task 2: `DatasetDetail.tsx` (NEW)
- [ ] Triggered when clicking output dataset in Datasets tab
- [ ] Shows:
  - Display name (editable input + save button → `PUT /rename`)
  - Column table: name | type | distinct | nulls | sample values
  - Row count, file size, last modified timestamp
  - **Linked Cards** section → clickable card items → open `CardViewer`
- [ ] Back button to return to dataset list

### Task 3: `CardViewer.tsx` (NEW)
- [ ] Receives `cardEndpoint` prop (e.g. `"yoy"` or `"revenue-by-year"`)
- [ ] Calls `/api/pipeline/card/filters` once → populates filter dropdowns
- [ ] **Smart filter bar**:
  - Dropdown per column (BLカテゴリ, ステータス名, ERAWANコード, プロジェクト名)
  - Active filter values shown as **tags** next to each dropdown
  - Filters passed as query params to card endpoint
- [ ] Renders pivot table based on `chart_type`:
  - `yoy` → 3-col table: month | 当年 | 前年 | 昨対比 (with conditional colors)
  - `revenue-by-year` → dynamic columns: month | 2025 | 2026 | ... (years from API)
- [ ] Totals row at bottom

### Task 4: `PipelineSteps.tsx` (EXTRACT)
- [ ] Move existing pipeline steps rendering from PipelineManager
- [ ] No logic changes, just extraction

### Task 5: Push & verify
- [ ] `git add . && git commit && git push`
- [ ] Verify Vercel build passes (no unused imports)
- [ ] Test on https://domo-toolkit.vercel.app/pipeline

---

## UI Flow

```
Pipeline Manager
├─ [Dropdown: Dataflow 215 ▼]     ← multi-dataflow selector
├─ Status bar (idle/running/success)
├─ Tabs: Overview | Datasets | Data Explorer | Pipeline Steps
│
├─ Datasets tab
│   ├─ Input Datasets (8 files, read-only list)
│   └─ Output Datasets
│       └─ Click "Backlog結合_運用課アカウント" →
│           ├─ DatasetDetail panel
│           │   ├─ Editable display name
│           │   ├─ Column table (55 cols)
│           │   ├─ Stats (128k rows, 10MB, last run)
│           │   └─ Linked Cards:
│           │       ├─ 売上昨対比 → Click → CardViewer
│           │       └─ 月別売上額 → Click → CardViewer
│           │
│           └─ CardViewer (inline, below detail)
│               ├─ Filter bar: [BLカテゴリ ▼] [ステータス ▼] [ERAWAN ▼]
│               │              Active: "課題リスト" ×  "完了" ×
│               └─ Pivot table with totals row
```
