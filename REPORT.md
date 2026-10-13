# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đỗ Đình Long |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602673 |
| GitHub | long2110d |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt A (`raw_telemetry_a`, 3021 dòng) và B (`raw_telemetry_b`, 3234 dòng) thấy ở bronze:

1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`).
4. **Bàn đạp**: A là tỷ lệ 0–1 (`gas=0.028`), B là phần trăm (`throttle_pct=2.0`).
5. **Đồng hồ**: B chạy lệch so với video, `bronze_video_files` ghi `telemetry_clock_offset_ms = 1500` cho drive_b, 0 cho drive_a.

Mỗi nguồn một bảng vì hai schema khác nhau: gộp ngay thì phải quyết định cách đổi đơn vị/thời gian ở tầng ingest, và nếu đổi sai thì bản gốc đã mất. Để mọi cột là chữ thì ingest **không bao giờ thất bại** vì một ô xấu (ô trống, `"abc"`, số vô lý) và bronze giữ **đúng nguyên bản** để chạy lại silver khi sửa luật. Việc ép kiểu, đổi đơn vị, loại dòng lỗi dồn về silver, nơi có test. Nạp lại lần hai báo `already present` (nhận diện bằng sha256), và `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (`WRITE`, `rows_added: 3021`): bronze idempotent.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging đưa cả hai nguồn về **cùng cột, m/s, `timestamptz` UTC**:
- `stg_telemetry_a`: chỉ ép kiểu (`ts_utc` → `timestamptz`, tốc độ đã là m/s).
- `stg_telemetry_b`: đổi tên cột, `spd_kmh / 3.6` → m/s, `throttle_pct/100`, `brake_pct/100` → 0–1, và `to_timestamp((timestamp_ms − offset_ms)/1000)`, trong đó `offset_ms = 1500` lấy từ `bronze.video_files` (khai báo trong `sources.json`), tức là **trừ độ lệch đồng hồ** bằng một join, không viết cứng.

Bằng chứng: sau chuẩn hoá `bat_dau` của B là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (chưa trừ thì sẽ là `14:30:01.509`); verify module 2 báo "first sample within 0.5 s of video start". Tốc độ tối đa sau đổi đơn vị hợp lý: A 71 km/h, B 100 km/h. Kết quả: silver 6149 dòng sạch (A 2968, B 3181), 106 dòng cách ly.

| drive | duplicate | speed_missing | speed_out_of_range | sạch + cách ly = nhận |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 + 53 = 3234 |

Dòng lỗi vào `silver_telemetry_rejects` kèm `reject_reason` chứ không bị xoá để **kiểm toán được**: đếm được `sạch + cách ly = nhận về` (không mất dòng im lặng), biết nguồn nào/cảm biến nào hỏng nhiều, và nếu luật sai (ví dụ ngưỡng tốc độ quá chặt) thì sửa luật rồi chạy lại để lấy dòng đó về, không phải nạp lại từ nguồn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (ngưỡng 1000 ms, `severity: warn` trong `dbt/models/schema.yml`) đo **khoảng mất tín hiệu** giữa hai mẫu telemetry. Mất tín hiệu là hiện tượng thật của cảm biến (của tôi: mỗi drive có 1 khoảng > 1 s, dài nhất A 3073 ms, B 3124 ms); dữ liệu còn lại vẫn đúng, và khung hình rơi vào khoảng đó sẽ bị loại ở bước join (`sync_tol_ms = 150`). Nên chỉ **báo** để người vận hành biết, không dừng cả pipeline. `in_range` (tốc độ 0–70 m/s, mặc định `error`) kiểm tra một **bất biến**: tốc độ > 70 m/s (252 km/h) là vô lý về vật lý, nếu lọt vào silver thì nghĩa là luật làm sạch đã hỏng, dữ liệu sai sẽ đi vào dataset huấn luyện, nên phải **dừng**.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 → `dbt test` ra `PASS=5 WARN=1 ERROR=1`, test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng 3 + 3 dòng `speed_out_of_range` của hai drive lẽ ra phải bị cách ly). Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Ý nghĩa: luật làm sạch và test là **hai lớp độc lập**; khi lớp làm sạch bị nới (lỗi cấu hình, ai đó sửa nhầm), cổng chất lượng vẫn chặn được và trong Airflow các task sau `dbt_test` sẽ không chạy, dữ liệu xấu không đi tiếp xuống gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` (trip `trip-00003`, 60 khung): bảng `silver.frames` trước có **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau **hai lần chạy lại** partition đó vẫn **1246 dòng**, vân tay `6c7480dfde7e` (`unchanged: true`). Kiểu sai `append` chạy 2 lần cho trip này ra **120 dòng** cho 60 khung, tức **nhân đôi**. `cli history silver.frames` cho thấy hai lần chạy lại là hai commit mới (version 26, 27), mỗi commit `WRITE` với `predicate: trip_id = 'trip-00003'` và `rows_added: 60`: thay đúng partition đó, không đụng 25 trip còn lại.

Delta `overwrite` theo predicate là **một giao dịch nguyên tử**: file mới được ghi xong rồi mới có một commit vào `_delta_log` vừa đánh dấu xoá file cũ của trip vừa thêm file mới. Nếu lỗi giữa chừng (sập lúc đang decode video, mất kết nối MinIO), commit không xảy ra, người đọc vẫn thấy **phiên bản cũ đầy đủ**, chạy lại là xong. Còn "xoá rồi ghi lại" là hai thao tác: sập sau khi xoá thì trip đó **mất dữ liệu**, và người đọc giữa hai thao tác thấy bảng thiếu. Ngoài ra bản cũ vẫn xem được bằng time travel.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 / 602 | 8 / 644 |
| 2 | 149 / 602 | 53 / 644 |
| 6 | 363 / 602 | 244 / 644 |

**Khung đen** (độ sáng trung bình < `dark_luma = 12`) là khung **hỏng**: camera/encoder lỗi, không có thông tin gì, nên bị **loại** khỏi gold (A 6, B 4). **Khung tối** (độ sáng < `dark_soft = 40` nhưng ≥ 12) là **điều kiện thật**: đường hầm, bóng râm, chiều tối; ảnh vẫn nhìn thấy đường, nên chỉ **gắn cờ và giữ lại** (A 4, B 61). Tôi đồng ý: mô hình xe tự lái phải học được cả cảnh thiếu sáng, bỏ khung tối sẽ làm dataset lệch về trời sáng; cờ vẫn cho phép lọc hoặc cân bằng lại sau. Ngưỡng cứng 12 có nhược điểm là một khung đêm rất tối nhưng vẫn có đèn có thể bị coi là đen.

Ngưỡng trùng (`threshold.json`): 0 bit → A 61, B 8; 2 bit (đang dùng) → A 149, B 53; 6 bit → A 363, B 244 (hơn một nửa drive A). Tôi chọn **2**: đủ bắt camera đứng hình ở A (verify: 11/12 khung đứng hình bị bắt) và các khung xe gần như đứng yên, nhưng không loại khung thật sự khác nhau. Ở 6 bit, B (xe chạy nhanh, cảnh đổi liên tục) vẫn mất 244/644 khung, tức là đã loại cả khung có nội dung mới. Ngưỡng phụ thuộc tốc độ: A trung bình 38,1 km/h, 15,3% thời gian dưới 7 km/h nên hai khung cách nhau 0,5 s gần như giống nhau → nhiều trùng; B trung bình 70,6 km/h nên ít trùng. Xe càng chậm (hoặc lấy mẫu càng dày) thì khung liền kề càng giống, và một ngưỡng cố định sẽ loại nhiều hơn; hợp lý hơn là ngưỡng thấp kết hợp với tốc độ (ví dụ chỉ coi là trùng khi tốc độ gần 0 hoặc khi camera đứng hình).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: 1019 khung hợp lệ thuộc 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** bị chia cắt qua nhiều split, và **cả 26 trip** có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt; dataset của tôi có 18 trip train, 4 val, 4 test.

Lấy mẫu 2 khung/giây nên hai khung liền nhau cách 0,5 s gần như cùng một cảnh: cùng con đường, cùng xe phía trước, cùng ánh sáng, thời tiết. Chia theo khung thì với mỗi khung test gần như luôn có một "anh em sinh đôi" trong train, mô hình chỉ cần **nhớ** cảnh chứ không cần **tổng quát hoá**. Điểm test vì thế đo khả năng nhận lại cảnh đã thấy, cao giả tạo; khi gặp đường mới thật sự thì sụt mạnh. Chia theo trip bảo đảm test là những cảnh mô hình **chưa từng thấy**, giống tình huống triển khai thật.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của `sha256` trên danh sách `frame_id|split` đã sắp xếp (`pipeline/publish.py`, `export_dataset`). Nó **đặt tên theo nội dung**: cùng tập khung và cùng cách chia thì cùng tên, khác một khung hoặc một khung đổi split thì khác tên. Ngày giờ hay số đếm thì chạy lại cùng dữ liệu vẫn ra tên mới (không biết hai bản có giống nhau không), và hai bản khác nhau có thể trùng tên trên hai máy. Băm nội dung giúp so sánh, cache, và tái lập: một kết quả huấn luyện ghi `fb6509ec3b13` là chỉ đúng một tập dữ liệu.

Hai lần chạy liên tiếp: `step 4` rồi `cli export-dataset` đều ra **`fb6509ec3b13`**, cùng `manifest_sha256 = 0652d728…`, dù `created_at` khác nhau, và `step all --reset` từ lake trống cũng ra `fb6509ec3b13`.

MSSV được trộn vào thứ tự xếp trip: `gold_dataset_frames.sql` sắp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)` rồi cắt 70%/15%/15%. Đổi MSSV thì thứ tự đổi → trip nào vào train/val/test đổi → số khung mỗi split đổi → version đổi. Tôi thử đổi sang `2024000`: version thành **`8fc131884584`**, split 703/182/134 (của tôi `2A202602673`: 786/108/125); vẫn 18/4/4 trip vì tỷ lệ chia theo số trip. Đặt lại MSSV của tôi thì về đúng `fb6509ec3b13`. Tập khung hình không đổi (1019), chỉ cách chia đổi.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/fb6509ec3b13/manifest.json`, phần `lineage` (lần chạy `step 4` sau thí nghiệm idempotent):

```json
"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
"silver.frames": 27, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
"input_video_sha256": {"drive_a": "636ba1a9…5e7e9aa", "drive_b": "bfa62703…bbee9fdc"}
```

Trả lời "dataset này được tạo từ dữ liệu nào?": từ **hai video** có sha256 `636ba1a9…` (drive_a) và `bfa62703…` (drive_b); từ **phiên bản Delta** cụ thể của từng bảng trung gian: telemetry thô A/B version 0, `silver.frames` **version 27** (con số lớn vì 26 partition trip được ghi riêng, cộng 2 lần chạy lại `trip-00003` trong thí nghiệm idempotent), các bảng silver dbt version 0. Manifest còn ghi `rules` (bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; split "per trip, stratified by drive, md5 order"), `by_split` 786/108/125, `by_drive` A 436 / B 583, và `manifest_sha256`.

Ba tháng sau, để tái tạo đúng dataset: (1) `dataset_version` + `manifest.parquet` liệt kê chính xác từng `frame_id`, split, đường dẫn ảnh; (2) các phiên bản Delta trong `lineage` cho phép **time travel** đọc lại đúng trạng thái bảng lúc đó; (3) sha256 video để kiểm tra file gốc còn nguyên; (4) commit git của mã/`dbt_project.yml` (ngưỡng `dark_luma`, `dup_hamming`, `sync_tol_ms`…) và **MSSV** dùng làm salt cho split. Manifest chưa ghi commit git và các tham số ngưỡng, đó là điểm nên bổ sung.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
