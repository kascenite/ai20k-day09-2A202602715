# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Lương Sỹ Khánh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602715 |
| GitHub | https://github.com/kascenite/ai20k-day09-2A202602715 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-02086 |

> Ghi chú: MSSV `2A202602715` có chữ nên `int(MSSV)` trong GUIDE không chạy được nên dùng `int(MSSV)` = `202602715`: `202602715 % 26 = 3`, ra `trip-02086`.

## Kết quả cuối

- `dataset_version` của bạn: `e274938b642c`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `726 / 135 / 158` (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt A và B:
1. Tên cột: A `ts_utc, speed_mps, heading_deg, lat, lon...`; B `timestamp_ms, spd_kmh, hdg, latitude, longitude...`.
2. Đơn vị tốc độ: A m/s, B km/h.
3. Timestamp: A chuỗi ISO UTC, B epoch mili giây.
4. Bàn đạp: A `gas`/`brake` thang 0..1, B `throttle_pct`/`brake_pct` theo %.
5. Đồng hồ: B lệch 1500 ms so với video, A lệch 0.

Mỗi nguồn một bảng vì schema khác nhau, gộp sớm là phải chọn đơn vị và xử lý lệch ngay lúc nạp, chọn sai thì mất dữ liệu gốc. Cột để dạng chữ (`VARCHAR`) để không dòng nào bị loại vì lỗi ép kiểu; ép kiểu dời sang silver, nơi dòng lỗi vào `rejects`.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

B được đổi về tên cột chung, m/s (chia 3,6), timestamp UTC (từ epoch ms) và **trừ 1500 ms** lệch đồng hồ. Bằng chứng: mẫu đầu của B là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00`.

| drive | duplicate | speed_missing | speed_out_of_range |
|---|---|---|---|
| drive_a | 20 | 30 | 3 |
| drive_b | 20 | 30 | 3 |

Số dòng cân: A 2968 + 53 = 3021, B 3181 + 53 = 3234. Dòng lỗi vào `rejects` kèm lý do để không mất dữ liệu im lặng, tra được nguyên nhân và khôi phục nếu luật lọc sai.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`in_range` là `error` vì tốc độ vô lý làm hỏng dữ liệu phía sau nên phải dừng pipeline. `max_gap_ms` là `warn` vì chỉ báo cảm biến mất tín hiệu (thiếu dữ liệu, không sai), không nên dừng cả pipeline.

`break_gate.json`: nới `max_speed_mps` từ 70 lên 1000, test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (3 mỗi drive), `PASS=5 WARN=1 ERROR=1`; khôi phục xong về `PASS=6 WARN=1 ERROR=0`. Cổng chất lượng chặn dữ liệu xấu ngay cả khi luật làm sạch bị nới nhầm.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Trip `trip-00003`: trước 1246 dòng, sau hai lần chạy lại vẫn 1246 dòng, dấu vân tay `6c7480dfde7e` không đổi. Kiểu `append`: trip có **120 dòng** thay vì 60 (nhân đôi). `overwrite` theo `trip_id` là một commit nguyên tử: chết giữa chừng thì chưa có commit, bảng vẫn như cũ; còn xoá rồi ghi là hai bước, chết giữa chừng thì trip mất sạch dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** (luma dưới 12) không còn thông tin nên bị loại; khung **tối** (luma 12 đến 40: hầm, bóng râm) vẫn có nội dung nên chỉ gắn cờ và giữ lại (A: 6 đen, 4 tối; B: 4 đen, 61 tối). Tôi đồng ý vì điều kiện thiếu sáng là có thật khi lái xe.

Tôi chọn ngưỡng **2**: ngưỡng 0 bỏ sót khung gần trùng do nhiễu nén, ngưỡng 6 loại hơn một nửa khung A (363/602) và bắt đầu loại cả khung khác nhau thật. Xe chạy chậm thì khung liền nhau giống nhau hơn nên trùng nhiều hơn: A (khoảng 38 km/h) có 149 khung trùng, B (khoảng 70 km/h) chỉ 53.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Chia theo khung hình: **26/26 trip** bị chia cắt và có khung ở cả train lẫn test. Chia theo trip: **0 trip** bị chia cắt. Các khung liền nhau gần như giống hệt, nên khung test có "bản sao" trong train, mô hình chỉ cần nhớ cảnh nên điểm cao giả tạo, gặp cảnh mới ngoài đường thì kém.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Version là sha256 (12 ký tự đầu) của danh sách `frame_id|split`, nên cùng nội dung thì cùng version và chỉ cần đổi một khung hoặc một split là version đổi; ngày giờ hay số đếm không cho tính chất này. Hai lần chạy liên tiếp đều ra `e274938b642c`. MSSV được trộn vào cách xếp trip nên mỗi người có split khác nhau: thử đặt MSSV tạm `TEST0001` thì version thành `9ac47dcef388` (train/val/test 722/159/138), đặt lại thì về `e274938b642c`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lineage` ghi: sha256 hai video (drive_a `636ba1a9...`, drive_b `bfa62703...`) và version Delta các bảng (`bronze.*` 0, `bronze.video_files` 1, `silver.telemetry` 0, `silver.frames_flagged` 0, `silver.frame_telemetry` 0, `silver.frames` 25). Để tái tạo cần: video đúng sha256, đúng version các bảng (time travel), mã và tham số trong `dbt_project.yml` cùng commit git, và MSSV dùng làm muối chia split.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với `LAB_FAIL_ONCE=trip-02086`: **1** task đỏ (`silver_frames` map_index 3, log `simulated crash while extracting trip-02086`), **7** task `upstream_failed` (dbt_run/test silver và gold, publish_lake, export_dataset, publish_warehouse), 29 task thành công. Tôi clear đúng task đỏ (kèm 7 task phía sau theo mặc định, 25 task `silver_frames` còn lại không chạy lại), DAG chuyển xanh. Dữ liệu không nhân đôi: `silver.frames` 1246 dòng với 1246 `frame_id` khác nhau, `trip-02086` đúng 60 dòng; `dataset_version` vẫn `e274938b642c`, `verify` 27/27. Ảnh: `airflow_crash_log.png`, `airflow_after_clear.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Giải mã video và Airflow một máy** (`PARALLELISM=2`, FFmpeg trong BashOperator): thay bằng Celery/Kubernetes executor tự co giãn, chia việc theo (ngày, xe, trip).
2. **MinIO một nút và nhiều file nhỏ** (hơn 2300 object cho 1246 khung): thay bằng S3 nhiều nút, partition Delta theo ngày/xe, nén file định kỳ, gom khung thành shard Parquet.
3. **dbt + DuckDB một file và Postgres warehouse**: thay bằng Spark/Trino hoặc dbt `incremental`, warehouse dạng cột cho dashboard.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select trip_id, round(100.0*(frames-frames_kept)/frames, 1) as dropped_pct
from marts.mart_trip_summary order by dropped_pct desc limit 12
```

Drive A bỏ 27,6% khung (602 vào, 436 giữ), drive B chỉ 9,5% (644 vào, 583 giữ). Trip bỏ nhiều nhất là `trip-04622` (56,7%), rồi `trip-03904` (50,0%) và `trip-02086` (46,7%); ở các trip này khung đen và mờ bằng 0, toàn bộ là khung gần trùng. `trip-04622` chạy trung bình 67 km/h mà vẫn có 34/60 khung trùng nên xe chậm không giải thích được, tôi nghi camera đứng hình nhưng chưa kiểm chứng.

## Nhìn lại

- Bất ngờ ở mất khung chủ yếu do khung gần trùng (149 ở drive A) chứ không do khung đen hay mờ; và chỉ đổi MSSV là `dataset_version` đổi hẳn dù dữ liệu y nguyên.
- Chưa rõ các khoảng mất tín hiệu ở `max_gap_ms` (luôn WARN) ảnh hưởng khung nào; module 3 chỉ bắt 11/12 khung đứng hình ở drive A.
- Lỗi chưa bắt: ngưỡng trùng cố định 2 bit không phân biệt xe dừng đèn đỏ hợp lệ với camera hỏng.
- Sự cố môi trường container (uid 50000) không ghi được vào `work/`, `report/`, `dbt/`; tôi xử lý bằng ACL, không sửa `docker-compose.yml`.
