# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602715` |
| GitHub | `kascenite` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02086` |

## Kết quả cuối

- `dataset_version` của bạn: `e274938b642c`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `726 / 135 / 158` (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt giữa `raw_telemetry_a` (3021 dòng) và `raw_telemetry_b` (3234 dòng):
1. Tên cột: A là `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B là `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. Đơn vị tốc độ: A là m/s (dòng đầu 1.213), B là km/h (dòng đầu 55.11).
3. Kiểu timestamp: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. Thang bàn đạp: A `gas`/`brake` trong 0..1 (0.028), B `throttle_pct`/`brake_pct` theo % (2.0).
5. Đồng hồ thiết bị: `video_files` ghi `telemetry_clock_offset_ms` là 0 cho A và 1500 cho B, tức đồng hồ B lệch 1,5 giây so với video.

Mỗi nguồn một bảng vì schema khác nhau: gộp ngay sẽ buộc phải chọn tên cột, đơn vị và xử lý độ lệch đồng hồ ngay lúc nạp, và nếu chọn sai thì dữ liệu gốc đã mất. Mọi cột để dạng chữ (`typeof` ra `VARCHAR`, kể cả ô trống) để bronze giữ nguyên bản, không có dòng nào bị loại vì lỗi ép kiểu. Việc ép kiểu, đổi đơn vị và trừ độ lệch được dời sang silver, nơi dòng lỗi đi vào `rejects` kèm lý do. Nhờ vậy khi luật silver đổi, ta chạy lại từ bronze mà không cần nạp lại nguồn.

Bằng chứng chạy lại không đổi: lần nạp thứ hai báo `already present` (nhận diện theo sha256 của video, ví dụ A `636ba1a9...`), và `cli history bronze.raw_telemetry_a` chỉ có một commit (`version 0`, `WRITE`, `rows_added 3021`).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai staging model `stg_telemetry_a.sql` và `stg_telemetry_b.sql` đưa cả hai nguồn về cùng một schema: tên cột chung, tốc độ m/s (B chia km/h cho 3,6), timestamp kiểu UTC (B đổi từ epoch ms), bàn đạp theo thang 0..1. Với B, pipeline còn **trừ độ lệch đồng hồ 1500 ms** khai báo trong `sources.json`.

Bằng chứng đã đúng: thời điểm mẫu đầu của B sau chuẩn hoá là `2025-10-08 14:30:00.009+00:00`, khớp thời điểm bắt đầu video (`14:30:00`), chỉ lệch 9 ms (module 2 đòi lệch dưới 0,5 giây). Nếu không trừ thì mẫu đầu sẽ ở `14:30:01.5`. Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), dưới 130 km/h.

| drive | duplicate | speed_missing | speed_out_of_range |
|---|---|---|---|
| drive_a | 20 | 30 | 3 |
| drive_b | 20 | 30 | 3 |

Cân bằng số dòng: A là 2968 sạch + 53 cách ly = 3021 nhận về; B là 3181 + 53 = 3234. Tổng `silver.telemetry_rejects` là 106 dòng.

Dòng lỗi vào `rejects` kèm `reject_reason` thay vì bị xoá vì: (1) xoá im lặng làm số dòng không còn cân (nhận về = sạch + cách ly), không ai biết đã mất gì; (2) có lý do thì tra được nguyên nhân gốc (cảm biến, nguồn ghi trùng) và báo ngược cho nguồn; (3) nếu luật lọc sai thì dòng vẫn còn để khôi phục.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`in_range` (tốc độ 0..70 m/s) là `error` vì giá trị vô lý làm hỏng dữ liệu về sau (tính trung bình, huấn luyện), nên phải dừng pipeline, không cho dữ liệu xấu đi tiếp. `max_gap_ms` (khoảng cách giữa hai mẫu liên tiếp vượt 1000 ms) là `warn` vì nó báo **mất tín hiệu cảm biến**, tức dữ liệu thiếu chứ không sai. Đây là đặc điểm của nguồn, không do pipeline và không sửa được ở downstream. Dừng cả pipeline vì nó thì không đáng, nhưng vẫn cần ghi nhận (ở bước 2 dbt test ra `PASS=6 WARN=1 ERROR=0`).

Thí nghiệm `break-gate` nới `max_speed_mps` từ 70 lên 1000 để 6 dòng vô lý (3 dòng mỗi drive) lọt vào silver. Kết quả trong `break_gate.json`: `gate_failed: true`; test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**; summary khi nới là `PASS=5 WARN=1 ERROR=1`, sau khi khôi phục là `PASS=6 WARN=1 ERROR=0`. 6 dòng khớp đúng số `speed_out_of_range` ở bảng quarantine (3 + 3).

Điều này cho thấy cổng chất lượng là lớp bảo vệ độc lập với luật làm sạch: khi một luật bị nới nhầm (hoặc bị lỗi), test ở cổng vẫn phát hiện và chặn các task phía sau, nên dữ liệu xấu không lan sang gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm chạy trên `trip-00003` (60 khung). `idempotency.json`: bảng `silver.frames` có 1246 dòng, dấu vân tay `6c7480dfde7e`; sau **hai lần** chạy lại partition vẫn là 1246 dòng, dấu vân tay `6c7480dfde7e`, `unchanged: true`. Khi mô phỏng kiểu `append` sai và chạy hai lần, trip này có **120 dòng** thay vì 60, tức nhân đôi (2x); số dòng cả bảng sẽ phình tương ứng và mọi bảng phía sau (gold, mart) đếm trùng.

`overwrite` theo predicate `trip_id = ...` là một **commit nguyên tử** trong log Delta: dòng cũ của trip bị đánh dấu xoá và dòng mới được thêm trong cùng một commit, người đọc chỉ thấy bản trước hoặc bản sau, không thấy trạng thái ở giữa. Nếu tiến trình chết giữa chừng thì commit chưa được ghi, bảng vẫn nguyên như cũ, chạy lại là xong. Còn "xoá rồi ghi lại" là hai bước tách rời: chết sau bước xoá thì trip mất sạch dữ liệu và người đọc có thể thấy bảng thiếu. Predicate theo trip cũng giới hạn ảnh hưởng đúng một partition, 25 trip còn lại không bị đụng. Nhờ vậy backfill một trip hỏng chạy lại bao nhiêu lần cũng ra cùng một kết quả.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(tổng khung: drive A 602, drive B 644; số khác lấy từ `threshold.json`, ví dụ ngưỡng 1/4/8 là A 109/269/438 và B 23/137/348)

**Đen và tối.** Khung **đen** có độ sáng trung bình (luma) dưới 12: không còn thông tin hình ảnh (camera mất tín hiệu hoặc che kín), pipeline **loại**. Khung **tối** có luma từ 12 đến dưới 40: vẫn có nội dung (đường hầm, bóng râm, ban đêm) nhưng thiếu sáng, pipeline **gắn cờ nhưng giữ lại**. Kết quả của tôi: A có 6 đen và 4 tối, B có 4 đen và 61 tối. Tôi đồng ý vì khung tối là điều kiện lái xe có thật; loại hết thì mô hình không bao giờ học được cách chạy trong hầm hay buổi tối. Cờ vẫn cho phép lọc hoặc cân bằng lại sau nếu cần.

**Ngưỡng trùng.** Ngưỡng càng cao thì càng nhiều khung bị coi là trùng: A tăng từ 61 (0 bit) lên 149 (2 bit) rồi 363 (6 bit) trên 602 khung, tức ở 6 bit hơn một nửa khung bị loại. Tôi chọn giữ ngưỡng **2** (mức pipeline đang dùng). Ngưỡng 0 chỉ bắt khung giống hệt từng bit nên bỏ sót khung gần như trùng do nhiễu nén video; ngưỡng 6 trở lên bắt đầu loại cả những khung khác nhau thật (cảnh đổi nhẹ khi xe vẫn đang chạy).

**Phụ thuộc tốc độ xe.** Xe chậm thì hai khung liên tiếp (cách nhau 0,5 giây) gần giống nhau hơn nên trùng nhiều hơn: tốc độ trung bình A là khoảng 38 km/h, B khoảng 70 km/h, và ở ngưỡng 2 A có 149 khung trùng trong khi B chỉ có 53. A còn có camera đứng hình 6 giây, làm tăng thêm số khung trùng. Vì vậy một ngưỡng cố định không công bằng giữa các tình huống; ở bãi đỗ xe hoặc kẹt xe cần ngưỡng chặt hơn hoặc nên lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` (1019 khung đáng dùng, 26 trip): chia **theo khung hình** ngẫu nhiên 70/15/15 làm **26/26 trip** bị chia cắt, và cả 26 trip đều có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt.

Các khung liền nhau cách nhau 0,5 giây, cùng đoạn đường, cùng ánh sáng, cùng xe phía trước nên gần như giống hệt (ở bước 3 có 149 khung của drive A giống khung liền trước chỉ với dHash khác tối đa 2 bit). Khi một nửa trip nằm ở train và nửa còn lại ở test, mỗi khung test đều có "anh em sinh đôi" trong train; mô hình chỉ cần nhớ cảnh là đoán đúng, không cần học khái quát. Điểm kiểm thử vì thế cao giả tạo (đo khả năng ghi nhớ), nhưng khi gặp cảnh hoàn toàn mới trên đường thật thì kém hơn nhiều. Chia theo trip buộc test là những cảnh mô hình chưa từng thấy, nên điểm phản ánh đúng hơn. Pipeline còn có test `no_straddle` trong dbt để chặn lỗi này, và module 4 kiểm tra `no trip is in two splits` (đạt, 26 trip).

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Trong `export_dataset`, version là 12 ký tự đầu của sha256 trên danh sách `frame_id|split` của mọi khung. Nó là **băm nội dung**, nên: cùng nội dung (cùng khung, cùng split) thì ra cùng version; chỉ cần một khung đổi split hoặc thêm bớt một khung thì version khác. Ngày giờ hoặc số đếm thì không có tính này: chạy lại cùng dữ liệu vẫn ra version mới (không biết hai bản có giống nhau không), còn hai bản khác nhau có thể trùng số. Băm nội dung cho phép đối chiếu dataset và phát hiện thay đổi âm thầm.

Hai lần chạy liên tiếp của tôi cho cùng `e274938b642c` (`step 4`, rồi `cli export-dataset`, và cả `step all --reset` trước đó), dù `created_at` trong manifest khác nhau vì giờ không nằm trong băm.

Khác với bạn bè vì MSSV được trộn vào thứ tự md5 khi xếp trip vào train/val/test, nên mỗi người có tập trip ở mỗi split khác nhau, dù dữ liệu gốc giống nhau. Tôi thử đặt MSSV tạm là `TEST0001` rồi chạy lại `step 4`: version đổi từ `e274938b642c` sang `9ac47dcef388`, và số khung đổi từ train/val/test = 726/135/158 sang 722/159/138 (tổng vẫn 1019, vẫn 18/4/4 trip). Sau khi đặt lại MSSV `2A202602715` thì version trở về `e274938b642c`. Điều này cho thấy giảng viên chỉ cần so version để biết bài chạy bằng đúng MSSV hay không.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lineage` trong `gold/datasets/e274938b642c/manifest.json` (lần chạy cuối) cho biết dataset này được tạo từ:
- Hai video gốc, nhận diện bằng sha256: drive_a `636ba1a98dfc...e9aa`, drive_b `bfa62703f645...9fdc`.
- Phiên bản Delta của từng bảng đã dùng: `bronze.raw_telemetry_a` 0, `bronze.raw_telemetry_b` 0, `bronze.trips_raw` 0, `bronze.video_files` 1, `silver.telemetry` 0, `silver.frames_flagged` 0, `silver.frame_telemetry` 0, và `silver.frames` **25** (26 trip, mỗi trip một commit, nên version đến 25).

Manifest còn ghi `rules` (bỏ khung đen, mờ, trùng, mất đồng bộ; chia theo trip, phân tầng theo drive, thứ tự md5), `frames` 1019, `manifest_sha256` `a83f223a...0cfe561` và `created_at`.

Để tái tạo sau 3 tháng cần: (1) hai video đúng sha256 ở trên; (2) đúng phiên bản của các bảng Delta đó (time travel đọc lại được bảng ở version cũ); (3) mã pipeline và các tham số trong `dbt_project.yml` (`dup_hamming: 2`, `dark_luma: 12`, `blur_var: 40`, `sync_tol_ms: 150`, `split_train: 0.70`, `split_val: 0.15`) cùng commit git của repo; (4) **MSSV `2A202602715`**, vì nó là muối của cách chia split. Chạy lại đủ cả bốn thứ thì ra đúng `dataset_version` `e274938b642c`; chỉ cần sai một thứ thì version đổi.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của tôi là `trip-02086` (chỉ số 3 trong 26 trip; xem lưu ý về cách tính ở mục Nhìn lại). Tôi đặt `LAB_FAIL_ONCE=trip-02086`, trigger `day09_reset` rồi `day09_video_pipeline` (run `manual__2026-10-12T12:36:54+00:00`).

- Task đỏ: **1** (`silver_frames` map_index 3). Log ghi `RuntimeError: simulated crash while extracting trip-02086; run it again`.
- `upstream_failed`: **7** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`); 29 task còn lại thành công (tổng 37 task instance).
- Tôi clear đúng task đỏ đó (mở task `silver_frames`, map index 3, bấm Clear; hộp thoại ghi 8 task bị ảnh hưởng: task đỏ cộng 7 task phía sau). 25 task `silver_frames` còn lại **không chạy lại** (đều `try_number` 1). Task đỏ chạy lại thành công ở lần thử 2, 7 task phía sau chạy tiếp, cuối cùng cả 37 task `success` và DAG xanh.
- Dữ liệu không nhân đôi: `silver.frames` có 1246 dòng và 1246 `frame_id` khác nhau; `trip-02086` có đúng 60 dòng. Lịch sử Delta của trip này chỉ có **một** commit `WRITE` (60 dòng, `predicate trip_id = 'trip-02086'`), vì lần sập xảy ra trước khi ghi nên không có gì bị ghi dở.
- `dataset_version` **không đổi**: vẫn `e274938b642c`, `verify` vẫn 27/27.
- Ảnh: `report/screenshots/airflow_crash_log.png` (log task đỏ, thấy `trip-02086`) và `airflow_after_clear.png` (Graph xanh sau khi clear).

Nhờ mỗi trip là một partition ghi bằng overwrite theo predicate, và dbt/publish chỉ chạy sau khi mọi trip xong, một trip hỏng được chạy lại riêng mà không đụng phần còn lại.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Số đo hiện tại: 2 drive (khoảng 10 phút video, 26 trip) chạy hết DAG trong khoảng 1 phút 40 giây, trên một container Airflow có `PARALLELISM=2`. Nếu mỗi xe một drive cỡ 5 phút thì 500 xe là khoảng 250 lần khối lượng hiện tại; ba chỗ vỡ trước:

1. **Giải mã video và điều phối.** `silver_frames` giải mã bằng FFmpeg trong BashOperator, mỗi lần chỉ 2 task song song, một executor trên một máy. Nút cổ chai là giải mã video (slide 24). Thay bằng Celery hoặc Kubernetes executor với worker tự co giãn (có thể giải mã bằng GPU), và chia việc theo (ngày, xe, trip). Số task mapped từ 26 lên hàng chục nghìn mỗi ngày, nên cần phân vùng DAG theo ngày hoặc dùng sự kiện nạp (hàng đợi) thay vì một DAG lớn.
2. **Kho lưu trữ và bảng Delta nhỏ lẻ.** Mỗi trip một commit và mỗi khung hình một file JPEG: 1246 khung đã là hơn 2300 object trong MinIO một nút, đến hàng chục triệu thì list và ghi file nhỏ sẽ chậm. Thay bằng object storage nhiều nút (S3), partition Delta theo ngày/xe, nén file định kỳ (`OPTIMIZE`/`VACUUM`), và gom khung hình thành shard (Parquet/WebDataset) thay vì file lẻ.
3. **Biến đổi và warehouse một tiến trình.** dbt + DuckDB chạy trên một file `lakehouse.duckdb` (một người ghi) và dbt dựng lại toàn bộ bảng mỗi lần, cộng với một Postgres làm warehouse. Thay bằng engine phân tán (Spark/Trino) hoặc DuckDB theo từng phân vùng, mô hình dbt `incremental`, và warehouse dạng cột (BigQuery, ClickHouse...) cho dashboard (slide 22).

Ngoài ra `dataset_version` hiện băm toàn bộ khung hình mỗi lần xuất, cũng sẽ cần tính tăng dần theo phân vùng.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select trip_id, round(100.0*(frames-frames_kept)/frames, 1) as dropped_pct
from marts.mart_trip_summary order by dropped_pct desc limit 12
```

Câu thứ hai gộp theo drive: `select drive_id, sum(frames), sum(frames_kept), round(100.0*(sum(frames)-sum(frames_kept))/sum(frames),1) from marts.mart_trip_summary group by drive_id`.

Kết quả: drive A bỏ **27,6%** khung (602 vào, 436 giữ lại), drive B chỉ **9,5%** (644 vào, 583 giữ lại). Trip bỏ nhiều nhất là `trip-04622` (56,7%: 34 trong 60 khung bị bỏ), rồi `trip-03904` (50,0%) và `trip-02086` (46,7%), đều của drive A. Tôi tra thêm cờ trong `silver_frames_flagged`: ở các trip này khung đen và mờ đều bằng 0, toàn bộ khung bị bỏ là **khung gần trùng** (28/60 ở `trip-02086`, 34/60 ở `trip-04622`). Với `trip-02086` (tốc độ trung bình 17,6 km/h) và `trip-08888` (6,5 km/h) thì xe chạy chậm giải thích được. Riêng `trip-04622` chạy trung bình 67,1 km/h mà vẫn có 34 khung trùng, nên xe chậm không giải thích được; tôi nghi do camera đứng hình (drive A có đoạn đứng hình 6 giây), nhưng chưa kiểm chứng thêm.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Bất ngờ nhất: nguyên nhân chính khiến mất khung không phải khung đen hay mờ mà là khung gần trùng (149 khung ở drive A, trong khi chỉ có 6 đen và 9 mờ), và một thí nghiệm nhỏ (đổi MSSV) đổi hẳn `dataset_version` dù dữ liệu y nguyên.
- Gặp lỗi môi trường: container chạy bằng uid 50000 không ghi được vào `work/`, `report/`, `dbt/` (thuộc uid 1000), nên `dbt` báo `Permission denied`; tôi xử lý bằng ACL cho uid 50000, không sửa `docker-compose.yml`.
- Chưa rõ: `max_gap_ms` luôn WARN, nhưng tôi chưa xem cụ thể những khoảng mất tín hiệu nào và chúng ảnh hưởng khung nào; module 3 cũng chỉ bắt được 11/12 khung đứng hình ở drive A, tức vẫn sót 1 khung mà pipeline chưa bắt.
- Lỗi mà tôi nghĩ pipeline chưa bắt: khung gần trùng bị loại bằng ngưỡng cố định 2 bit, nên cảnh xe dừng đèn đỏ hợp lệ và camera hỏng đều bị coi giống nhau; pipeline không phân biệt hai trường hợp này.
- Giả định: MSSV `2A202602715` có chữ nên công thức `int(MSSV) % 26` trong GUIDE không chạy được; tôi lấy phần chữ số `202602715` rồi chia 26 dư 3, ra `trip-02086`. Nếu giảng viên tính cách khác thì trip sẽ khác.
