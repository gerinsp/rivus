# Integration crash/restart terisolasi

Fixture ini memakai **Colima**, dua MySQL 8.4.11 containers terpisah (source dan metadata), MySQL job registry/lease nyata, serta Hadoop Iceberg catalog nyata dengan Parquet/manifest/metadata files di temporary warehouse. Source memakai ROW/FULL binlog; metadata memakai durable InnoDB commit settings. Port hanya loopback `49316` dan `49317`. Test tidak membaca repository `.env` atau mengakses production. Docker context aktif tidak perlu diubah.

Jalankan dari repository root:

```sh
colima start --activate=false
docker --context colima compose -f tests/recovery/compose.yml up -d --wait
env RIVUS_RECOVERY_INTEGRATION=1 go test -p=1 -tags=integration ./pkg/connectors/iceberg ./pkg/meta -run '^(TestAutomaticRecoveryReal|TestWorkerFenceReal|TestRecoveryReal)' -count=1 -v -timeout=5m
```

Secara default metadata-container crash scenario di-skip. Untuk turut menguji SIGKILL dan restart **mysqld fixture**, beri explicit opt-in:

```sh
env RIVUS_RECOVERY_INTEGRATION=1 RIVUS_RECOVERY_CONTAINER_FAULTS=1 go test -p=1 -tags=integration ./pkg/connectors/iceberg ./pkg/meta -run '^(TestAutomaticRecoveryReal|TestWorkerFenceReal|TestRecoveryReal)' -count=1 -v -timeout=5m
docker --context colima compose -f tests/recovery/compose.yml down --volumes
```

Container fault test menolak target selain Colima container `rivus-recovery-test-metadata-1` dengan label project/service dan port binding yang persis sesuai fixture. Exit code 137 wajib terbukti sebelum restart. Jangan menjalankan suite ini paralel dengan test lain yang memakai fixture yang sama; `-p=1` menjaga package metadata tidak diuji ketika metadata server sedang di-kill. Teardown hanya menghapus services/volumes project fixture tersebut.

## Skenario dan oracle

Harness Fix 4 menjalankan **JobManager, worker claim/renewal, MySQL CDC, ownership fence, dan sink pipeline nyata** di subprocess. Factory test memakai Hadoop catalog sebagai pengganti constructor REST/S3; fault decorators hanya tersedia dengan build tag `integration`. SIGKILL dikirim parent pada barrier teramati:

- RECOVERING setelah lost response + gagal membaca proof, intent masih PREPARED.
- Setelah positive proof/COMMITTED tetapi sebelum batch/checkpoint selesai.
- Setelah durable checkpoint write tetapi sebelum receipt cleanup.
- SIGSTOP menghentikan worker dan lease renewal; worker baru mengambil expired lease; SIGCONT membangunkan stale process yang checkpoint/mark-nya ditolak.
- SIGKILL worker + SIGKILL mysqld metadata; restart membuktikan durable operation ID, intent, checkpoint, desired RUNNING, dan submission tetap sama.

Worker baru harus memakai normal `ClaimJobs` setelah lease expiry, **tanpa manual resubmit atau penggantian submission**. Oracle membaca fresh catalog untuk memeriksa IDs/payload dan duplicate rows, snapshot count (1 untuk crashed batch, 2 setelah source row baru), durable intents, checkpoint coverage, dan registry. MySQL dapat menyimpan control-event checkpoint BEGIN sebelum data row; oracle memastikan checkpoint tetap beku selama recovery dan tidak melewati intent data yang belum selesai, bukan menyamakan semua checkpoint dengan posisi awal fixture.

SQL trigger keyed ke fixture masing-masing menyebabkan COMMITTED write failure; penghapusan trigger memungkinkan bounded automatic recovery tanpa recommit. Missing proof tetap FAILED/BLOCKED setelah 5 attempts, dengan PREPARED dan checkpoint dipertahankan serta cleanup ditolak.

Metadata tests memakai **dua MySQL sessions nyata**: advisory-lock barrier yang terlihat pada `performance_schema.metadata_locks` menahan offset mutation, competing registry update wajib mendapat lock timeout 1205, lalu stale token ditolak setelah takeover. Slow trigger membuat lease expired saat SQL mutation; second ownership check wajib rollback offset 42 ke 4.

Claim-poll regression menahan registry row milik running job sambil worker lain melakukan claim dan renewal untuk queued job. Dua cases memastikan legacy submission yang kosong diberi token durable dan existing token tidak diganti. Global backfill sebelum claim awalnya menyebabkan timeout pada unrelated locked row; follow-up patch membatasi backfill pada claim candidate. Detail: [worker ownership recovery](../../docs/worker-ownership-recovery.md).

Harness sebelumnya tetap diuji: SIGKILL sebelum request, sesudah catalog commit, sesudah COMMITTED, dan sesudah checkpoint; SQL cleanup failure wajib rollback ketiga checkpoint rows. Hanya SIGKILL exit yang dihitung sebagai intended crash; ordinary test errors, startup failures, atau timeout menggagalkan test.

## Batas bukti

Fresh-snapshot admission regression memakai source dan metadata MySQL nyata. Saved file `mysql-bin.000000` mendahului available fixture history; actual source diagnostics mengembalikan `purged` tanpa menghapus fixture binlog. Empat cases (ALL/master, PREPARED/COMMITTED) mempertahankan role/status, submission, lease, checkpoint dan receipts setelah resubmit ditolak. Public ID menemukan internal-key intent. Detail: [Iceberg checkpoint recovery](../../docs/iceberg-checkpoint-recovery.md).

Jalankan **34 baseline scenarios**, termasuk regression baru dan existing crash faults, setelah fixture setup:

```sh
env RIVUS_RECOVERY_INTEGRATION=1 RIVUS_RECOVERY_CONTAINER_FAULTS=1 go test -p=1 -tags=integration ./pkg/api ./pkg/core ./pkg/connectors/iceberg ./pkg/meta -run '^(TestDeleteRecoveryReal|TestFreshSnapshotReal|TestPurgedBinlog|TestJobRebuildReal|TestAutomaticRecoveryReal|TestWorkerFenceReal|TestRecoveryReal)' -count=1 -v -timeout=5m
```

Admission cases tidak menguji actual source purge, binlog restoration atau target rebuild. Original 16 crash/fence/claim scenarios tetap lulus tanpa skips pada verification terbaru.

Follow-up menambah 14 scenarios: actual SIGKILL setelah checkpoint transaction berada di tengah batch yang committed; master resubmit setelah purge untuk configured full reset; 10 metadata archive/reset/fence cases; blocked Delete HTTP 409; dan actual master/snapshot/streaming rebuild pipeline. Restart membaca suffix dari MySQL binlog nyata, mempertahankan satu Iceberg snapshot untuk batch awal, lalu melanjutkan CDC tanpa duplicate. Rebuild archive mencakup config/checkpoint/snapshot/progress/receipts dan digabung atomik dengan reset + fresh submission + queue. Trigger archive/delete failure membuktikan rollback. PREPARED dan worker yang belum release ownership tetap ditolak. Pipeline test menghapus old rows dari populated target dan empty-source target, mengisi ulang dari source, memverifikasi snapshot completion/handoff, lalu menerima row CDC baru dengan worker streaming.

Version 2 manifest menyimpan offset per identity dengan catalog fingerprint. Legacy hash-only receipt di tengah checkpoint tetap fail closed; tidak dimigrasikan dengan menebak event yang telah tercakup. `auto` dengan replace filters tidak dianggap full truncate. Full rebuild menolak matching source filters dan Trino reset. Submission-bound marker serta durable snapshot start mengizinkan snapshot suffix restart tanpa menghapus prefix lagi; unit tests memeriksa missing/confirmed state, missing position, metadata failure dan ownership loss. Actual target truncate/reload diuji dengan Hadoop catalog, tetapi production REST/S3 dan consumer cutover belum diuji. Final evidence follow-up: `/private/tmp/rivus-rebuild-verified-integration.jsonl`, 34 PASS tanpa skips. Fixture containers/volumes dibersihkan setelah verification.

Ini menguji actual process crash, metadata-server crash, lease failover, dan local Iceberg persistence. Catalog response loss/unavailability memakai test decorators, bukan network fault pada REST server nyata. REST/S3, source-server crash, snapshot/rolling-spool SIGKILL, external writers/rollback/expiration, host failure, dan exactly-once untuk semua kombinasi config belum dibuktikan.

Hasil dan fix shutdown ordering yang ditemukan pada suite ini dicatat di `/Users/gerin/app/rivus-oss-learning/20-automatic-recovery-crash-integration.md`.

## Compatibility upgrade dan rollback

Crash tests dengan writer/reader versi yang sama belum cukup untuk membuktikan compatibility. Build script berikut mengekspor commit deployed **ff300c01937e5bbd408aa268a4b0923b53e211c6** dari Git dan menambahkan test probes tanpa mengubah production code baseline. Binary bukan build ulang dari current worktree. Revision stamp wajib cocok; jika integration dipilih tetapi binary/stamp tidak tersedia, test **FAIL**, bukan skip.

Setelah fixture Colima di atas berjalan:

```sh
bash tests/recovery/build-compatibility-baseline.sh /tmp/rivus-ff300c0.test
env RIVUS_RECOVERY_INTEGRATION=1 RIVUS_COMPATIBILITY_BASELINE_BINARY=/tmp/rivus-ff300c0.test go test -p=1 -tags=integration ./pkg/connectors/iceberg ./pkg/meta -run '^(TestCompatibilityReal|TestCompatibilitySchemaReal)' -count=1 -v -timeout=3m
```

Enam integration cases memeriksa:

1. New rebuild protocol ditolak old binary sebelum catalog access.
2. Old binary SIGKILL setelah real catalog commit meninggalkan legacy PREPARED receipt; new worker membuktikan fingerprint asli, replay tanpa recommit, checkpoint dan cleanup. Oracle: satu row, satu snapshot.
3. New worker menulis version 2 receipt; old binary rollback menolak format, mempertahankan checkpoint/receipt dan tidak menulis data tambahan.
4. Old binary melakukan checkpoint mutation setelah takeover oleh new worker; existing fence menolak mutation.
5. Old registry DDL di disposable database dimigrasikan dua kali; config/status dipertahankan dan submission token stabil.
6. Old snapshot/progress DDL dimigrasikan dua kali; checkpoint/start/progress tetap utuh, legacy done tetap version 0/BLOCKED, unfinished snapshot dapat menjalani metadata completion barrier secara idempotent.

Seluruh **40 leaf scenarios** (34 baseline + 6 compatibility), termasuk mysqld crash:

```sh
env RIVUS_RECOVERY_INTEGRATION=1 RIVUS_RECOVERY_CONTAINER_FAULTS=1 RIVUS_COMPATIBILITY_BASELINE_BINARY=/tmp/rivus-ff300c0.test go test -p=1 -tags=integration ./pkg/api ./pkg/core ./pkg/connectors/iceberg ./pkg/meta -run '^(TestCompatibilityReal|TestCompatibilitySchemaReal|TestDeleteRecoveryReal|TestFreshSnapshotReal|TestPurgedBinlog|TestJobRebuildReal|TestAutomaticRecoveryReal|TestWorkerFenceReal|TestRecoveryReal)' -count=1 -v -timeout=5m
docker --context colima compose -f tests/recovery/compose.yml down --volumes
```

CI menjalankan enam compatibility cases di MySQL Docker fixtures setiap push/PR; build baseline memakai full Git history. CI tidak menjalankan Colima-specific mysqld fault suite. Tests lintas binary memakai deterministic source events dan real metadata/Hadoop catalog, bukan actual source CDC pada tiap case; actual MySQL CDC tetap diuji oleh baseline. Schema tests memverifikasi metadata barrier, bukan sink data commit. Unit regressions tambahan menjaga STOPPING restoration, legacy partial-prefix rejection, marker/submission matching dan last verification cause ketika timeout.

Rollback yang fail closed berarti data/checkpoint dilindungi, bukan job pasti bisa running pada image lama. Version 2 receipt dan `rebuild-truncate-append-v1` memerlukan compatible worker. Old worker dapat claim lalu fail; rolling upgrade availability belum dijamin. Semua earlier releases, REST/S3, Doris migration dan setiap scheduling kombinasi old/new belum diuji. Runbook compatibility dan remaining production cases: `/Users/gerin/app/rivus-oss-learning/24-upgrade-rollback-compatibility.md`.

## Filtered Trino rebuild fixture

**Local verification PASS; rollout diverifikasi terpisah.** Fixture tambahan memakai Trino 483, PostgreSQL JDBC Iceberg catalog, native iceberg-go SQL catalog yang sama, dan warehouse files yang dibagi melalui mount Colima. Adapter `local://` hanya ada pada test dengan build tag `integration`; production catalog constructor tidak diubah. Port PostgreSQL/Trino hanya loopback `49318`/`49319`. PostgreSQL dan Trino credentials hanya fixture. Sisakan sekitar 8–10 GB disk sebelum image pull/build agar VM tidak mengalami I/O error.

Sesi terbaru memakai profil terpisah `rivus-recovery`, karena profil default mengalami I/O/boot error setelah disk host penuh. Volume default, termasuk `mysql_prisma`, tidak dihapus. Default tetap stopped; pemulihannya merupakan pekerjaan terpisah. Docker context aktif tidak diubah.

```sh
colima start --profile rivus-recovery --activate=false --memory 3 --cpu 2 --disk 20
export RIVUS_RECOVERY_TRINO_ROOT=/tmp/colima-rivus-recovery
mkdir -p "$RIVUS_RECOVERY_TRINO_ROOT/rivus-recovery-trino-data" "$RIVUS_RECOVERY_TRINO_ROOT/rivus-recovery-trino-warehouse"
docker --context colima-rivus-recovery compose -f tests/recovery/compose.yml -f tests/recovery/trino-compose.yml up -d --wait
env RIVUS_RECOVERY_TRINO_INTEGRATION=1 GOMAXPROCS=2 go test -p=1 -ldflags='-s -w' -tags=integration ./pkg/connectors/iceberg -run '^TestScopedTrinoReal' -count=1 -v -timeout=8m
```

`RIVUS_RECOVERY_TRINO_ROOT` harus merupakan host directory yang di-mount ke VM dengan path sama. Untuk profil default gunakan `/tmp/colima`; profil terpisah memakai `/tmp/colima-rivus-recovery`. Fixture Trino berjalan sebagai root hanya untuk izin write pada shared mount lokal. JVM heap dibatasi 1 GB. Healthcheck HTTP bukan pengganti keberhasilan SQL pertama.

Sembilan execution cases disiapkan: filtered DELETE/MERGE; terminal response hilang; submit response hilang; target memiliki duplicate PK; SIGKILL setelah terminal response dan sesudah COMMITTED pada reset/MERGE; serta master resubmit purged → snapshot populated/empty source → CDC. SIGKILL cases memakai subprocess sink dan ledger/fence nyata; lifecycle pipeline memakai JobManager/MySQL nyata, tetapi seluruh snapshot-worker progress crash boundaries belum dicakup oleh subprocess tersebut. Query-history disappearance atau missing query identity tetap BLOCKED, tanpa POST retry.

Status aktual: sembilan real Trino execution cases **PASS**, termasuk empat actual SIGKILL/restart cases, lost terminal/submit responses, duplicate target PK admission, dan purged-resubmit pipeline menuju CDC. Test awal menemukan duplicate preserved PK masih diterima serta native row count menghitung physical records tanpa delete files; keduanya dibetulkan tanpa melemahkan expected row/payload assertions. Factory regression tambahan memakai REST HTTP test double dan accepted QueryInfo untuk membuktikan submission token terikat sebelum constructor melakukan reconciliation. No production fault injection.

Verifikasi yang sudah PASS pada worktree terbaru: `go test ./...` (dengan `-p=1 -ldflags='-s -w'` untuk mengurangi penggunaan disk), 20 MySQL archive/reset cases, dua MySQL query-ID persistence/fence cases, empat worker fence cases, enam actual `ff300c0` binary/schema cases, dan 19 native crash/restart cases. Satu metadata-container SIGKILL case tidak dijalankan karena safety guard-nya mengharuskan context default `colima`. Jangan mengaktifkan container faults pada profil baru sebelum guard/target context diperbarui dan diverifikasi.

Evidence lokal: `/private/tmp/rivus-scoped-full-unit-tests.log`, `rivus-scoped-metadata-integration.log`, `rivus-scoped-query-metadata-integration.log`, `rivus-scoped-worker-fence-integration.log`, `rivus-scoped-native-integration.log`, dan `rivus-scoped-compatibility-integration.log`. Tidak ada exactly-once claim atau rollout dari hasil ini.

## UNKNOWN recovery melalui target baru

`TestRecoveryTrinoRealIsolatedTargetKeepsUnknownAndHandsOffCDC` menjalankan normal new-job submission ke namespace baru sementara job lama FAILED/STOPPED mempertahankan PREPARED intent dan unavailable old checkpoint. Source/metadata MySQL, Trino 483/JDBC catalog dan Iceberg files nyata digunakan. Rolling snapshot memakai cutoff September, menguji populated/empty source, sink-confirmed completion, CDC checkpoint, dan original target/checkpoint/UNKNOWN evidence tetap utuh. Bukan same-ID resubmit atau scoped WIP rebuild.

```sh
env RIVUS_RECOVERY_TRINO_ROOT=/tmp/colima-rivus-recovery RIVUS_RECOVERY_TRINO_INTEGRATION=1 GOMAXPROCS=2 go test -p=1 -ldflags='-s -w' -tags=integration ./pkg/connectors/iceberg -run '^TestRecoveryTrinoRealIsolatedTargetKeepsUnknownAndHandsOffCDC$' -count=1 -v -timeout=2m
```

PASS pada current worktree dan exported production commit 74467d5 (production code unchanged; fixture-only test helpers/dependencies). Fixture memakai dua source tables; tidak membuktikan completeness 363 production selections, source retention untuk snapshot panjang, REST/S3 network faults, rolling-spool SIGKILL atau consumer cutover. Shared mount image 483 memakai physical `local.location=/`, agar `local:///warehouse/...` dipetakan ke shared `/warehouse/...`. HTTP health saja sempat lolos sebelum catalog startup gagal; test harus menunggu actual SQL success.

## Job presentation dan final verification

`TestJobPresentationRealRenameSurvivesWorkerUpdates` memakai metadata MySQL nyata: rename tidak mengganti submission, lease, checkpoint atau source/sink config; stale-name worker status write serta control-plane UPSERT tidak menimpa nama baru. `TestJobPresentationRealExplicitStopKeepsRecoveryEvidence` membuktikan explicit stop idempotent mempertahankan PREPARED/checkpoint. UI regressions memastikan STOPPED/STOPPING/FAILED/PAUSED/DONE tidak menampilkan saved CDC progress sebagai aktivitas berjalan; actual progress tetap tersedia pada API.

Final local worktree verification: full `go test ./... -count=1` PASS; related core rename/view race PASS; 60 integration leaf scenarios PASS, satu metadata-container SIGKILL case SKIP karena default-context guard; dua query-ID persistence/fence cases terpisah PASS. Jumlah execution yang lulus **62**, tanpa menghitung skipped case. Combined suite mencakup enam pinned ff300c0 binary/schema compatibility cases dan sembilan Trino cases. Evidence `/private/tmp/rivus-recovery-and-ui-full-unit-final.log`, `rivus-recovery-and-ui-integration-final.log`, `rivus-trino-query-final.log`, `rivus-job-name-race-final.log`, `rivus-job-status-ui-green.log`.

Guard otomatis untuk arbitrary STOPPING tanpa worker lease tidak ditambahkan: asynchronous Delete pada control plane dapat tetap berjalan. Explicit Cancel menyelesaikan durable STOPPED fence; pending deletion tetap dilindungi oleh reservation dan receipt cleanup checks. Runtime connector log/snapshot job-name labels tetap label saat constructor dibuat; rename mengubah display/durable config serta future constructors tanpa restart connector.

## Native scoped snapshot writer

Use the same isolated profile and Trino fixture above. New cases: `TestScopedNativeRealBulkLargeRowPartitionMoveAndExpiredHistory`, `TestScopedNativeRealSIGKILLResumesWithoutResetOrDuplicate`, and `TestNativeRebuildUpgradeRealPreservesEpochCursorAndReceipts`. The existing scoped pipeline now exercises the default v2 native data writer; legacy v1 execution/crash cases remain. Source/target/metadata are loopback fixture services only.

```sh
env RIVUS_RECOVERY_INTEGRATION=1 RIVUS_RECOVERY_TRINO_INTEGRATION=1 RIVUS_RECOVERY_TRINO_ROOT=/tmp/colima-rivus-recovery GOMAXPROCS=2 go test -p=1 -ldflags='-s -w' -tags=integration ./pkg/connectors/iceberg ./pkg/meta -run '^(TestScopedTrinoReal|TestScopedNativeReal|TestNativeRebuildUpgradeReal|TestJobRebuildRealMySQL)' -count=1 -v -timeout=4m
```

39 integration leaf cases passed on October 14. Full unit suite passed. The large-row case includes a 1.33 MB value, preserved outside/NULL-date rows, a PK moving partitions, positive catalog proof after query-history expiry, one native bulk commit and no new MERGE mutation. Two native SIGKILL cases confirm that the reset and accepted prefix are not repeated; changing chunk size during replay retains the accepted range.
