# youtube-wav — distributed client

A client worker that pulls batches of YouTube links from an orchestrator,
downloads each as **16 kHz WAV** (plus auto-subtitle when available), uploads
the result to **S3**, and reports back. Designed for **fan-out across many
VPSes** (vast.ai / cloud GPUs / anything Linux).

The orchestrator decides:
- which links to assign,
- the type folder on S3 (`tts` first, then `stt`, then `tv`),
- when a link should be retried elsewhere or finally given up on.

You bring the bandwidth.

---

## Install

```bash
git clone https://github.com/yentur/youtube-wav.git
cd youtube-wav
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
sudo apt-get install -y ffmpeg     # required by yt-dlp WAV extraction
```

## Run

```bash
export API_TOKEN='<shared secret from the orchestrator owner>'

python main.py \
  --api http://51.102.128.158:8000 \
  --batch-size 64 \
  --concurrency 8 \
  --machine-id $(hostname)
```

All flags can also come from env vars: `API_BASE_URL`, `API_TOKEN`,
`BATCH_SIZE`, `CONCURRENCY`, `MACHINE_ID`, `WORKDIR`, `LOG_FILE`.

The orchestrator hands out AWS credentials and link batches **only** to
clients that present the right token via `Authorization: Bearer <token>`
(or `X-API-Token`). Without it `/config`, `/batch`, `/report` all return
`401`. Ask the operator for the token; never check it into git.

## Flags

| Flag             | Default                       | Meaning                              |
|------------------|-------------------------------|--------------------------------------|
| `--api`          | `http://51.102.128.158:8000`  | Orchestrator base URL                |
| `--token`        | _(env `API_TOKEN`)_           | Shared secret for the orchestrator   |
| `--machine-id`   | `$(hostname)`                 | Unique id reported back to server    |
| `--batch-size`   | `64`                          | Links pulled per `/batch` call       |
| `--concurrency`  | `8`                           | Parallel downloads inside a batch    |
| `--workdir`      | _(temp dir)_                  | Where to stage downloads             |
| `--log-file`     | `download_log.csv`            | Per-video CSV log path               |
| `--no-log-file`  | off                           | Disable the CSV log                  |
| `--keep-files`   | off                           | Don't delete WAV/SRT after upload    |

## Output

Files are uploaded to:

```
s3://<bucket>/<type>/<video_id>.wav
s3://<bucket>/<type>/<video_id>.srt   # only if auto-subtitle exists
```

`<type>` is `tts`, `stt`, or `tv` — chosen by the orchestrator based on
the row's `type` column in the master `videolist.csv`.

## Live dashboard

Built with [rich](https://github.com/Textualize/rich): a single panel shows
machine id, active type, success/skip/fail counters, upload throughput,
server-side total + ETA, and remaining links per type. Per-video status
lines stream above the dashboard:

```
▶ batch #N [type] pulled K links
✓ #N·k/K [type] <video_id> <bytes> in Xs
⤼ #N·k/K [type] <video_id> already on S3
✗ #N·k/K [type] <video_id> <error_class>: <short err>
▶ batch #N done in Xs — n ok / n skip / n fail
```

## Retry / failure semantics

- **Transient errors** are not counted toward the fail limit — the link
  goes back into the orchestrator's queue immediately. The two transient
  classes are:
  - `cookie` (auth / login walls / age-gate)
  - `ratelimit` (YouTube 429 / "session has been rate-limited")
- A link is finally marked `failed` only after **3 different machines**
  hit a non-transient error on it (`extract`, `network`, `other`).
- Previously-failed links are re-queued automatically when the
  orchestrator restarts; only `success` and `skipped` rows in
  `results.csv` are treated as final.
- Already-uploaded links (WAV present on S3) are reported as `skipped` so
  no work is duplicated when a machine restarts.

## Crash safety

- The client is stateless — kill it, restart it, scale it horizontally.
- The orchestrator reclaims any batch that hasn't been reported within
  30 minutes and re-queues it.

## Troubleshooting

| Symptom                                  | Try                                            |
|------------------------------------------|------------------------------------------------|
| Lots of `ratelimit` errors               | Run from more / different IPs; reduce concurrency |
| Lots of `cookie`-classed auth errors     | Datacenter IPs are bot-blocked; use residential / vast.ai |
| `wav_missing` errors                     | `apt install ffmpeg`                           |
| Hangs on `boto3` upload                  | Check egress firewall / S3 credentials         |
| `get_batch` retries                      | Server URL wrong or server down                |
