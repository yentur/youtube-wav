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
  --batch-size 10 \
  --concurrency 4 \
  --machine-id $(hostname)
```

All flags can also come from env vars: `API_BASE_URL`, `API_TOKEN`,
`BATCH_SIZE`, `CONCURRENCY`, `MACHINE_ID`, `COOKIES_FILE`, `WORKDIR`.

The orchestrator hands out AWS credentials and link batches **only** to
clients that present the right token via `Authorization: Bearer <token>`
(or `X-API-Token`). Without it `/config`, `/batch`, `/report` all return
`401`. Ask the operator for the token; never check it into git.

### Optional: cookies for age-restricted / region-locked videos

Pass a Netscape-format `cookies.txt`:

```bash
python main.py --cookies /path/to/www.youtube.com_cookies.txt
```

## Flags

| Flag             | Default                       | Meaning                              |
|------------------|-------------------------------|--------------------------------------|
| `--api`          | `http://51.102.128.158:8000`  | Orchestrator base URL                |
| `--token`        | _(env `API_TOKEN`)_           | Shared secret for the orchestrator   |
| `--machine-id`   | `$(hostname)`                 | Unique id reported back to server    |
| `--batch-size`   | `10`                          | Links pulled per `/batch` call       |
| `--concurrency`  | `4`                           | Parallel downloads inside a batch    |
| `--cookies`      | _(none)_                      | Path to a YouTube `cookies.txt`      |
| `--workdir`      | _(temp dir)_                  | Where to stage downloads             |
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
server-side total + ETA, and remaining links per type.

## Retry / failure semantics

- The orchestrator marks a link **final-failed** once **3 different
  machines** have failed it (and the failure isn't a cookie/auth error).
- Cookie/auth failures are not counted toward the 3-strike limit — the link
  is re-queued so a different machine (with different cookies) can try.
- Already-uploaded links (WAV present on S3) are reported as `skipped` so
  no work is duplicated when a machine restarts.

## Crash safety

- The client is stateless — kill it, restart it, scale it horizontally.
- The orchestrator reclaims any batch that hasn't been reported within
  30 minutes and re-queues it.

## Troubleshooting

| Symptom                                    | Try                                       |
|--------------------------------------------|-------------------------------------------|
| Lots of `cookie` errors                    | Pass `--cookies cookies.txt`              |
| `wav_missing` errors                       | `apt install ffmpeg`                      |
| Hangs on `boto3` upload                    | Check egress firewall / S3 credentials    |
| `get_batch` retries                        | Server URL wrong or server down           |
