"""
Flask UI around ``onlygemini.run_all``: pick a subset of ``onlygemini.COMPANIES`` (no
default selection), optional reference uploads, then **one Gemini request per
selected company**. Progress is shown in the browser and polls until the workbook
is ready to download.

Requires ``GEMINI_API_KEY`` or ``GOOGLE_API_KEY``. Install Flask: ``pip install flask``.

Run::

    export GEMINI_API_KEY=...
    export FLASK_SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")
    python onlygemini_flask.py

Then open the printed URL in **Chrome or Safari**. Cursor’s Simple Browser / preview
often shows “Access to 127.0.0.1 was denied” for local servers—that is a client
restriction, not Flask. Use a normal browser window, or try ``http://localhost:PORT/``.

Environment:
    FLASK_SECRET_KEY, ONLYGEMINI_FLASK_MAX_UPLOAD_MB,
    ONLYGEMINI_FLASK_HOST (default ``0.0.0.0`` — listen on all interfaces; use
    ``127.0.0.1`` if you want loopback-only),
    ONLYGEMINI_FLASK_PORT (default ``5000``).
"""
from __future__ import annotations

import io
import mimetypes
import os
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, render_template_string, request, send_file, url_for

from onlygemini import COMPANIES, run_all

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-change-for-production")

_max_mb = int(os.environ.get("ONLYGEMINI_FLASK_MAX_UPLOAD_MB", "40"))
app.config["MAX_CONTENT_LENGTH"] = max(1, _max_mb) * 1024 * 1024

_jobs_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}


def _allowed_company_subset(requested: list[str]) -> list[str] | None:
    """Return companies in ``COMPANIES`` list order, or None if nothing valid."""
    allowed = set(COMPANIES)
    picked = {c for c in requested if c in allowed}
    if not picked:
        return None
    return [c for c in COMPANIES if c in picked]


def _collect_uploads() -> list[tuple[bytes, str]]:
    file_parts: list[tuple[bytes, str]] = []
    for f in request.files.getlist("files"):
        if not f or not getattr(f, "filename", None) or not str(f.filename).strip():
            continue
        raw = f.read()
        if not raw:
            continue
        mime = (f.mimetype or "").strip()
        if not mime or mime == "application/octet-stream":
            guessed, _ = mimetypes.guess_type(f.filename)
            mime = (guessed or "application/octet-stream").strip()
        file_parts.append((raw, mime))
    return file_parts


def _run_job(
    job_id: str,
    companies: list[str],
    file_parts: list[tuple[bytes, str]],
    out_path: Path,
) -> None:
    def cb(done: int, total: int, label: str) -> None:
        with _jobs_lock:
            j = _jobs.get(job_id)
            if j is not None:
                j["done"] = done
                j["total"] = total
                j["label"] = label

    try:
        run_all(
            companies=companies,
            output_path=out_path,
            verbose=False,
            uploaded_file_parts=file_parts if file_parts else None,
            use_per_company_requests=True,
            progress_callback=cb,
        )
        with _jobs_lock:
            j = _jobs.get(job_id)
            if j is not None:
                j["status"] = "done"
                j["output_path"] = str(out_path)
    except Exception as e:
        try:
            out_path.unlink(missing_ok=True)
        except OSError:
            pass
        with _jobs_lock:
            j = _jobs.get(job_id)
            if j is not None:
                j["status"] = "error"
                j["error"] = str(e)


_INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>OnlyGemini</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 46rem; margin: 2rem auto; padding: 0 1rem; }
    p.note { color: #444; font-size: 0.95rem; }
    label { display: block; margin-top: 1rem; font-weight: 600; }
    .company-panel {
      margin-top: 0.5rem;
      border: 1px solid #ccc;
      border-radius: 6px;
      padding: 0.75rem 1rem;
      max-height: 14rem;
      overflow: auto;
      background: #fafafa;
    }
    .company-panel label {
      font-weight: normal;
      margin-top: 0.35rem;
      cursor: pointer;
    }
    .company-panel input { margin-right: 0.5rem; }
    .toolbar { margin-top: 0.5rem; display: flex; gap: 0.5rem; flex-wrap: wrap; }
    .toolbar button { font-size: 0.85rem; padding: 0.25rem 0.6rem; cursor: pointer; }
    button#runBtn { margin-top: 1.25rem; padding: 0.5rem 1rem; font-size: 1rem; cursor: pointer; }
    #runBtn:disabled { opacity: 0.6; cursor: not-allowed; }
    #progressWrap { margin-top: 1.5rem; display: none; }
    #progressWrap.active { display: block; }
    #progressCounts { font-size: 1.15rem; font-weight: 600; margin-bottom: 0.35rem; }
    #progressBar {
      height: 1.1rem;
      border-radius: 4px;
      background: #e8e8e8;
      overflow: hidden;
    }
    #progressBar > i {
      display: block;
      height: 100%;
      width: 0%;
      background: #1a6;
      transition: width 0.25s ease;
    }
    #detail { color: #333; font-size: 0.95rem; margin-top: 0.5rem; min-height: 1.25rem; }
    .err { color: #a40000; margin-top: 1rem; white-space: pre-wrap; }
  </style>
</head>
<body>
  <h1>Advanced therapy estimates</h1>
  <p class="note">Choose which companies to include (same list as <code>onlygemini.COMPANIES</code>).
  Each selected name triggers its own Gemini request—nothing is batched into one call for all.
  Optional reference files are sent on every request.</p>

  <form id="form" enctype="multipart/form-data">
    <label>Companies <span style="font-weight:normal;color:#666">(select at least one)</span></label>
    <div class="company-panel" id="companyPanel">
      {% for c in companies %}
      <label><input type="checkbox" name="companies" value="{{ c|e }}"/> {{ c }}</label>
      {% endfor %}
    </div>
    <div class="toolbar">
      <button type="button" id="btnAll">Select all</button>
      <button type="button" id="btnNone">Clear</button>
    </div>

    <label for="files">Reference files (optional)</label>
    <input id="files" type="file" name="files" multiple accept=".pdf,.png,.jpg,.jpeg,.webp,.txt,.html,.xlsx,.docx"/>

    <div><button type="submit" id="runBtn">Run &amp; download Excel</button></div>
  </form>

  <div id="progressWrap" aria-live="polite">
    <div id="progressCounts"></div>
    <div id="progressBar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" aria-label="Run progress">
      <i id="progressFill"></i>
    </div>
    <div id="detail"></div>
  </div>
  <p id="err" class="err" style="display:none;"></p>

  <script>
  const form = document.getElementById("form");
  const runBtn = document.getElementById("runBtn");
  const progressWrap = document.getElementById("progressWrap");
  const progressCounts = document.getElementById("progressCounts");
  const progressBar = document.getElementById("progressBar");
  const progressFill = document.getElementById("progressFill");
  const detailEl = document.getElementById("detail");
  const errEl = document.getElementById("err");
  const boxes = () => Array.from(form.querySelectorAll('input[name="companies"]'));

  document.getElementById("btnAll").addEventListener("click", () => {
    boxes().forEach((b) => { b.checked = true; });
  });
  document.getElementById("btnNone").addEventListener("click", () => {
    boxes().forEach((b) => { b.checked = false; });
  });

  function showErr(msg) {
    errEl.style.display = msg ? "block" : "none";
    errEl.textContent = msg || "";
  }

  function setProgress(done, total, label) {
    progressWrap.classList.add("active");
    const pct = total > 0 ? Math.min(100, Math.round((100 * done) / total)) : 0;
    progressCounts.textContent = done + " / " + total + " companies completed";
    progressFill.style.width = pct + "%";
    progressBar.setAttribute("aria-valuenow", String(pct));
    progressBar.setAttribute("aria-valuemax", "100");
    detailEl.textContent = label || "";
  }

  async function poll(jobId) {
    const statusUrl = "{{ url_status }}".replace("JOB", jobId);
    const downloadUrl = "{{ url_download }}".replace("JOB", jobId);
    const t = setInterval(async () => {
      try {
        const r = await fetch(statusUrl);
        const j = await r.json();
        if (!r.ok) {
          clearInterval(t);
          runBtn.disabled = false;
          showErr(j.error || r.statusText);
          return;
        }
        setProgress(j.done, j.total, j.label);
        if (j.status === "done") {
          clearInterval(t);
          setProgress(j.total, j.total, "Downloading spreadsheet…");
          window.location = downloadUrl;
          runBtn.disabled = false;
        }
        if (j.status === "error") {
          clearInterval(t);
          runBtn.disabled = false;
          showErr(j.error || "Job failed");
        }
      } catch (e) {
        clearInterval(t);
        runBtn.disabled = false;
        showErr(String(e));
      }
    }, 200);
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    showErr("");
    const chosen = boxes().filter((b) => b.checked);
    if (chosen.length === 0) {
      showErr("Select at least one company.");
      return;
    }
    progressWrap.classList.remove("active");
    progressFill.style.width = "0%";
    detailEl.textContent = "";
    runBtn.disabled = true;
    const fd = new FormData(form);
    try {
      const r = await fetch("{{ url_create_job }}", { method: "POST", body: fd });
      const j = await r.json();
      if (!r.ok) {
        runBtn.disabled = false;
        showErr(j.error || r.statusText);
        return;
      }
      setProgress(0, j.total, "Queued…");
      poll(j.job_id);
    } catch (ex) {
      runBtn.disabled = false;
      showErr(String(ex));
    }
  });
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def index() -> str:
    return render_template_string(
        _INDEX_HTML,
        companies=COMPANIES,
        url_create_job=url_for("create_job"),
        url_status=url_for("job_status", job_id="JOB"),
        url_download=url_for("job_download", job_id="JOB"),
    )


@app.post("/api/jobs")
def create_job():
    if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
        return jsonify(error="Set GEMINI_API_KEY or GOOGLE_API_KEY."), 400

    selected = _allowed_company_subset(request.form.getlist("companies"))
    if not selected:
        return jsonify(error="Select at least one valid company."), 400

    file_parts = _collect_uploads()
    job_id = str(uuid.uuid4())
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    out_path = Path(tmp.name)
    tmp.close()

    with _jobs_lock:
        _jobs[job_id] = {
            "done": 0,
            "total": len(selected),
            "label": "",
            "status": "running",
            "error": None,
            "output_path": None,
        }

    thread = threading.Thread(
        target=_run_job,
        args=(job_id, selected, file_parts, out_path),
        daemon=True,
    )
    thread.start()
    return jsonify(job_id=job_id, total=len(selected))


@app.get("/api/jobs/<job_id>")
def job_status(job_id: str):
    with _jobs_lock:
        j = _jobs.get(job_id)
    if j is None:
        return jsonify(error="Unknown job_id."), 404
    return jsonify(
        done=j["done"],
        total=j["total"],
        label=j.get("label", ""),
        status=j["status"],
        error=j.get("error"),
    )


@app.get("/api/jobs/<job_id>/download")
def job_download(job_id: str):
    with _jobs_lock:
        j = _jobs.get(job_id)
    if j is None or j.get("status") != "done":
        abort(404)
    path = Path(j["output_path"])
    if not path.is_file():
        abort(404)
    try:
        payload = path.read_bytes()
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        with _jobs_lock:
            _jobs.pop(job_id, None)

    return send_file(
        io.BytesIO(payload),
        as_attachment=True,
        download_name="onlygemini_results.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def main() -> None:
    # 0.0.0.0 avoids some IPv4/localhost mismatches; use 127.0.0.1 for loopback-only.
    host = os.environ.get("ONLYGEMINI_FLASK_HOST", "0.0.0.0")
    try:
        port = int(os.environ.get("ONLYGEMINI_FLASK_PORT", "5000"))
    except ValueError:
        port = 5000
    loopback = f"http://127.0.0.1:{port}/"
    localhost = f"http://localhost:{port}/"
    print(
        f"\nonlygemini_flask listening on http://{host}:{port}/\n"
        f"Open in an external browser: {loopback} or {localhost}\n"
        "If the IDE preview says access denied, use Chrome/Safari (not Simple Browser).\n",
        flush=True,
    )
    app.run(
        host=host,
        port=port,
        threaded=True,
        debug=os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true", "yes"),
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
