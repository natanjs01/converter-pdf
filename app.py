# app.py - Backend Flask (PDF -> DOCX/XLSX) com CORS robusto para GitHub Pages
# Front: https://natanjs01.github.io
# Endpoint: POST /convert?to=excel|word

import io, os, tempfile, warnings
from typing import List
from flask import Flask, request, send_file, abort, make_response
from flask_cors import CORS
import pdfplumber
from docx import Document
from openpyxl import Workbook

warnings.filterwarnings("ignore", category=DeprecationWarning)

app = Flask(__name__)
MAX_MB = 20

# ---- CORS principal (flask-cors)
ALLOWED_ORIGINS = {"https://natanjs01.github.io"}
CORS(
    app,
    resources={r"/convert": {"origins": list(ALLOWED_ORIGINS)}},
    methods=["POST", "OPTIONS"],
    allow_headers=["Content-Type"],
    max_age=86400,
)

# ---- CORS extra (garantia em todas as respostas, inclusive erros)
@app.after_request
def add_cors_headers(resp):
    origin = request.headers.get("Origin", "")
    if origin in ALLOWED_ORIGINS:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

# ======== Conversão para Word ========
def pdf_to_docx(file_stream) -> bytes:
    doc = Document()
    with pdfplumber.open(file_stream) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            if i > 1:
                doc.add_page_break()
            doc.add_heading(f"Página {i}", level=2)
            text = page.extract_text() or ""
            for line in text.splitlines():
                doc.add_paragraph(line)
    bio = io.BytesIO(); doc.save(bio); bio.seek(0)
    return bio.read()

# ======== Excel robusto: auto (tabela x formulário) + saneamento ========
import re, statistics
from openpyxl import Workbook

EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384
CELL_MAX = 32_000  # margem de segurança (< 32767)

_xml_illegal_re = re.compile(
    u"[\u0000-\u0008\u000b\u000c\u000e-\u001f\uD800-\uDFFF\uFFFE\uFFFF]"
)

def _sanitize_cell(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = _xml_illegal_re.sub("", s)
    s = " ".join(s.replace("\xa0", " ").split())
    if len(s) > CELL_MAX:
        s = s[:CELL_MAX]
    return s

def _ensure_sheet_capacity(wb, ws_name_base, ws, add_rows, add_cols):
    r, c = ws.max_row or 0, ws.max_column or 0
    need = (r + add_rows > EXCEL_MAX_ROWS) or (max(c, add_cols) > EXCEL_MAX_COLS)
    if not need:
        return ws
    i = 2
    names = {s.title for s in wb.worksheets}
    while f"{ws_name_base}_{i}" in names:
        i += 1
    return wb.create_sheet(f"{ws_name_base}_{i}")

def _append_rows(wb, ws, ws_base, rows, sep=False):
    if sep and ws.max_row > 0:
        ws = _ensure_sheet_capacity(wb, ws_base, ws, 1, 1)
        ws.append([])
    for row in rows:
        row = [_sanitize_cell(x) for x in row]
        ws2 = _ensure_sheet_capacity(wb, ws_base, ws, 1, len(row))
        if ws2 is not ws:
            ws = ws2
        ws.append(row)
    return ws

def _words(page):
    w = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
    return [it for it in w if _sanitize_cell(it.get("text"))]

def _group_by_y(words, y_tol=3):
    rows = {}
    for w in words:
        ybin = round(w["top"] / y_tol)
        rows.setdefault(ybin, []).append(w)
    lines = []
    for _, items in sorted(rows.items(), key=lambda kv: kv[0]):
        items.sort(key=lambda w: w["x0"])
        lines.append(items)
    return lines

# ---------- EXTRAÇÃO MODO TABELA (grade por gaps) ----------
def _build_grid(words, max_cols=18):
    if not words:
        return [0, 1e9], lambda x: 0
    xs = sorted(w["x0"] for w in words)
    gaps = [xs[i+1] - xs[i] for i in range(len(xs)-1)]
    if not gaps:
        return [0, 1e9], lambda x: 0
    med = statistics.median(gaps)
    p90 = sorted(gaps)[int(len(gaps)*0.90)] if len(gaps) >= 10 else max(gaps)
    thr = max(med * 2.6, p90)  # sensível o bastante p/ separar colunas
    boundaries = [xs[0] - 6]
    for i, g in enumerate(gaps):
        if g >= thr:
            boundaries.append(xs[i] + g/2)
    boundaries.append(xs[-1] + 6)

    # limita
    if len(boundaries) - 1 > max_cols:
        step = (len(boundaries) - 1) / max_cols
        new_b = [boundaries[0]]
        acc = 0
        for _ in range(max_cols-1):
            acc += step
            new_b.append(boundaries[int(round(acc))])
        new_b.append(boundaries[-1])
        boundaries = new_b

    def col_index(x):
        lo, hi = 0, len(boundaries)-1
        while lo < hi:
            mid = (lo + hi) // 2
            if x < boundaries[mid]:
                hi = mid
            else:
                lo = mid + 1
        return max(0, min(lo-1, len(boundaries)-2))
    return boundaries, col_index

def _materialize_table(words):
    if not words:
        return []
    bounds, cidx = _build_grid(words)
    ncols = max(1, len(bounds) - 1)
    lines = _group_by_y(words)
    table = []
    for line in lines:
        cells = [""] * ncols
        prev_col, prev_x1 = None, None
        for w in line:
            col = cidx(w["x0"])
            text = _sanitize_cell(w["text"])
            if prev_col == col and prev_x1 is not None:
                gap = w["x0"] - prev_x1
                avgw = (bounds[col+1] - bounds[col])
                join = max(8, avgw * 0.15)
                if gap <= join:
                    cells[col] = (cells[col] + " " + text).strip()
                else:
                    cells[col] = (cells[col] + " " + text).strip()
                prev_x1 = w["x1"]
            else:
                cells[col] = (cells[col] + " " + text).strip() if cells[col] else text
                prev_col, prev_x1 = col, w["x1"]
        if any(c for c in cells):
            table.append([_sanitize_cell(c) for c in cells])
    return table

# ---------- EXTRAÇÃO MODO FORMULÁRIO (campo: valor) ----------
_LABEL_HINTS = {"nome", "placa", "data", "telefone", "modelo", "montadora",
                "ano", "km", "código", "descricao", "descrição", "abs", "airbag", "injeção"}

def _is_probably_label(text):
    t = text.lower().rstrip(":")
    return (text.endswith(":")) or (t in _LABEL_HINTS and len(text) <= 25)

def _materialize_form(words):
    """
    Varre linha a linha; se encontrar um token que parece rótulo ("Campo:" ou palavra de rótulo),
    junta todo texto à direita até o próximo rótulo -> produz [Campo | Valor].
    """
    lines = _group_by_y(words)
    rows = []
    for line in lines:
        # junta tudo da linha em pares campo:valor (pode ter vários por linha)
        i = 0
        while i < len(line):
            t = _sanitize_cell(line[i]["text"])
            if _is_probably_label(t):
                campo = t.rstrip(":")
                # pegue tudo à direita até encontrar outro rótulo forte
                j = i + 1
                vals = []
                while j < len(line):
                    tj = _sanitize_cell(line[j]["text"])
                    if _is_probably_label(tj):
                        break
                    vals.append(tj)
                    j += 1
                valor = " ".join(vals).strip()
                rows.append([campo, valor])
                i = j
            else:
                i += 1
    # fallback: se nada virou par, retorna a linha inteira numa coluna única
    if not rows:
        for line in lines:
            rows.append([" ".join(_sanitize_cell(w["text"]) for w in line)])
    return rows

# ---------- AUTO-DET ECÇÃO ----------
def _page_mode(words):
    """
    Heurística simples:
      - muitas ocorrências de ":" e diversidade de x0 -> formulário
      - linhas densas e gaps regulares -> tabela
    """
    if not words:
        return "empty"
    lines = _group_by_y(words)
    colon = sum(1 for w in words if ":" in w["text"])
    uniq_x = len({int(w["x0"] // 10) for w in words})
    avg_line_len = sum(len(l) for l in lines) / max(1, len(lines))
    if colon >= 8 and uniq_x >= 20:
        return "form"
    if avg_line_len >= 6:
        return "table"
    # fallback
    return "form" if colon >= 3 else "table"

def pdf_to_excel(file_stream_or_path, force_mode: str = "auto") -> bytes:
    """
    Gera UMA aba 'Dados' (Dados_2, _3... se exceder) com:
      - modo 'table'  -> grid de colunas por gaps
      - modo 'form'   -> pares Campo | Valor
      - modo 'auto'   -> decide por página
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Dados"
    base = "Dados"

    # trabalhar com caminho físico
    need_cleanup = False
    if isinstance(file_stream_or_path, (str, os.PathLike)):
        pdf_path = str(file_stream_or_path)
    else:
        fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
        with os.fdopen(fd, "wb") as tmp:
            tmp.write(file_stream_or_path.read())
        file_stream_or_path.seek(0)
        need_cleanup = True

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pidx, page in enumerate(pdf.pages, start=1):
                words = _words(page)

                mode = force_mode
                if force_mode == "auto":
                    mode = _page_mode(words)

                if mode == "table":
                    rows = _materialize_table(words)
                elif mode == "form":
                    rows = _materialize_form(words)
                else:
                    rows = [["(Página sem conteúdo)"]] if not words else _materialize_table(words)

                if ws.max_row > 0:
                    ws = _append_rows(wb, ws, base, [[]])  # separador visual

                header = [f"Página {pidx} • modo: {mode}"]
                ws = _append_rows(wb, ws, base, [header])
                ws = _append_rows(wb, ws, base, rows)
    finally:
        if need_cleanup and os.path.exists(pdf_path):
            os.remove(pdf_path)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


# ======== Preflight (OPTIONS) ========
@app.route("/convert", methods=["OPTIONS"])
def convert_options():
    # O flask-cors já cuida, mas devolvemos 204 explicitamente
    resp = make_response("", 204)
    return resp

@app.post("/convert")
def convert():
    to_fmt = request.args.get("to", "").lower()
    mode = (request.args.get("mode") or "auto").lower()  # novo
    f = request.files.get("file")
    if to_fmt not in {"excel", "word"}:
        return abort(400, "Parâmetro 'to' precisa ser 'excel' ou 'word'.")
    if not f:
        return abort(400, "Envie o arquivo no campo 'file'.")
    if f.mimetype != "application/pdf":
        return abort(400, "Envie um PDF válido.")

    f.seek(0, os.SEEK_END); size = f.tell(); f.seek(0)
    if size > MAX_MB * 1024 * 1024:
        return abort(413, f"Arquivo excede {MAX_MB} MB.")

    try:
        if to_fmt == "word":
            out = pdf_to_docx(f.stream)
            return send_file(io.BytesIO(out),
                mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                as_attachment=True,
                download_name=(f.filename or "arquivo").rsplit(".",1)[0] + ".docx")
        else:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(f.read()); tmp_path = tmp.name
            try:
                with open(tmp_path, "rb") as fp:
                    out = pdf_to_excel(fp, force_mode=mode)   # <<< usa o mode
            finally:
                os.remove(tmp_path)
            return send_file(io.BytesIO(out),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=(f.filename or "arquivo").rsplit(".",1)[0] + ".xlsx")
    except Exception as e:
        return abort(500, f"Falha na conversão: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
