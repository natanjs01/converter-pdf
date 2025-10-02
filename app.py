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

# ======== Excel: extrator com grade de colunas (sem Camelot) ========
import statistics

EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384

def _ensure_sheet_capacity(wb, ws_name_base, ws, rows_to_add_len, cols_to_add_len):
    cur_rows = ws.max_row or 0
    cur_cols = ws.max_column or 0
    need_new = ((cur_rows + rows_to_add_len) > EXCEL_MAX_ROWS) or (max(cur_cols, cols_to_add_len) > EXCEL_MAX_COLS)
    if not need_new:
        return ws
    idx = 2
    existing = {s.title for s in wb.worksheets}
    while f"{ws_name_base}_{idx}" in existing:
        idx += 1
    return wb.create_sheet(f"{ws_name_base}_{idx}")

def _append_rows(wb, ws, ws_base_name, rows, add_separator=False):
    if add_separator and ws.max_row > 0:
        ws = _ensure_sheet_capacity(wb, ws_base_name, ws, 1, 1)
        ws.append([])
    for row in rows:
        ws2 = _ensure_sheet_capacity(wb, ws_base_name, ws, 1, len(row))
        if ws2 is not ws:
            ws = ws2
        ws.append(row)
    return ws

def _clean_text(s: str) -> str:
    return " ".join((s or "").replace("\t"," ").replace("\xa0"," ").split())

def _page_words(page):
    # palavras com coordenadas; manter ordenação natural
    words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
    # filtra detritos
    words = [w for w in words if _clean_text(w.get("text",""))]
    return words

def _build_column_grid(words, max_cols=14):
    """
    Cria fronteiras de colunas pela análise de gaps entre x0 das palavras.
    - Junta 'clusters' onde o gap é pequeno
    - Coloca uma fronteira quando o gap >> mediana
    Retorna lista de limiares X (col_boundaries) e uma função que mapeia x -> idx da coluna
    """
    if not words:
        return [0, 1e9], lambda x: 0

    xs = sorted(w["x0"] for w in words)
    gaps = [xs[i+1] - xs[i] for i in range(len(xs)-1)]
    if not gaps:
        return [0, 1e9], lambda x: 0

    med = statistics.median(gaps)
    p90 = sorted(gaps)[int(len(gaps)*0.90)] if len(gaps) >= 10 else max(gaps)
    # threshold adaptativo: valor alto entre mediana*2.8 e percentil 90
    thr = max(med*2.8, p90)

    # fronteiras onde gap é grande
    boundaries = [xs[0] - 5]  # margem à esquerda
    acc = xs[0]
    for i, g in enumerate(gaps):
        if g >= thr:
            boundaries.append(xs[i] + g/2)
    boundaries.append(xs[-1] + 5)       # margem à direita

    # limita nº de colunas
    if len(boundaries) - 1 > max_cols:
        step = (len(boundaries) - 1) / max_cols
        new_b = [boundaries[0]]
        acc = 0.0
        for _ in range(max_cols-1):
            acc += step
            idx = int(round(acc))
            new_b.append(boundaries[idx])
        new_b.append(boundaries[-1])
        boundaries = new_b

    def col_index(x):
        # binária simples
        lo, hi = 0, len(boundaries)-1
        while lo < hi:
            mid = (lo + hi) // 2
            if x < boundaries[mid]:
                hi = mid
            else:
                lo = mid + 1
        return max(0, min(lo-1, len(boundaries)-2))

    return boundaries, col_index

def _group_rows_by_y(words, y_tol=3):
    """
    Agrupa palavras em linhas pelo Y (com tolerância).
    Retorna lista de linhas, cada linha é lista de palavras (ordenadas por x0).
    """
    rows = {}
    for w in words:
        ybin = round(w["top"] / y_tol)
        rows.setdefault(ybin, []).append(w)
    lines = []
    for _, items in sorted(rows.items(), key=lambda kv: kv[0]):
        items.sort(key=lambda w: w["x0"])
        lines.append(items)
    return lines

def _materialize_table(words):
    """
    Constrói tabela com N colunas fixas:
      1) grid de colunas global por página
      2) mapeia cada palavra -> coluna
      3) cola palavras vizinhas na mesma célula (gap pequeno)
    """
    if not words:
        return []

    # 1) grade global de colunas
    boundaries, col_index = _build_column_grid(words)

    # 2) linhas por Y
    lines = _group_rows_by_y(words)

    ncols = max(1, len(boundaries) - 1)
    table = []

    for line_words in lines:
        cells = [""] * ncols
        # varre da esquerda pra direita, agrupando palavras próximas
        prev_col = None
        prev_x1 = None
        for w in line_words:
            col = col_index(w["x0"])
            text = _clean_text(w["text"])
            if not text:
                continue

            # se mesma coluna e muito perto do texto anterior: concatena
            if prev_col is not None and col == prev_col and prev_x1 is not None:
                gap = w["x0"] - prev_x1
                # limite de "união" depende da largura média da coluna
                avg_col_width = (boundaries[col+1] - boundaries[col])
                join_limit = max(8, avg_col_width * 0.15)  # adaptativo
                if gap <= join_limit:
                    cells[col] = (cells[col] + " " + text).strip()
                else:
                    # mesma coluna, mas gap grande -> adiciona com espaço
                    cells[col] = (cells[col] + " " + text).strip()
                prev_x1 = w["x1"]
                continue

            # mudou de coluna: coloca texto na coluna nova
            if cells[col]:
                cells[col] = (cells[col] + " " + text).strip()
            else:
                cells[col] = text

            prev_col = col
            prev_x1 = w["x1"]

        # remove linha vazia?
        if any(_clean_text(c) for c in cells):
            table.append([_clean_text(c) for c in cells])

    return table

def pdf_to_excel(file_stream_or_path) -> bytes:
    """
    Extrai palavras com pdfplumber, calcula grade de colunas por página,
    alinha todas as linhas nessa grade e escreve em uma única aba 'Dados'
    (criando Dados_2, _3... se necessário).
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Dados"
    ws_base = "Dados"

    # trabalhar com caminho físico melhora compatibilidade
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
                words = _page_words(page)
                rows = _materialize_table(words)

                if rows:
                    # separador visual entre páginas (sem quebrar grade)
                    if ws.max_row > 0:
                        ws = _append_rows(wb, ws, ws_base, [[]], add_separator=False)
                    ws = _append_rows(wb, ws, ws_base, rows, add_separator=False)
                else:
                    ws = _append_rows(wb, ws, ws_base, [[f"(Página {pidx} sem conteúdo detectado)"]], add_separator=True)
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

# ======== Endpoint principal ========
@app.post("/convert")
def convert():
    to_fmt = request.args.get("to", "").lower()
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
                    out = pdf_to_excel(fp)
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
