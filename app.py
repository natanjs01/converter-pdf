import io
import os
import tempfile
import warnings
from typing import List
from flask import Flask, request, send_file, abort, send_from_directory
import pdfplumber
from docx import Document
from openpyxl import Workbook

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Camelot é opcional (melhora extração de tabelas)
try:
    import camelot
    CAMEL0T_OK = True
except Exception:
    CAMEL0T_OK = False

app = Flask(__name__)
MAX_MB = 20

# ------------------ WORD ------------------

def pdf_to_docx(file_stream) -> bytes:
    """Converte PDF em DOCX simples (texto por linha)."""
    doc = Document()
    with pdfplumber.open(file_stream) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            doc.add_heading(f"Página {i}", level=2)
            for line in text.splitlines():
                doc.add_paragraph(line)
            doc.add_page_break()
    bio = io.BytesIO()
    doc.save(bio)
    bio.seek(0)
    return bio.read()

# ------------------ EXCEL (única aba com overflow) ------------------

EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384

def _camelot_extract_tables(pdf_path: str, page_no: int, flavor: str):
    return camelot.read_pdf(
        pdf_path,
        pages=str(page_no),
        flavor=flavor,
        strip_text="\n",
        edge_tol=50,
        row_tol=10,
    )

def _pdfplumber_tables(page: pdfplumber.page.Page) -> List[List[List[str]]]:
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 5,
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 3,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
    }
    try:
        return page.extract_tables(table_settings=table_settings)
    except Exception:
        return []

def _xy_cluster_rows(page: pdfplumber.page.Page) -> List[List[str]]:
    words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
    if not words:
        text = page.extract_text() or ""
        return [[ln] for ln in text.splitlines() if ln.strip()]

    rows = {}
    y_tol = 3
    for w in words:
        y = round(w["top"] / y_tol)
        rows.setdefault(y, []).append(w)

    result = []
    for _, items in sorted(rows.items(), key=lambda kv: kv[0]):
        items.sort(key=lambda w: w["x0"])
        cols, cur, prev_x1 = [], [], None
        gap_threshold = 20
        for it in items:
            if prev_x1 is None:
                cur.append(it["text"]); prev_x1 = it["x1"]; continue
            gap = it["x0"] - prev_x1
            if gap > gap_threshold:
                cols.append(" ".join(cur)); cur = [it["text"]]
            else:
                cur.append(it["text"])
            prev_x1 = it["x1"]
        if cur: cols.append(" ".join(cur))
        cols = [" ".join(c.split()) for c in cols]
        if any(c.strip() for c in cols):
            result.append(cols)
    return result

def _ensure_sheet_capacity(wb: Workbook, ws_name_base: str, ws, rows_to_add_len: int, cols_to_add_len: int):
    """
    Garante capacidade: se estourar linhas/colunas, cria nova aba e retorna (ws, row_idx).
    """
    # openpyxl não fornece row count sem percorrer, então usamos ws.max_row:
    current_rows = ws.max_row if ws.max_row else 0
    current_cols = ws.max_column if ws.max_column else 0

    need_new_sheet = (
        (current_rows + rows_to_add_len) > EXCEL_MAX_ROWS or
        (max(current_cols, cols_to_add_len) > EXCEL_MAX_COLS)
    )
    if not need_new_sheet:
        return ws

    # cria nova aba com sufixo incremental
    idx = 2
    while f"{ws_name_base}_{idx}" in [s.title for s in wb.worksheets]:
        idx += 1
    return wb.create_sheet(f"{ws_name_base}_{idx}")

def _append_rows(wb: Workbook, ws, ws_base_name: str, rows: List[List[str]], add_separator: bool):
    """
    Adiciona linhas na aba atual; cria nova aba automaticamente se estourar limites.
    """
    # separador visual entre blocos (opcional)
    if add_separator and ws.max_row > 0:
        if ws.max_row + 1 > EXCEL_MAX_ROWS:
            ws = _ensure_sheet_capacity(wb, ws_base_name, ws, 2, 1)
        ws.append([])

    # acrescenta em blocos, verificando capacidade
    for row in rows:
        cols_len = len(row)
        ws_candidate = _ensure_sheet_capacity(wb, ws_base_name, ws, 1, cols_len)
        if ws_candidate is not ws:
            ws = ws_candidate  # mudou de aba
        ws.append(row)
    return ws

def pdf_to_excel(file_stream_or_path) -> bytes:
    """
    Junta tudo em UMA aba 'Dados' (cria 'Dados_2', 'Dados_3' somente se ultrapassar limite).
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Dados"
    ws_base = "Dados"

    # Precisamos de caminho físico para Camelot
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
                page_had_content = False

                # 1) Camelot lattice
                if CAMEL0T_OK:
                    try:
                        tables = _camelot_extract_tables(pdf_path, pidx, "lattice")
                        for t in tables:
                            rows = t.df.values.tolist()
                            ws = _append_rows(wb, ws, ws_base, rows, add_separator=page_had_content)
                            page_had_content = True
                    except Exception:
                        pass
                    # 2) Camelot stream
                    try:
                        tables = _camelot_extract_tables(pdf_path, pidx, "stream")
                        for t in tables:
                            rows = t.df.values.tolist()
                            ws = _append_rows(wb, ws, ws_base, rows, add_separator=page_had_content)
                            page_had_content = True
                    except Exception:
                        pass

                # 3) pdfplumber linhas
                if not page_had_content:
                    tables = _pdfplumber_tables(page)
                    if tables:
                        for tbl in tables:
                            ws = _append_rows(wb, ws, ws_base, tbl, add_separator=page_had_content)
                            page_had_content = True

                # 4) Cluster XY
                if not page_had_content:
                    rows = _xy_cluster_rows(page)
                    if rows:
                        ws = _append_rows(wb, ws, ws_base, rows, add_separator=page_had_content)
                        page_had_content = True

                # Se nada mesmo, coloca marcador (opcional)
                if not page_had_content:
                    ws = _append_rows(wb, ws, ws_base, [["(Página", str(pidx), "sem conteúdo detectado)"]], add_separator=True)

    finally:
        if need_cleanup and os.path.exists(pdf_path):
            os.remove(pdf_path)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# ------------------ FLASK ROUTES ------------------

@app.get("/")
def home():
    return send_from_directory(".", "index.html")

@app.get("/<path:path>")
def static_proxy(path):
    return send_from_directory(".", path)

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

    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)
    if size > MAX_MB * 1024 * 1024:
        return abort(413, f"Arquivo excede {MAX_MB} MB.")

    try:
        if to_fmt == "word":
            out = pdf_to_docx(f.stream)
            return send_file(
                io.BytesIO(out),
                mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                as_attachment=True,
                download_name=(f.filename or "arquivo").rsplit(".", 1)[0] + ".docx"
            )
        else:
            # Para Excel, salve em arquivo temporário para melhor compatibilidade
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(f.read())
                tmp_path = tmp.name
            try:
                with open(tmp_path, "rb") as fp:
                    out = pdf_to_excel(fp)
            finally:
                os.remove(tmp_path)

            return send_file(
                io.BytesIO(out),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=(f.filename or "arquivo").rsplit(".", 1)[0] + ".xlsx"
            )
    except Exception as e:
        return abort(500, f"Falha na conversão: {e}")

if __name__ == "__main__":
    # Rode: python app.py -> http://127.0.0.1:8000
    app.run(host="0.0.0.0", port=8000)
