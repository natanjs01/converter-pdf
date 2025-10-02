# ======== Excel robusto com detectores de "RptReport" e "Desligados" ========
import io, os, re, tempfile, statistics
from typing import List, Tuple, Dict
import pdfplumber
from openpyxl import Workbook

EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384
CELL_MAX = 32_000  # margem segura (< 32767)

_xml_illegal_re = re.compile(u"[\u0000-\u0008\u000b\u000c\u000e-\u001f\uD800-\uDFFF\uFFFE\uFFFF]")

def _sanitize_cell(s: str) -> str:
    if s is None: return ""
    if not isinstance(s, str): s = str(s)
    s = _xml_illegal_re.sub("", s).replace("\xa0", " ")
    s = " ".join(s.split())
    return s[:CELL_MAX]

def _ensure_sheet_capacity(wb, ws_name_base, ws, add_rows, add_cols):
    r, c = ws.max_row or 0, ws.max_column or 0
    need = (r + add_rows > EXCEL_MAX_ROWS) or (max(c, add_cols) > EXCEL_MAX_COLS)
    if not need: return ws
    i = 2
    names = {s.title for s in wb.worksheets}
    while f"{ws_name_base}_{i}" in names: i += 1
    return wb.create_sheet(f"{ws_name_base}_{i}")

def _append_rows(wb, ws, ws_base, rows, sep=False):
    if sep and ws.max_row > 0:
        ws = _ensure_sheet_capacity(wb, ws_base, ws, 1, 1)
        ws.append([])
    for row in rows:
        row = [_sanitize_cell(x) for x in row]
        ws2 = _ensure_sheet_capacity(wb, ws_base, ws, 1, len(row))
        if ws2 is not ws: ws = ws2
        ws.append(row)
    return ws

def _words(page):
    w = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
    return [it for it in w if _sanitize_cell(it.get("text"))]

def _group_by_y(words, y_tol=3):
    rows = {}
    for w in words:
        rows.setdefault(round(w["top"]/y_tol), []).append(w)
    out = []
    for _, items in sorted(rows.items(), key=lambda kv: kv[0]):
        items.sort(key=lambda w: w["x0"])
        out.append(items)
    return out

# ----------------- Detectores -----------------
def _page_text(page) -> str:
    return (page.extract_text() or "").replace("\xa0"," ")

def _detect_rpt_lojas(text: str) -> bool:
    # Relatórios com "LOJA 11 = ..." + cabeçalho CHAPA/NOME/FUNÇÃO/REF/VALOR/SIND
    return ("LOJA" in text and "CHAPA" in text and "FUNÇÃO" in text and "VALOR" in text)

def _detect_rpt_desligados(text: str) -> bool:
    # "Relatório de Colaboradores Desligados" com cabeçalho Nome Cpf Dt.Admissão Dt. Demissão Filial Chapa
    return ("Relatório de Colaboradores" in text and "Desligados" in text and "Nome" in text and "Cpf" in text)

# ----------------- Utilitários de grade por cabeçalho -----------------
def _build_grid_from_header(words, header_tokens: List[str]) -> Tuple[List[float], callable]:
    """
    Procura os tokens do cabeçalho na página e usa os Xs como marcos de coluna.
    Se não achar, devolve grade por gaps.
    """
    # normalização simples
    def norm(s): return _sanitize_cell(s).lower()

    header_pos = {}
    for w in words:
        t = norm(w["text"])
        for token in header_tokens:
            if token in t and token not in header_pos:
                header_pos[token] = (w["x0"] + w["x1"]) / 2.0

    if len(header_pos) >= 2:
        xs = [header_pos[t] for t in header_tokens if t in header_pos]
        xs = sorted(xs)
        # fronteiras = meio entre centros
        bounds = [xs[0] - 30.0]
        for i in range(len(xs)-1):
            bounds.append((xs[i] + xs[i+1]) / 2.0)
        bounds.append(xs[-1] + 80.0)  # margem direita mais folgada
        def col_index(x):
            lo, hi = 0, len(bounds)-1
            while lo < hi:
                mid = (lo + hi) // 2
                if x < bounds[mid]: hi = mid
                else: lo = mid + 1
            return max(0, min(lo-1, len(bounds)-2))
        return bounds, col_index

    # fallback: grade por gaps
    return _build_grid_by_gaps(words)

def _build_grid_by_gaps(words, max_cols=18):
    if not words: return [0,1e9], lambda x:0
    xs = sorted(w["x0"] for w in words)
    gaps = [xs[i+1]-xs[i] for i in range(len(xs)-1)]
    if not gaps: return [0,1e9], lambda x:0
    med = statistics.median(gaps)
    p90 = sorted(gaps)[int(len(gaps)*0.90)] if len(gaps) >= 10 else max(gaps)
    thr = max(med*2.6, p90)
    bounds = [xs[0]-8]
    for i,g in enumerate(gaps):
        if g >= thr: bounds.append(xs[i] + g/2)
    bounds.append(xs[-1]+8)
    if len(bounds)-1 > max_cols:
        step = (len(bounds)-1)/max_cols
        nb=[bounds[0]]; acc=0
        for _ in range(max_cols-1):
            acc += step; nb.append(bounds[int(round(acc))])
        nb.append(bounds[-1]); bounds=nb
    def cidx(x):
        lo,hi=0,len(bounds)-1
        while lo<hi:
            m=(lo+hi)//2
            if x<bounds[m]: hi=m
            else: lo=m+1
        return max(0,min(lo-1,len(bounds)-2))
    return bounds, cidx

# ----------------- RPT_LOJAS -----------------
_HEADERS_LOJAS = ["chapa","nome","funç","ref","valor","sind"]

def _extract_loja_name(line_words: List[dict]) -> str:
    txt = " ".join(_sanitize_cell(w["text"]) for w in line_words)
    # exemplos: "LOJA 33 = LÍDER AUGUSTO MONTENEGRO"
    m = re.search(r"LOJA\s+\d+\s*=\s*(.+)$", txt, flags=re.I)
    return m.group(1).strip() if m else txt.strip()

def _materialize_rpt_lojas(words) -> List[List[str]]:
    lines = _group_by_y(words)
    loja_atual = ""
    rows = []
    # localizar cabeçalho e fixar grade
    bounds, cidx = _build_grid_from_header(words, _HEADERS_LOJAS)

    for line in lines:
        txt = " ".join(_sanitize_cell(w["text"]) for w in line)
        if txt.upper().startswith("LOJA "):
            loja_atual = _extract_loja_name(line)
            continue
        if "TOTAL DE FUNCIONÁRIOS" in txt.upper() or "TOTAL DO EVENTO" in txt.upper():
            continue
        if txt.startswith("Página") or txt.startswith("Relat") or txt.startswith("Data:"):
            continue
        if any(k in txt for k in ["DESC.", "R627", "R687"]):
            # cabeçalhos/títulos do relatório
            continue

        # monta linha por grade
        ncols = max(1, len(bounds)-1)
        cells = [""]*ncols
        for w in line:
            col = cidx(w["x0"])
            cells[col] = (cells[col]+" "+_sanitize_cell(w["text"])).strip()
        if any(cells):
            # garantia de 6 colunas (CHAPA,NOME,FUNÇÃO,REF,VALOR,SIND)
            while len(cells) < 6: cells.append("")
            rows.append([loja_atual] + cells[:6])  # Loja + 6 campos

    # header
    if rows:
        rows.insert(0, ["Loja","Chapa","Nome","Função","Ref","Valor","Sind"])
    return rows

# ----------------- RPT_DESLIGADOS -----------------
_HEADERS_DESL = ["nome","cpf","admiss","demiss","filial","chapa"]

def _materialize_rpt_desligados(words) -> List[List[str]]:
    # grade ancorada em "Nome Cpf Dt.Admissão Dt. Demissão Filial Chapa"
    bounds, cidx = _build_grid_from_header(words, _HEADERS_DESL)
    lines = _group_by_y(words)
    rows = []
    for line in lines:
        txt = " ".join(_sanitize_cell(w["text"]) for w in line)
        if "Relatório de Colaboradores" in txt or "CNPJ:" in txt or "PAG.:" in txt:
            continue
        if txt.startswith("Página") or txt.startswith("Rel:"):
            continue

        ncols = max(1, len(bounds)-1)
        cells = [""]*ncols
        for w in line:
            col = cidx(w["x0"])
            cells[col] = (cells[col]+" "+_sanitize_cell(w["text"])).strip()
        if any(cells):
            while len(cells) < 6: cells.append("")
            rows.append(cells[:6])

    if rows:
        rows.insert(0, ["Nome","CPF","Dt.Admissão","Dt.Demissão","Filial","Chapa"])
    return rows

# ----------------- GENÉRICO (table/form) -----------------
_LABEL_HINTS = {"nome","placa","data","telefone","modelo","montadora",
                "ano","km","código","descricao","descrição","abs","airbag","injeção"}

def _is_probably_label(text):
    t = _sanitize_cell(text).lower().rstrip(":")
    return (text.endswith(":")) or (t in _LABEL_HINTS and len(text) <= 25)

def _materialize_form(words):
    lines = _group_by_y(words)
    rows = []
    for line in lines:
        i = 0
        while i < len(line):
            t = _sanitize_cell(line[i]["text"])
            if _is_probably_label(t):
                campo = t.rstrip(":")
                j = i + 1
                vals = []
                while j < len(line):
                    tj = _sanitize_cell(line[j]["text"])
                    if _is_probably_label(tj): break
                    vals.append(tj); j += 1
                rows.append([campo, " ".join(vals).strip()])
                i = j
            else:
                i += 1
    if not rows:
        for line in lines:
            rows.append([" ".join(_sanitize_cell(w["text"]) for w in line)])
    return rows

def _build_grid_by_gaps_table(words):
    bounds, cidx = _build_grid_by_gaps(words)
    lines = _group_by_y(words)
    table = []
    ncols = max(1, len(bounds)-1)
    for line in lines:
        cells = [""]*ncols
        prev_col, prev_x1 = None, None
        for w in line:
            col = cidx(w["x0"])
            text = _sanitize_cell(w["text"])
            if prev_col == col and prev_x1 is not None:
                cells[col] = (cells[col]+" "+text).strip()
                prev_x1 = w["x1"]
            else:
                cells[col] = (cells[col]+" "+text).strip() if cells[col] else text
                prev_col, prev_x1 = col, w["x1"]
        if any(cells): table.append(cells)
    return table

def _auto_mode(words):
    if not words: return "empty"
    text = " ".join(_sanitize_cell(w["text"]) for w in words)
    colon = text.count(":")
    uniq_x = len({int(w["x0"]//10) for w in words})
    lines = _group_by_y(words)
    avg_len = sum(len(l) for l in lines)/max(1,len(lines))
    if colon >= 8 and uniq_x >= 20: return "form"
    if avg_len >= 6: return "table"
    return "form" if colon >= 3 else "table"

def pdf_to_excel(file_stream_or_path, force_mode: str = "auto") -> bytes:
    """
    Modes:
      - 'rpt'  -> tenta RPT_LOJAS e RPT_DESLIGADOS automaticamente
      - 'table' / 'form'  -> força o modo genérico
      - 'auto' (padrão)   -> detecta (rpt|table|form) por página
    """
    wb = Workbook(); ws = wb.active; ws.title = "Dados"; base = "Dados"

    # trabalhar com caminho físico
    need_cleanup = False
    if isinstance(file_stream_or_path, (str, os.PathLike)):
        pdf_path = str(file_stream_or_path)
    else:
        fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
        with os.fdopen(fd, "wb") as tmp: tmp.write(file_stream_or_path.read())
        file_stream_or_path.seek(0); need_cleanup = True

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pidx, page in enumerate(pdf.pages, start=1):
                text = _page_text(page)
                words = _words(page)

                mode = force_mode
                if force_mode == "auto":
                    # primeiro tenta os modelos conhecidos (rpt)
                    if _detect_rpt_lojas(text) or _detect_rpt_desligados(text):
                        mode = "rpt"
                    else:
                        mode = _auto_mode(words)

                if mode == "rpt":
                    if _detect_rpt_lojas(text):
                        rows = _materialize_rpt_lojas(words)
                    elif _detect_rpt_desligados(text):
                        rows = _materialize_rpt_desligados(words)
                    else:
                        rows = _build_grid_by_gaps_table(words)
                elif mode == "table":
                    rows = _build_grid_by_gaps_table(words)
                elif mode == "form":
                    rows = _materialize_form(words)
                else:
                    rows = [["(Página sem conteúdo)"]]

                if ws.max_row > 0: ws = _append_rows(wb, ws, base, [[]])  # separador
                ws = _append_rows(wb, ws, base, [[f"Página {pidx} • modo: {mode}"]])
                ws = _append_rows(wb, ws, base, rows)
    finally:
        if need_cleanup and os.path.exists(pdf_path): os.remove(pdf_path)

    bio = io.BytesIO(); wb.save(bio); bio.seek(0)
    return bio.read()
