import os, io, re, zipfile, unicodedata, tempfile
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
from fpdf import FPDF

# ----------------- Texto / Unicode helpers -----------------
SMART_MAP = str.maketrans({
    "‘": "'", "’": "'", "‚": ",",
    "“": '"', "”": '"', "„": '"',
    "–": "-", "—": "-", "…": "...",
    "\u00A0": " ",  # nbsp
})
def to_pdf_text(s: str, ensure_latin1: bool = False) -> str:
    if s is None:
        return ""
    t = unicodedata.normalize("NFKC", str(s)).translate(SMART_MAP)
    if ensure_latin1:
        t = t.encode("latin-1", "ignore").decode("latin-1")
    return t

def safe_name(s: str, maxlen: int = 80) -> str:
    if s is None: s = ""
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:maxlen] if s else "sin_nombre"

# ----------------- Mapping de columnas -----------------
ALIASES: Dict[str, List[str]] = {
    "DNI": ["DNI", "Documento", "Doc", "NIF", "DNI.", "DNI.1"],
    "Telf Fijo": ["Telf Fijo", "Telefono", "Teléfono", "Telf", "Tel Fijo", "Telefono Fijo"],
    "Fecha Estado": ["Fecha Estado", "Fecha Baja", "Fecha de Estado", "Fec Estado", "Fecha Estado."],
    "Dirección": ["Dirección", "Direccion", "Dirección.", "Direcci\u00f3n"],
    "Tracking": ["Tracking", "Tracking.1", "Nro Tracking", "Guía", "Guia", "Numero Guia"],
    "Brand": ["Brand", "Marca", "MARCA"],
}
FALLBACK_IDX = {"DNI":2, "Telf Fijo":3, "Fecha Estado":4, "Dirección":5, "Tracking":6, "Brand":15}
REQUIRED_MIN = {"Brand", "Tracking"}

def prefer_non_empty(cols: List[pd.Series]) -> pd.Series:
    if not cols: return pd.Series(dtype=object)
    out = cols[0].copy()
    for s in cols[1:]:
        mask = out.isna() | (out.astype(str).str.strip() == "")
        out.loc[mask] = s.loc[mask]
    return out.astype(str)

def to_date_str(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return dt.dt.strftime("%Y-%m-%d").fillna("")

def pick_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    warnings: List[str] = []
    df = df.copy().dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]

    out: Dict[str, pd.Series] = {}
    missing_required = set()

    for target, alias_list in ALIASES.items():
        alias_lowers = {a.strip().lower() for a in alias_list}
        candidates = [c for c in df.columns if c.strip().lower() in alias_lowers]
        if candidates:
            out[target] = prefer_non_empty([df[c] for c in candidates]).fillna("").astype(str).str.strip()
        else:
            idx = FALLBACK_IDX.get(target)
            if idx is not None and idx < df.shape[1]:
                out[target] = df.iloc[:, idx].fillna("").astype(str).str.strip()
                warnings.append(f"'{target}' no encontrado por nombre; usado índice {idx}.")
            else:
                out[target] = pd.Series([""] * len(df), dtype=str)
                if target in REQUIRED_MIN:
                    missing_required.add(target)
                else:
                    warnings.append(f"Columna opcional '{target}' ausente; se rellenará vacía.")

    df_out = pd.DataFrame({
        "DNI": out["DNI"],
        "Telf Fijo": out["Telf Fijo"],
        "Dirección": out["Dirección"],
        "Tracking": out["Tracking"],
        "Fecha Estado": to_date_str(out["Fecha Estado"]),
        "Brand": out["Brand"],
    })

    mask_min = (df_out["Brand"].str.strip() != "") & (df_out["Tracking"].str.strip() != "")
    df_out = df_out[mask_min].copy()
    if df_out.empty:
        warnings.append("Tras filtrar por Brand/Marca y Tracking, no hay filas válidas.")
    if missing_required:
        warnings.append("Faltan columnas mínimas en cabeceras: " + ", ".join(sorted(missing_required)))
    return df_out, warnings

# ----------------- PDF helpers -----------------
class PDF(FPDF):
    def header(self): pass
    def footer(self): pass

def add_unicode_font_or_fallback(pdf: FPDF, size=8, bold=False):
    font_name = "DejaVu"
    base = os.path.dirname(os.path.abspath(__file__))
    regular = os.path.join(base, "DejaVuSans.ttf")
    bold_ttf = os.path.join(base, "DejaVuSans-Bold.ttf")
    if os.path.exists(regular) and os.path.exists(bold_ttf):
        try:
            if bold:
                if not getattr(pdf, "_dejavu_bold_loaded", False):
                    pdf.add_font(font_name, "B", bold_ttf, uni=True)
                    pdf._dejavu_bold_loaded = True
                pdf.set_font(font_name, "B", size)
            else:
                if not getattr(pdf, "_dejavu_loaded", False):
                    pdf.add_font(font_name, "", regular, uni=True)
                    pdf._dejavu_loaded = True
                pdf.set_font(font_name, "", size)
            pdf._unicode_ok = True
            return
        except Exception:
            pass
    pdf.set_font("Arial", "B" if bold else "", size)
    pdf._unicode_ok = False

def get_line_count(text: str, col_width: float, pdf_obj: FPDF) -> int:
    if not text: return 1
    words = str(text).split()
    lines, current = 1, ""
    for w in words:
        candidate = (f"{current} {w}").strip()
        if pdf_obj.get_string_width(candidate) <= col_width - 1:
            current = candidate
        else:
            lines += 1
            current = w
    return max(1, lines)

# ----------------- Núcleo: Excel -> ZIP bytes -----------------
def excel_to_zip_bytes(xlsx_path: str) -> Tuple[bytes, List[str], List[str]]:
    xls = pd.ExcelFile(xlsx_path)
    hoja = xls.sheet_names[0]

    df0 = pd.read_excel(xls, sheet_name=hoja, header=0, dtype=str)
    looks_like_data = df0.columns.to_series().astype(str).str.contains(
        r"\d{2}/\d{2}|\d{4}-\d{2}-\d{2}", regex=True
    ).any()
    if looks_like_data:
        df_raw = pd.read_excel(xls, sheet_name=hoja, header=None, dtype=str)
        header_row = 0
        for i in range(min(10, len(df_raw))):
            row = df_raw.iloc[i].astype(str).fillna("")
            if (row.str.strip() != "").sum() >= 3:
                header_row = i
                break
        df = pd.read_excel(xls, sheet_name=hoja, header=header_row, dtype=str)
    else:
        df = df0

    df_norm, warns = pick_columns(df)
    if df_norm.empty:
        raise ValueError("No hay filas válidas. Revisa que exista al menos Brand/Marca y Tracking con datos.")

    headers = ["DNI", "Telf Fijo", "Dirección", "Tracking", "Fecha Baja"]
    widths  = [30,    25,           70,          50,        25]

    pdf_names: List[str] = []
    zip_buffer = io.BytesIO()
    with tempfile.TemporaryDirectory(prefix="pdfs_") as tmpdir:
        for brand, grupo in df_norm.groupby("Brand"):
            pdf = PDF(orientation="P", unit="mm", format="A4")
            pdf.alias_nb_pages()
            pdf.add_page()

            # Ajuste y centrado
            content_w = pdf.w - pdf.l_margin - pdf.r_margin
            scale = min(1.0, (content_w / sum(widths)))
            w_scaled = [round(w * scale, 2) for w in widths]
            table_w = sum(w_scaled)
            x_table = pdf.l_margin + (content_w - table_w) / 2.0

            # Título
            add_unicode_font_or_fallback(pdf, size=12, bold=True)
            titulo = to_pdf_text(f"Clientes - BRAND {str(brand)}", ensure_latin1=not getattr(pdf, "_unicode_ok", False))
            pdf.cell(0, 10, txt=titulo, ln=True, align="C")
            pdf.ln(5)

            # Cabecera
            add_unicode_font_or_fallback(pdf, size=8, bold=True)
            pdf.set_x(x_table)
            for i, h in enumerate(headers):
                pdf.cell(w_scaled[i], 8, h, border=1, align="C")
            pdf.ln()
            add_unicode_font_or_fallback(pdf, size=8, bold=False)

            page_bottom = pdf.h - pdf.b_margin

            # Filas
            for _, row in grupo.iterrows():
                dni   = to_pdf_text(row["DNI"],          ensure_latin1=not getattr(pdf, "_unicode_ok", False))
                telf  = to_pdf_text(row["Telf Fijo"],    ensure_latin1=not getattr(pdf, "_unicode_ok", False))
                direc = to_pdf_text(row["Dirección"],    ensure_latin1=not getattr(pdf, "_unicode_ok", False))
                track = to_pdf_text(row["Tracking"],     ensure_latin1=not getattr(pdf, "_unicode_ok", False))
                fecha = to_pdf_text(row["Fecha Estado"], ensure_latin1=not getattr(pdf, "_unicode_ok", False))

                lines = max(get_line_count(direc, w_scaled[2], pdf), 1)
                rh = 5 * lines

                if pdf.get_y() + rh > page_bottom:
                    pdf.add_page()
                    content_w = pdf.w - pdf.l_margin - pdf.r_margin
                    x_table = pdf.l_margin + (content_w - table_w) / 2.0
                    add_unicode_font_or_fallback(pdf, size=8, bold=True)
                    pdf.set_x(x_table)
                    for i, h in enumerate(headers):
                        pdf.cell(w_scaled[i], 8, h, border=1, align="C")
                    pdf.ln()
                    add_unicode_font_or_fallback(pdf, size=8, bold=False)

                pdf.set_x(x_table)
                y0 = pdf.get_y()
                x0 = x_table

                pdf.multi_cell(w_scaled[0], rh, dni,   border=1, align='L')
                pdf.set_xy(x0 + w_scaled[0], y0)

                pdf.multi_cell(w_scaled[1], rh, telf,  border=1, align='L')
                pdf.set_xy(x0 + w_scaled[0] + w_scaled[1], y0)

                pdf.multi_cell(w_scaled[2], 5,  direc, border=1, align='L')
                pdf.set_xy(x0 + w_scaled[0] + w_scaled[1] + w_scaled[2], y0)

                pdf.multi_cell(w_scaled[3], rh, track, border=1, align='L')
                pdf.set_xy(x0 + w_scaled[0] + w_scaled[1] + w_scaled[2] + w_scaled[3], y0)

                pdf.multi_cell(w_scaled[4], rh, fecha, border=1, align='L')
                pdf.set_y(y0 + rh)

            brand_name = safe_name(str(brand))
            pdf_path = os.path.join(tmpdir, f"{brand_name}.pdf")
            pdf.output(pdf_path)
            pdf_names.append(os.path.basename(pdf_path))

        # Crear ZIP en memoria
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in pdf_names:
                zf.write(os.path.join(tmpdir, name), arcname=name)

    return zip_buffer.getvalue(), pdf_names, warns

# ----------------- Streamlit UI -----------------
st.set_page_config(page_title="ZIP PDFs por Brand", layout="centered")
st.title("📦 Generar ZIP de PDFs por Brand/Marca")
st.caption("Sube un Excel (.xlsx). Se generará un PDF por Brand/Marca y podrás descargar un ZIP.")

with st.expander("Instrucciones", expanded=False):
    st.markdown("""
- Acepta **columnas ocultas**, **renombradas** (DNI↔Documento, Brand↔Marca, etc.) y **duplicadas** (Tracking, Tracking.1).
- Requisitos mínimos por fila: **Brand/Marca** y **Tracking** con datos.
- Para tildes/ñ perfectas, añade **DejaVuSans.ttf** y **DejaVuSans-Bold.ttf** en esta carpeta.
""")

uploaded = st.file_uploader("Sube el Excel (.xlsx)", type=["xlsx"])
run = st.button("⚙️ Generar ZIP", use_container_width=True, disabled=(uploaded is None))

if run and uploaded is not None:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name

    try:
        with st.status("Procesando archivo...", expanded=True) as status:
            st.write("1/3: Leyendo Excel y normalizando columnas…")
            zip_bytes, pdf_names, warns = excel_to_zip_bytes(tmp_path)
            st.write(f"2/3: Generados **{len(pdf_names)}** PDFs.")
            if warns:
                st.write("3/3: Advertencias:")
                st.code("\n".join(f"• {w}" for w in warns), language="text")
            else:
                st.write("3/3: Sin advertencias.")
            status.update(label="Proceso completado ✅", state="complete")

        st.success("¡Listo! Descarga tu ZIP abajo.")
        if pdf_names:
            st.write("Archivos incluidos:")
            st.code("\n".join(pdf_names), language="text")

        st.download_button(
            "⬇️ Descargar ZIP",
            data=zip_bytes,
            file_name=f"Reportes_PDFs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            use_container_width=True
        )
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        try: os.remove(tmp_path)
        except: pass