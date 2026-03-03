from __future__ import annotations

import os
import zipfile
import glob
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

import json
import argparse


# -----------------------------
# 1) DICTIONARIES
# -----------------------------

# For legibility, we will map the procedure type, the urgency, and
# tender result codes

PROCEDURE_CODE_MAP = {
    "1": "Abierto",
    "2": "Restringido",
    "3": "Negociado sin publicidad",
    "4": "Negociado con publicidad",
    "5": "Diálogo competitivo",
    "6": "Contrato menor",
    "7": "Derivado de acuerdo marco",
    "8": "Concurso de proyectos",
    "100": "Normas internas",
    "999": "Otros",
}

URGENCY_CODE_MAP = {"1": "Ordinaria", "2": "Urgente", "3": "Emergencia"}

TENDER_RESULT_CODE_MAP = {
    "1":  "Adjudicado Provisionalmente",
    "2":  "Adjudicado Definitivamente",
    "3":  "Desierto",
    "4":  "Desistimiento",
    "5":  "Renuncia",
    "6":  "Desierto Provisionalmente",
    "7":  "Desierto Definitivamente",
    "8":  "Adjudicado",
    "9":  "Formalizado",
    "10": "Licitador mejor valorado (requerimiento de documentación)",
}

# Column names. Mapping between names in the ATOM files and those that
# we will have in the parquet columns

COLUMN_NAME_MAP = {
    # base (fields for each entry)
    "id": "id",
    "updated": "updated",
    "title": "title",
    "summary": "summary",
    "link": "link",

    # Datos generales del expediente
    "estado": "estado",
    "expediente": "expediente",
    "objeto": "objeto",
    "valor_estimado": "valor_estimado",
    "presupuesto_sin_iva": "presupuesto_sin_iva",
    "presupuesto_con_iva": "presupuesto_con_iva",
    "duracion_dias": "duracion_dias",
    "cpv_list": "cpv_list",
    "pliego_tecnico_URI": "pliego_tecnico_URI",
    "pliego_tecnico_filename": "pliego_tecnico_filename",
    "pliego_admin_URI": "pliego_admin_URI",
    "pliego_admin_filename": "pliego_admin_filename",
    "TED id": "TED id",

    # Lugar de ejecución
    "subentidad_nacional": "subentidad_nacional",
    "codigo_subentidad_territorial": "codigo_subentidad_territorial",
    
    # Lotes
    "lotes": "lotes",

    # Proceso de licitación
    # Entidad adjudicadora
    "tipo_procedimiento": "tipo_procedimiento",
    "tramitacion": "tramitacion",
    "over_threshold": "over_threshold",
    "organo_nombre": "organo_nombre",
    "organo_id": "organo_id",
    "plazo_presentacion": "plazo_presentacion", # i.e., deadline

    # TenderResult normalizado
    "resultado": "resultado",
    "fecha_acuerdo": "fecha_acuerdo",
    "ofertas_recibidas": "ofertas_recibidas",
    "ofertas_pymes": "ofertas_pymes",
    "adjudicatario_nombre": "adjudicatario_nombre",
    "identificador": "identificador",
    "adjudicatario_pyme": "adjudicatario_pyme",
    "adjudicatario_ute": "adjudicatario_ute",
    "importe_total_sin_iva": "importe_total_sin_iva",
    "importe_total_con_iva": "importe_total_con_iva",

    # procedencia (útil para debug)
    "_source_zip": "_source_zip",
    "_source_atom": "_source_atom",
}

# De las columnas anteriores, las siguientes contienen listas
# de tuplas, y por tanto se van a serializar y deserializar a
# json para su escritura y su lectura

STRUCTURED_COLS = [
    "lotes",
    "resultado",
    "fecha_acuerdo",
    "ofertas_recibidas",
    "ofertas_pymes",
    "adjudicatario_nombre",
    "identificador",
    "adjudicatario_pyme",
    "adjudicatario_ute",
    "importe_total_sin_iva",
    "importe_total_con_iva",
]

# -----------------------------
# 2) Helpers XML
# -----------------------------

def localname(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def find_child_by_localname(elem: ET.Element, name: str) -> Optional[ET.Element]:
    for ch in list(elem):
        if localname(ch.tag) == name:
            return ch
    return None

def find_children_by_localname(elem: ET.Element, name: str) -> List[ET.Element]:
    return [ch for ch in list(elem) if localname(ch.tag) == name]

def find_first_text(elem: ET.Element, path: List[str]) -> Optional[str]:
    cur = elem
    for name in path:
        cur = find_child_by_localname(cur, name)
        if cur is None:
            return None
    txt = (cur.text or "").strip()
    return txt if txt != "" else None

def find_first_attr(elem: ET.Element, path: List[str], attr_name: str) -> Optional[str]:
    cur = elem
    for name in path:
        cur = find_child_by_localname(cur, name)
        if cur is None:
            return None
    return cur.attrib.get(attr_name)

def find_first_link_href(entry: ET.Element) -> Optional[str]:
    # <link href="..."/>
    for ch in list(entry):
        if localname(ch.tag) == "link":
            href = ch.attrib.get("href")
            if href:
                return href.strip()
    return None


# -----------------------------
# 3) Functions for data normalization
# -----------------------------

def parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # ISO 8601 con offset +01:00 -> fromisoformat lo soporta
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except Exception:
        try:
            # fallback pandas
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.to_pydatetime()
        except Exception:
            return None

def safe_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).strip().replace(",", "."))
    except Exception:
        return None

def combine_date_time(date_str: Optional[str], time_str: Optional[str]) -> Optional[pd.Timestamp]:
    if not date_str and not time_str:
        return None
    if date_str and time_str:
        return pd.to_datetime(f"{date_str} {time_str}", errors="coerce")
    if date_str:
        return pd.to_datetime(date_str, errors="coerce")
    return None

def duration_to_days(start: Optional[str], end: Optional[str], dur: Optional[str], unit: Optional[str]) -> Optional[int]:
    """
    This function is used to compute the estimated duration of the contract. This is necessary
    because the corresponding field can be filled in with initial and end dates, or alternatively
    with the duration in months, days or years ... This function outputs the expected duration
    in days
    """
    # 1) start + end -> diferencia en días
    if start and end:
        sdt = pd.to_datetime(start, errors="coerce")
        edt = pd.to_datetime(end, errors="coerce")
        if pd.notna(sdt) and pd.notna(edt):
            delta = (edt - sdt).days
            return int(delta) if delta >= 0 else None

    # 2) DurationMeasure unitCode="DAY|MON|ANN"
    if dur:
        try:
            v = float(str(dur).strip())
        except Exception:
            return None
        unit = (unit or "").strip().upper()
        # Aproximación razonable: mes=30, año=365 (si quieres exactitud calendárica lo ajustamos)
        mult = {"DAY": 1, "MON": 30, "ANN": 365}.get(unit, None)
        if mult is None:
            return None
        return int(round(v * mult))

    return None


# -----------------------------
# 4) Functions for extracting information from the ATOM
# These functions are now designed to work
# with elements from the XML tree to transform
# strings into a more convenient type for future use
# -----------------------------

def extract_cpv_list(cfs: ET.Element) -> List[str]:
    """
    Extracts all CPVs in ProcurementProject -> RequiredCommodityClassification ->
    ItemClassificationCode as a list of strings (even if only one is present)
    """
    cpvs: List[str] = []
    pp = find_child_by_localname(cfs, "ProcurementProject")
    if pp is None:
        return cpvs
    for rcc in find_children_by_localname(pp, "RequiredCommodityClassification"):
        code = find_first_text(rcc, ["ItemClassificationCode"])
        if code:
            cpvs.append(code)
    return cpvs

def extract_pliego(doc_ref_tag: str, cfs: ET.Element) -> Tuple[Optional[str], Optional[str]]:
    """
    doc_ref_tag: "LegalDocumentReference" o "TechnicalDocumentReference"
    Queremos:
      - URI: .../Attachment/ExternalReference/URI
      - filename: el ID del propio doc ref (NO el servlet de la URI; se extrae del XML)
    """
    doc = find_child_by_localname(cfs, doc_ref_tag)
    if doc is None:
        return None, None
    uri = find_first_text(doc, ["Attachment", "ExternalReference", "URI"])
    filename = find_first_text(doc, ["ID"])  # el atom ya trae el nombre "humano"
    return uri, filename

def extract_ted_id(cfs: ET.Element) -> Optional[str]:
    # En el esquema el UUID con schemeName="TED" aparece como <cbc:UUID schemeName="TED">...</cbc:UUID>
    for ch in list(cfs):
        if localname(ch.tag) == "UUID" and ch.attrib.get("schemeName") == "TED":
            txt = (ch.text or "").strip()
            return txt if txt else None
    return None

# -----------------------------
# 4.1) Functions for processing lots
# -----------------------------

def extract_lotes(cfs: ET.Element) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    Devuelve una lista [(idlote, name)]
    En caso de que no haya información de lotes devolverá una lista vacía []
    """
    lotes: List[Tuple[Optional[str], Optional[str]]] = []
    for lot in find_children_by_localname(cfs, "ProcurementProjectLot"):
        lote_id = find_first_text(lot, ["ID"])
        lote_obj = find_first_text(lot, ["ProcurementProject", "Name"])
        if lote_id or lote_obj:
            lotes.append((lote_id, lote_obj))
    return lotes

# Cuando no hay lotes devuelve directamente los valores de TenderResults. Si hay lotes,
# cada valor se devuelve en formato lista de tuplas [(idlote, valor)]
def extract_tender_results(cfs: ET.Element) -> Dict[str, Any]:
    """
    Reglas:
      - Si NO hay lotes (TenderResult único sin ProcurementProjectLotID) -> scalar
      - Si HAY lotes -> listas:
            campo = [(idlote, valor), ...]
        salvo "identificador":
            identificador = [(idlote, tipo, id), ...]
      - Si no hay lotes: identificador = (tipo, id)
    """
    trs = find_children_by_localname(cfs, "TenderResult")
    if not trs:
        return {}

    results: List[Dict[str, Any]] = []
    for tr in trs:
        id_lote = find_first_text(tr, ["AwardedTenderedProject", "ProcurementProjectLotID"])
        id_tipo = find_first_attr(tr, ["WinningParty", "PartyIdentification", "ID"], "schemeName")
        id_val = find_first_text(tr, ["WinningParty", "PartyIdentification", "ID"])

        # importes (cast a float si existen)
        sin_iva_txt = find_first_text(tr, ["AwardedTenderedProject", "LegalMonetaryTotal", "TaxExclusiveAmount"])
        con_iva_txt = find_first_text(tr, ["AwardedTenderedProject", "LegalMonetaryTotal", "PayableAmount"])
        sin_iva = float(sin_iva_txt) if sin_iva_txt else None
        con_iva = float(con_iva_txt) if con_iva_txt else None

        res_code = find_first_text(tr, ["ResultCode"])
        resultado = TENDER_RESULT_CODE_MAP.get(res_code, res_code)
        
        results.append({
            "idlote": id_lote,
            "resultado": resultado,
            "fecha_acuerdo": find_first_text(tr, ["AwardDate"]),
            "ofertas_recibidas": find_first_text(tr, ["ReceivedTenderQuantity"]),
            "ofertas_pymes": find_first_text(tr, ["SMEsReceivedTenderQuantity"]),
            "adjudicatario_nombre": find_first_text(tr, ["WinningParty", "PartyName", "Name"]),
            "identificador": (id_tipo, id_val),
            "adjudicatario_pyme": find_first_text(tr, ["SMEAwardedIndicator"]),
            "adjudicatario_ute": find_first_text(tr, ["WinningParty", "PartyLegalEntity", "CompanyTypeCode"]),
            "importe_total_sin_iva": sin_iva,
            "importe_total_con_iva": con_iva,
        })

    fields = [k for k in results[0].keys() if k != "idlote"]
    out: Dict[str, Any] = {}

    for field in fields:
        values: List[Any] = []
        for r in results:
            if r["idlote"] is None:
                #Si no hay lotes, devolvemos una lista creamos la 
                #tupla usando "-1" como identificador del lote
                if field == "identificador":
                    tipo, val = r[field]
                    values.append(("-1", tipo, val))
                else:
                    values.append(("-1",r[field]))
            else:
                if field == "identificador":
                    tipo, val = r[field]
                    values.append((r["idlote"], tipo, val))
                else:
                    values.append((r["idlote"], r[field]))
        
        out[field] = values
        """ Deprecated. Ahora devolvemos siempre una lista
        con los valores

        # scalar si solo hay un TenderResult y sin lote
        if len(results) == 1 and results[0]["idlote"] is None:
            out[field] = values[0]
        else:
            out[field] = values
        """
        

    return out


# -----------------------------
# 4.2) Extraemos una fila con todos los elementos de un <entry>
# -----------------------------
def extract_entry_all_fields(entry: ET.Element) -> Dict[str, Any]:
    """
    EXTRAEMOS:
    - base: id, title, summary, updated, link(href)
    - Datos generales del expediente: estado, expediente, objeto, presupuestos, duracion_dias, cpv_list, pliegos, TED id
    - Otros datos del expediente: subentidad*, lotes, procedimiento/tramitación, over_threshold, órgano, plazo_presentacion, TenderResult*
    """
    # base atom
    row: Dict[str, Any] = {
        "id": find_first_text(entry, ["id"]),
        "title": find_first_text(entry, ["title"]),
        "summary": find_first_text(entry, ["summary"]),
        "updated": find_first_text(entry, ["updated"]),
        "link": find_first_link_href(entry),
    }

    cfs = find_child_by_localname(entry, "ContractFolderStatus")
    if cfs is None:
        return row

    # --- PRIMERA TANDA ---
    row["estado"] = find_first_text(cfs, ["ContractFolderStatusCode"])
    row["expediente"] = find_first_text(cfs, ["ContractFolderID"])
    row["objeto"] = find_first_text(cfs, ["ProcurementProject", "Name"])

    row["valor_estimado"] = safe_float(find_first_text(cfs, ["ProcurementProject", "BudgetAmount", "EstimatedOverallContractAmount"]))
    row["presupuesto_sin_iva"] = safe_float(find_first_text(cfs, ["ProcurementProject", "BudgetAmount", "TaxExclusiveAmount"]))
    row["presupuesto_con_iva"] = safe_float(find_first_text(cfs, ["ProcurementProject", "BudgetAmount", "TotalAmount"]))

    # duración contrato (días)
    start = find_first_text(cfs, ["ProcurementProject", "PlannedPeriod", "StartDate"])
    end = find_first_text(cfs, ["ProcurementProject", "PlannedPeriod", "EndDate"])
    dur = find_first_text(cfs, ["ProcurementProject", "PlannedPeriod", "DurationMeasure"])
    dur_unit = None
    # unitCode está en el elemento DurationMeasure
    pp = find_child_by_localname(cfs, "ProcurementProject")
    if pp is not None:
        planned = find_child_by_localname(pp, "PlannedPeriod")
        if planned is not None:
            dm = find_child_by_localname(planned, "DurationMeasure")
            if dm is not None:
                dur_unit = dm.attrib.get("unitCode")
    row["duracion_dias"] = duration_to_days(start, end, dur, dur_unit)

    # CPVs
    row["cpv_list"] = extract_cpv_list(cfs)

    # Pliegos (URI + filename en el propio XML)
    #  - pliego_tecnico: TechnicalDocumentReference
    #  - pliego_admin:   LegalDocumentReference
    admin_uri, admin_fn = extract_pliego("LegalDocumentReference", cfs)
    tec_uri, tec_fn = extract_pliego("TechnicalDocumentReference", cfs)
    row["pliego_admin_URI"] = admin_uri
    row["pliego_admin_filename"] = admin_fn
    row["pliego_tecnico_URI"] = tec_uri
    row["pliego_tecnico_filename"] = tec_fn

    # TED id
    row["TED id"] = extract_ted_id(cfs)

    # Datos geográficos
    row["subentidad_nacional"] = find_first_text(cfs, ["ProcurementProject", "RealizedLocation", "CountrySubentity"])
    row["codigo_subentidad_territorial"] = find_first_text(cfs, ["ProcurementProject", "RealizedLocation", "CountrySubentityCode"])

    # Información de lotes
    row["lotes"] = extract_lotes(cfs)

    # Información sobre la tramitación del expediente
    proc_code = find_first_text(cfs, ["TenderingProcess", "ProcedureCode"])
    urg_code = find_first_text(cfs, ["TenderingProcess", "UrgencyCode"])
    row["tipo_procedimiento"] = PROCEDURE_CODE_MAP.get(proc_code, proc_code)
    row["tramitacion"] = URGENCY_CODE_MAP.get(urg_code, urg_code)

    row["over_threshold"] = find_first_text(cfs, ["TenderingProcess", "OverThresholdIndicator"])

    # Datos de la entidad que contrata
    row["organo_nombre"] = find_first_text(cfs, ["LocatedContractingParty", "Party", "PartyName", "Name"])
    row["organo_id"] = find_first_text(cfs, ["LocatedContractingParty", "Party", "PartyIdentification", "ID"])

    # Deadline para la presentación de ofertas
    end_date = find_first_text(cfs, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndDate"])
    end_time = find_first_text(cfs, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndTime"])
    row["plazo_presentacion"] = combine_date_time(end_date, end_time)

    # TenderResult (con lógica lotes y campo identificador unificado)
    row.update(extract_tender_results(cfs))

    return row


# -----------------------------
# 5) Funciones para procesar y fusionar los ficheros ZIP/ATOM 
# * Función para procesar 1 fichero ZIP / ATOM y guardar resultados en una clase propia
# * Fusión de las salidas de procesar varios ficheros, cuando hay ids repetidos
#   conserva la información del elemento con update más reciente
# -----------------------------

@dataclass
class LatestMapResult:
    """Esta clase almacena los resultados de procesar un fichero zip o atom
    source: es el nombre del fichero
    latest_by_id: un diccionario id_licitacion: diccionario con todos los campos
                  si en el fichero aparecen ids repetidos, nos quedamos con el 
                  más reciente de acuerdo a update
    """
    source: str
    latest_by_id: Dict[str, Dict[str, Any]]

def _ingest_one_source_to_latest_map(source_path: str) -> LatestMapResult:
    """
    Function for processing one zip or ATOM file (1 worker)
    Devuelve dict id -> row más reciente por updated.
    Es decir, si id duplicados, se va a quedar únicamente con la que tenga updated más reciente
    """
    p = Path(source_path)
    latest_by_id: Dict[str, Dict[str, Any]] = {}

    def update_latest(row: Dict[str, Any], src_zip: Optional[str], src_atom: Optional[str]):
        eid = row.get("id")
        if not eid:
            return
        # añadimos procedencia
        if src_zip:
            row["_source_zip"] = src_zip
        if src_atom:
            row["_source_atom"] = src_atom

        upd = row.get("updated")
        upd_dt = parse_iso_datetime(upd)
        prev = latest_by_id.get(eid)
        if prev is None:
            # si el id de esta fila no existe, se crea una entrada para 
            # el id de esta licitación
            latest_by_id[eid] = row
            return
        prev_dt = parse_iso_datetime(prev.get("updated"))
        # Actualizamos únicamente si row tiene un update más reciente
        # que el disponible en latest_by_id (si el id ya existe)
        # si no parsea, comparamos strings como fallback
        if prev_dt is None or upd_dt is None:
            if (upd or "") > (prev.get("updated") or ""):
                latest_by_id[eid] = row
            return
        if upd_dt > prev_dt:
            latest_by_id[eid] = row

    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p, "r") as z:
            atoms = [n for n in z.namelist() if n.lower().endswith(".atom")]
            for atom_name in atoms:
                with z.open(atom_name) as f:
                    try:
                        root = ET.parse(f).getroot()
                    except Exception:
                        continue
                for entry in list(root):
                    if localname(entry.tag) != "entry":
                        continue
                    row = extract_entry_all_fields(entry)
                    update_latest(row, src_zip=p.name, src_atom=atom_name)
        return LatestMapResult(source=p.name, latest_by_id=latest_by_id)

    elif p.suffix.lower() == ".atom":
        with p.open("rb") as f:
            root = ET.parse(f).getroot()
        for entry in list(root):
            if localname(entry.tag) != "entry":
                continue
            row = extract_entry_all_fields(entry)
            update_latest(row, src_zip=None, src_atom=p.name)
        return LatestMapResult(source=p.name, latest_by_id=latest_by_id)

    else:
        # Si no es .zip ni .atom, devolvemos un latest_by_id vacío
        return LatestMapResult(source=p.name, latest_by_id={})


def _merge_latest_maps(maps: Iterable[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    Merge de varios latest_by_id con regla: conservar la entrada más reciente por updated.
    """
    out: Dict[str, Dict[str, Any]] = {}

    for mp in maps:
        for eid, row in mp.items():
            prev = out.get(eid)
            if prev is None:
                out[eid] = row
                continue
            dt_new = parse_iso_datetime(row.get("updated"))
            dt_old = parse_iso_datetime(prev.get("updated"))
            if dt_old is None or dt_new is None:
                if (row.get("updated") or "") > (prev.get("updated") or ""):
                    out[eid] = row
                continue
            if dt_new > dt_old:
                out[eid] = row
    return out


# -----------------------------
# 6) Extractor Multicore
# Recibe una Lista de Paths, procesa cada una en un core diferente
# y fusiona los resultados
# -----------------------------

def _iter_input_paths(
    sources: Union[str, Path, Iterable[Union[str, Path]]]
) -> List[str]:
    if isinstance(sources, (str, Path)):
        return [str(Path(sources))]
    return [str(Path(x)) for x in sources]

def serialize_for_parquet(df):
    """
    Necesaria para poder guardar el parquet con pyarrow
    De lo contrario falla al guardar campos que contienen
    listas de tuplas
    """
    df = df.copy()
    for c in STRUCTURED_COLS:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: json.dumps(x, ensure_ascii=False)
                if x is not None else None
            )
    return df

def deserialize_from_parquet(df):
    df = df.copy()
    for c in STRUCTURED_COLS:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: json.loads(x)
                if isinstance(x, str) else None
            )
    return df

def build_latest_entries_multicore(
    sources: Union[str, Path, Iterable[Union[str, Path]]],
    parquet_path: Optional[Union[str, Path]] = None,
    *,
    max_workers: int = 12,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    - Lee zip(s) o atom(s) y conserva la entrada más reciente por <entry>/<id> según <updated>.
    - Si parquet_path:
        - si existe, lo lee (fastparquet) y consolida con los nuevos datos (misma regla latest por updated)
        - escribe el parquet resultante (fastparquet) en parquet_path
    - Devuelve el DataFrame final
    """
    paths = _iter_input_paths(sources)

    # 1) ingest multicore: un future por zip/atom
    latest_maps: List[Dict[str, Dict[str, Any]]] = []
    pbar = tqdm(total=len(paths), desc="Sources", disable=not show_progress)

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_ingest_one_source_to_latest_map, p): p for p in paths}
        for fut in as_completed(futs):
            res = fut.result()
            latest_maps.append(res.latest_by_id)
            pbar.update(1)
    pbar.close()

    # 2) merge de resultados
    latest_by_id = _merge_latest_maps(latest_maps)
    df = pd.DataFrame(list(latest_by_id.values()))

    # 3) normalizaciones de tipos y renombrado final
    # (si falta alguna col porque no existe en algunos entries, no pasa nada)
    numeric_cols = [
        "valor_estimado",
        "presupuesto_sin_iva",
        "presupuesto_con_iva",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "plazo_presentacion" in df.columns:
        df["plazo_presentacion"] = pd.to_datetime(df["plazo_presentacion"], errors="coerce")

    if "cpv_list" in df.columns:
        def _as_list(x):
            return x if isinstance(x, list) else ([] if pd.isna(x) else [x])
        df["cpv_list"] = df["cpv_list"].apply(_as_list)

    # renombre centralizado
    df = df.rename(columns={k: v for k, v in COLUMN_NAME_MAP.items() if k in df.columns})

    # 4) si hay parquet previo: consolidar
    if parquet_path is not None:
        parquet_path = Path(parquet_path)
        if parquet_path.exists():
            old = pd.read_parquet(parquet_path, engine="pyarrow")
            old = deserialize_from_parquet(old)
            # concat y dedupe por updated
            df = pd.concat([old, df], ignore_index=True)

        # asegurar que updated exista
        if "updated" in df.columns:
            # orden por updated (string ISO funciona ok la mayoría de casos; si prefieres, lo pasamos a datetime)
            df = df.sort_values("updated", kind="mergesort")

        # dedupe por id manteniendo el último
        df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

        # escribir parquet
        df = serialize_for_parquet(df)
        df.to_parquet(parquet_path, engine="pyarrow", index=False)

    return df


# -----------------------------
# 7) main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Ingesta global PLACE y guardado en parquet"
    )

    parser.add_argument(
        "--input_glob",
        type=str,
        required=True,
        help="Expresión glob para localizar los ZIP (ej: /data/PLACE/*.zip)",
    )

    parser.add_argument(
        "--output_parquet",
        type=str,
        required=True,
        help="Ruta del parquet de salida",
    )

    parser.add_argument(
        "--n_workers",
        type=int,
        default=4,
        help="Número de workers para procesamiento multicore",
    )

    parser.add_argument(
        "--show_progress",
        action="store_true",
        help="Mostrar barra de progreso",
    )

    args = parser.parse_args()

    # Resolver glob
    zip_paths = sorted(Path(p) for p in glob.glob(args.input_glob))
    
    if not zip_paths:
        raise ValueError(f"No se encontraron ZIPs con el patrón: {args.input_glob}")

    print(f"ZIPs encontrados: {len(zip_paths)}")
    print(f"Workers: {args.n_workers}")
    print(f"Salida: {args.output_parquet}")

    df_latest = build_latest_entries_multicore(
        zip_paths,
        parquet_path=args.output_parquet,
        max_workers=args.n_workers,
        show_progress=args.show_progress,
    )

    print("\nIngesta finalizada.")
    print(f"Número de licitaciones en el parquet: {len(df_latest)}")
    print("\nPrimeras filas:")
    print(df_latest.head())


if __name__ == "__main__":
    main()
