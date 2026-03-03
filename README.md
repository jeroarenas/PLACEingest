# PLACEingest

# PLACE ATOM → Parquet Ingestor

Script para convertir los ficheros ATOM publicados en PLACE (Plataforma de Contratación del Sector Público) a formato Parquet, con soporte multicore y cargas incrementales.

---

## 📦 Uso general

```bash
python atom2parquet.py \
  --input_glob "<patrón_glob_zip>" \
  --output_parquet "<ruta_parquet_salida>" \
  --n_workers 12 \
  --show_progress
```

### Parámetros

| Parámetro | Descripción |
|-----------|------------|
| `--input_glob` | Expresión glob para localizar los ZIP |
| `--output_parquet` | Ruta del parquet de salida |
| `--n_workers` | Número de procesos paralelos |
| `--show_progress` | Muestra barra de progreso |

---

## 🔁 Cargas incrementales (ejemplo 2025–2026)

Ejemplo para construir datasets acumulados por tipo.

---

### 🔹 Insiders

```bash
python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2025/licitacionesPerfilesContratanteCompleto3_2025*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/insiders_2526.parquet" --n_workers 12 --show_progress

python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2026/licitacionesPerfilesContratanteCompleto3_2026*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/insiders_2526.parquet" --n_workers 12 --show_progress
```

> ⚠️ Nota: falta enero de 2025 en la colección original.

---

### 🔹 Outsiders (Plataformas agregadas sin menores)

```bash
python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2025/PlataformasAgregadasSinMenores_2025*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/outsiders_2526.parquet" --n_workers 12 --show_progress

python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2026/PlataformasAgregadasSinMenores_2026*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/outsiders_2526.parquet" --n_workers 12 --show_progress
```

---

### 🔹 Contratos menores

```bash
python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2025/contratosMenoresPerfilesContratantes_2025*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/minors_2526.parquet" --n_workers 12 --show_progress

python atom2parquet.py --input_glob "/home/jarenas/Datasets/PLACE/2026/contratosMenoresPerfilesContratantes_2026*.zip" --output_parquet "/home/jarenas/Datasets/PLACE/minors_2526.parquet" --n_workers 12 --show_progress
```

---

## 🧩 Columnas estructuradas (listas serializadas)

Algunas columnas contienen estructuras como:

```python
[(idlote, valor)]
[(idlote, tipo, identificador)]
```

Estas estructuras no son compatibles directamente con `pyarrow`, por lo que se serializan a JSON antes de guardarse en Parquet.

Columnas afectadas:

```python
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
```

---

## 🔄 Deserialización tras lectura

Si necesitas recuperar las estructuras originales:

```python
import json
import pandas as pd

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

def deserialize_from_parquet(df):
    df = df.copy()
    for c in STRUCTURED_COLS:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: json.loads(x) if isinstance(x, str) else None
            )
    return df

parquet_path = "/ruta/al/parquet.parquet"

df = pd.read_parquet(parquet_path, engine="pyarrow")
df = deserialize_from_parquet(df)
```
