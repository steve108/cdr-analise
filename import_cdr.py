#!/usr/bin/env python3
"""
CDR ZIP Importer
================
Lê arquivos .zip de CDR_INPUT_DIR, extrai os CSVs internos e carrega os dados
no MySQL em lotes de BATCH_SIZE linhas.

Recursos de segurança:
  - SHA-256 do ZIP impede reimportar o mesmo arquivo.
  - raw_hash (SHA-256 do conteúdo da linha) impede duplicidade de registros.
  - last_line_imported permite retomar de onde parou se o processo for interrompido.

Uso:
    python import_cdr.py
"""

import csv
import hashlib
import json
import logging
import os
import re
import shutil
import traceback
import zipfile
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

# ─── Configuração ─────────────────────────────────────────────────────────────

load_dotenv()

DB_CONFIG: dict = {
    "host":     os.environ["MYSQL_HOST"],
    "port":     int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.environ["MYSQL_DATABASE"],
    "user":     os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
}

INPUT_DIR     = Path(os.environ["CDR_INPUT_DIR"])
PROCESSED_DIR = Path(os.environ["CDR_PROCESSED_DIR"])
ERROR_DIR     = Path(os.environ["CDR_ERROR_DIR"])
TEMP_DIR      = Path(os.environ["CDR_TEMP_DIR"])

BATCH_SIZE = 1000

# ─── Padrões de nome ──────────────────────────────────────────────────────────

# ex.: cdr_20260328_1213_hello.zip
_ZIP_RE = re.compile(r"^cdr_(\d{8})_(\d{4})_(.+)\.zip$", re.IGNORECASE)

# ex.: CDR_DATA_MO_hello_20260328_1213.csv
_CSV_RE = re.compile(r"^CDR_(DATA|SMS|VOICE)_(MO|MT)_", re.IGNORECASE)

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Utilitários gerais ────────────────────────────────────────────────────────


def ensure_dirs() -> None:
    """Cria as pastas de trabalho caso não existam."""
    for d in (INPUT_DIR, PROCESSED_DIR, ERROR_DIR, TEMP_DIR):
        d.mkdir(parents=True, exist_ok=True)


def get_connection() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(**DB_CONFIG)


def sha256_file(path: Path) -> str:
    """Calcula o SHA-256 de um arquivo em blocos (suporta arquivos grandes)."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_row(row: dict) -> str:
    """Calcula o SHA-256 do conteúdo de uma linha CSV (serializado em JSON canônico)."""
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def last_exc_str() -> str:
    return traceback.format_exc()[-2000:]


def move_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))


# ─── Helpers de cdr_import_batch ──────────────────────────────────────────────


def find_or_create_batch(
    conn,
    zip_path: Path,
    zip_hash: str,
    brand: str,
    ref_date: str,
    ref_hour: str,
) -> dict:
    """
    Retorna o registro existente em cdr_import_batch pelo hash do ZIP,
    ou cria um novo com status 'pending'.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM cdr_import_batch WHERE zip_hash = %s", (zip_hash,))
    row = cur.fetchone()
    cur.close()
    if row:
        return row

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cdr_import_batch
            (zip_filename, zip_path, zip_hash, brand, reference_date,
             reference_hour, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW())
        """,
        (zip_path.name, str(zip_path), zip_hash, brand, ref_date, ref_hour),
    )
    conn.commit()
    batch_id = cur.lastrowid
    cur.close()

    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM cdr_import_batch WHERE batch_id = %s", (batch_id,))
    row = cur.fetchone()
    cur.close()
    return row


def _update_batch(conn, batch_id: int, fields: dict) -> None:
    """Atualiza colunas específicas de cdr_import_batch."""
    set_clause = ", ".join(f"{col} = %s" for col in fields)
    params = list(fields.values()) + [batch_id]
    cur = conn.cursor()
    cur.execute(
        f"UPDATE cdr_import_batch SET {set_clause} WHERE batch_id = %s",
        params,
    )
    conn.commit()
    cur.close()


def set_batch_processing(conn, batch_id: int) -> None:
    _update_batch(conn, batch_id, {"status": "processing", "started_at": None})
    # MySQL aceita NOW() somente via SQL; usamos uma segunda chamada para a data
    cur = conn.cursor()
    cur.execute(
        "UPDATE cdr_import_batch SET started_at = NOW() WHERE batch_id = %s",
        (batch_id,),
    )
    conn.commit()
    cur.close()


def set_batch_done(
    conn, batch_id: int, total_files: int, total_rows: int, imported_rows: int
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cdr_import_batch
           SET status = 'done', finished_at = NOW(),
               total_files = %s, total_rows = %s, imported_rows = %s
         WHERE batch_id = %s
        """,
        (total_files, total_rows, imported_rows, batch_id),
    )
    conn.commit()
    cur.close()


def set_batch_error(conn, batch_id: int, error_message: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cdr_import_batch
           SET status = 'error', finished_at = NOW(), error_message = %s
         WHERE batch_id = %s
        """,
        (error_message[:2000], batch_id),
    )
    conn.commit()
    cur.close()


# ─── Helpers de cdr_import_file ───────────────────────────────────────────────


def find_or_create_import_file(
    conn, batch_id: int, csv_filename: str, service_type: str, direction: str
) -> dict:
    """
    Retorna o registro existente em cdr_import_file (batch_id + csv_filename),
    ou cria um novo com status 'pending'.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM cdr_import_file WHERE batch_id = %s AND csv_filename = %s",
        (batch_id, csv_filename),
    )
    row = cur.fetchone()
    cur.close()
    if row:
        return row

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cdr_import_file
            (batch_id, csv_filename, service_type, direction, status, created_at)
        VALUES (%s, %s, %s, %s, 'pending', NOW())
        """,
        (batch_id, csv_filename, service_type, direction),
    )
    conn.commit()
    file_id = cur.lastrowid
    cur.close()

    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM cdr_import_file WHERE import_file_id = %s", (file_id,)
    )
    row = cur.fetchone()
    cur.close()
    return row


def set_file_processing(conn, file_id: int) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE cdr_import_file SET status = 'processing', started_at = NOW() WHERE import_file_id = %s",
        (file_id,),
    )
    conn.commit()
    cur.close()


def set_file_done(conn, file_id: int, total_rows: int, imported_rows: int) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cdr_import_file
           SET status = 'done', finished_at = NOW(),
               total_rows = %s, imported_rows = %s
         WHERE import_file_id = %s
        """,
        (total_rows, imported_rows, file_id),
    )
    conn.commit()
    cur.close()


def set_file_error(conn, file_id: int, error_message: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cdr_import_file
           SET status = 'error', finished_at = NOW(), error_message = %s
         WHERE import_file_id = %s
        """,
        (error_message[:2000], file_id),
    )
    conn.commit()
    cur.close()


def update_file_progress(conn, file_id: int, last_line: int, imported_rows: int) -> None:
    """Persiste o progresso após cada lote — permite retomada em caso de falha."""
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cdr_import_file
           SET last_line_imported = %s, imported_rows = %s
         WHERE import_file_id = %s
        """,
        (last_line, imported_rows, file_id),
    )
    conn.commit()
    cur.close()


# ─── Mapeamento de colunas CSV → DB ───────────────────────────────────────────

# Colunas de cdr_raw que vêm do CSV (exclui colunas de controle).
_CDR_COLUMNS = [
    "id_cdr", "id_brand", "brand_id",
    "id_subscription", "subscription_id", "subscription_type",
    "msisdn", "imsi", "originator", "originator_hidden", "destination",
    "day", "start_date", "parse_day", "parse_date",
    "traffic_units", "traffic_units_rated_session", "amount_charged",
    "concept", "discount", "package_id", "package_instance_id",
    "rating_pack_ref", "price_plan_ref", "price_ref", "subscription_balance",
    "rating_pack_traffic_limit_status", "rating_pack_amount_limit_status",
    "traffic_units_sub_session", "traffic_units_rated_sub_session",
    "amount_charged_sub_session", "balance_sub_session",
    "subscription_location", "location", "gprs_station_id",
]

# Forma normalizada de cada coluna DB → nome DB original
_NORM_TO_DB: dict[str, str] = {
    re.sub(r"[^a-z0-9]+", "_", c.lower()): c for c in _CDR_COLUMNS
}


def _normalize(name: str) -> str:
    """Remove espaços, converte para minúsculas e substitui chars especiais por '_'."""
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def build_getter(fieldnames: list[str]) -> dict[str, str | None]:
    """
    Constrói um mapeamento  db_column → csv_header  para um CSV específico.
    Se o CSV não tiver determinada coluna, o valor é None.

    Normaliza os nomes dos headers do CSV para tolerar variações de maiúsculas
    e caracteres separadores (ex.: "MSISDN", "msisdn", "Msisdn" → "msisdn").
    """
    norm_csv: dict[str, str] = {_normalize(h): h for h in fieldnames}
    return {
        db_col: norm_csv.get(norm_key)
        for norm_key, db_col in _NORM_TO_DB.items()
    }


# ─── Construção do tuple de INSERT ────────────────────────────────────────────

_INSERT_SQL = """
INSERT IGNORE INTO cdr_raw (
    batch_id, import_file_id, service_type, direction,
    id_cdr, id_brand, brand_id,
    id_subscription, subscription_id, subscription_type,
    msisdn, imsi, originator, originator_hidden, destination,
    day, start_date, parse_day, parse_date,
    traffic_units, traffic_units_rated_session, amount_charged,
    concept, discount, package_id, package_instance_id,
    rating_pack_ref, price_plan_ref, price_ref, subscription_balance,
    rating_pack_traffic_limit_status, rating_pack_amount_limit_status,
    traffic_units_sub_session, traffic_units_rated_sub_session,
    amount_charged_sub_session, balance_sub_session,
    subscription_location, location, gprs_station_id,
    raw_line_number, raw_hash, raw_data_json, created_at
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s,
    %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s, NOW()
)
"""


def _v(row: dict, header: str | None) -> str | None:
    """Retorna o valor de uma célula CSV ou None se ausente/vazio."""
    if header is None:
        return None
    val = row.get(header)
    return val if val not in (None, "") else None


def build_row_tuple(
    batch_id: int,
    file_id: int,
    service_type: str,
    direction: str,
    getter: dict[str, str | None],
    csv_row: dict,
    line_number: int,
) -> tuple:
    """Constrói o tuple de parâmetros para o INSERT em cdr_raw."""
    g = getter
    r = csv_row
    return (
        batch_id, file_id, service_type, direction,
        _v(r, g.get("id_cdr")),         _v(r, g.get("id_brand")),
        _v(r, g.get("brand_id")),
        _v(r, g.get("id_subscription")), _v(r, g.get("subscription_id")),
        _v(r, g.get("subscription_type")),
        _v(r, g.get("msisdn")),          _v(r, g.get("imsi")),
        _v(r, g.get("originator")),      _v(r, g.get("originator_hidden")),
        _v(r, g.get("destination")),
        _v(r, g.get("day")),             _v(r, g.get("start_date")),
        _v(r, g.get("parse_day")),       _v(r, g.get("parse_date")),
        _v(r, g.get("traffic_units")),   _v(r, g.get("traffic_units_rated_session")),
        _v(r, g.get("amount_charged")),
        _v(r, g.get("concept")),         _v(r, g.get("discount")),
        _v(r, g.get("package_id")),      _v(r, g.get("package_instance_id")),
        _v(r, g.get("rating_pack_ref")), _v(r, g.get("price_plan_ref")),
        _v(r, g.get("price_ref")),       _v(r, g.get("subscription_balance")),
        _v(r, g.get("rating_pack_traffic_limit_status")),
        _v(r, g.get("rating_pack_amount_limit_status")),
        _v(r, g.get("traffic_units_sub_session")),
        _v(r, g.get("traffic_units_rated_sub_session")),
        _v(r, g.get("amount_charged_sub_session")),
        _v(r, g.get("balance_sub_session")),
        _v(r, g.get("subscription_location")),
        _v(r, g.get("location")),        _v(r, g.get("gprs_station_id")),
        line_number,
        sha256_row(csv_row),
        json.dumps(csv_row, ensure_ascii=False),
    )


# ─── Importação de um CSV ─────────────────────────────────────────────────────


def import_csv(
    conn,
    csv_path: Path,
    batch_id: int,
    file_rec: dict,
    service_type: str,
    direction: str,
) -> tuple[int, int]:
    """
    Importa um único CSV para cdr_raw.

    Retoma de onde parou usando last_line_imported.
    Retorna (total_rows, imported_rows).
    """
    file_id = file_rec["import_file_id"]

    if file_rec["status"] == "done":
        log.info("  [skip] CSV já concluído: %s", csv_path.name)
        return int(file_rec.get("total_rows") or 0), int(file_rec.get("imported_rows") or 0)

    resume_from   = int(file_rec.get("last_line_imported") or 0)
    imported_rows = int(file_rec.get("imported_rows") or 0)

    set_file_processing(conn, file_id)
    if resume_from > 0:
        log.info("  Retomando CSV %s da linha %d", csv_path.name, resume_from)
    else:
        log.info("  Importando CSV: %s", csv_path.name)

    total_rows = 0
    batch: list[tuple] = []
    cur = conn.cursor()

    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            getter = build_getter(reader.fieldnames or [])

            for line_num, raw_row in enumerate(reader, start=1):
                total_rows += 1

                # Pula linhas já confirmadas no banco
                if line_num <= resume_from:
                    continue

                row_dict = dict(raw_row)
                batch.append(
                    build_row_tuple(
                        batch_id, file_id, service_type, direction,
                        getter, row_dict, line_num,
                    )
                )

                if len(batch) >= BATCH_SIZE:
                    cur.executemany(_INSERT_SQL, batch)
                    conn.commit()
                    imported_rows += len(batch)
                    update_file_progress(conn, file_id, line_num, imported_rows)
                    log.info(
                        "    lote confirmado até linha %d (%d acumuladas)",
                        line_num, imported_rows,
                    )
                    batch.clear()

            # Flush do lote final
            if batch:
                cur.executemany(_INSERT_SQL, batch)
                conn.commit()
                imported_rows += len(batch)
                update_file_progress(conn, file_id, total_rows, imported_rows)

        set_file_done(conn, file_id, total_rows, imported_rows)
        log.info(
            "  Concluído: %s — %d/%d linhas importadas",
            csv_path.name, imported_rows, total_rows,
        )
        return total_rows, imported_rows

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        set_file_error(conn, file_id, last_exc_str())
        raise

    finally:
        cur.close()


# ─── Processamento de um ZIP ──────────────────────────────────────────────────


def parse_zip_name(name: str) -> tuple[str, str, str] | tuple[None, None, None]:
    """
    Extrai (ref_date, ref_hour, brand) do nome do ZIP.
    Ex.: cdr_20260328_1213_hello.zip → ('2026-03-28', '1213', 'hello')
    """
    m = _ZIP_RE.match(name)
    if not m:
        return None, None, None
    date_str, hhmm, brand = m.group(1), m.group(2), m.group(3)
    ref_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return ref_date, hhmm, brand


def parse_csv_name(name: str) -> tuple[str, str] | tuple[None, None]:
    """
    Extrai (service_type, direction) do nome do CSV.
    Ex.: CDR_DATA_MO_hello_20260328_1213.csv → ('DATA', 'MO')
    """
    m = _CSV_RE.match(name)
    if not m:
        return None, None
    return m.group(1).upper(), m.group(2).upper()


def process_zip(conn, zip_path: Path) -> None:
    """Processa um arquivo ZIP completo: extrai, importa todos os CSVs e move ao final."""
    log.info("Processando ZIP: %s", zip_path.name)

    ref_date, ref_hour, brand = parse_zip_name(zip_path.name)
    if ref_date is None:
        log.warning("Nome de ZIP não reconhecido, pulando: %s", zip_path.name)
        return

    zip_hash = sha256_file(zip_path)
    batch = find_or_create_batch(conn, zip_path, zip_hash, brand, ref_date, ref_hour)
    batch_id: int = batch["batch_id"]

    if batch["status"] == "done":
        log.info("ZIP já importado (hash já existe como 'done'), pulando: %s", zip_path.name)
        return

    # Pasta temporária isolada por ZIP (evita colisão entre execuções paralelas)
    zip_work = TEMP_DIR / zip_path.stem
    zip_work.mkdir(parents=True, exist_ok=True)

    try:
        set_batch_processing(conn, batch_id)

        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_entries = sorted(e for e in zf.namelist() if e.lower().endswith(".csv"))
            zf.extractall(zip_work)

        total_files = 0
        total_rows  = 0
        imported    = 0

        for entry in csv_entries:
            csv_filename = Path(entry).name
            service_type, direction = parse_csv_name(csv_filename)

            if service_type is None:
                log.warning("  CSV ignorado (nome não reconhecido): %s", csv_filename)
                continue

            total_files += 1
            file_rec = find_or_create_import_file(
                conn, batch_id, csv_filename, service_type, direction
            )

            t, i = import_csv(
                conn, zip_work / entry, batch_id, file_rec, service_type, direction
            )
            total_rows += t
            imported   += i

        set_batch_done(conn, batch_id, total_files, total_rows, imported)
        move_file(zip_path, PROCESSED_DIR / zip_path.name)
        log.info("ZIP concluído → movido para: %s", PROCESSED_DIR / zip_path.name)

    except Exception as exc:
        set_batch_error(conn, batch_id, str(exc)[:2000])
        move_file(zip_path, ERROR_DIR / zip_path.name)
        log.error(
            "Erro no ZIP %s → movido para error. Causa: %s", zip_path.name, exc
        )

    finally:
        shutil.rmtree(zip_work, ignore_errors=True)


# ─── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    ensure_dirs()
    conn = get_connection()
    try:
        zip_files = sorted(INPUT_DIR.glob("*.zip"))
        if not zip_files:
            log.info("Nenhum arquivo ZIP encontrado em: %s", INPUT_DIR)
            return

        log.info("Encontrados %d ZIP(s) para processar.", len(zip_files))
        for zip_path in zip_files:
            process_zip(conn, zip_path)
    finally:
        conn.close()

    log.info("Importação concluída.")


if __name__ == "__main__":
    main()
