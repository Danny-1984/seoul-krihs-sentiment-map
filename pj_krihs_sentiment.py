# pj_krihs_sentiment.py
# -*- coding: utf-8 -*-

"""
KRIHS(국토연구원) 부동산 심리지수 크롤러 + Supabase 업로드 (Incremental 수집 포함)
"""

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
from datetime import date
from typing import Dict, Tuple, Optional, Iterable, List

from playwright.sync_api import sync_playwright

import os
from dotenv import load_dotenv
from supabase import create_client, Client


# ============================================================
# 0. Supabase 헬퍼 함수들
# ============================================================

def get_supabase_client() -> Client:
    """Supabase 클라이언트 생성 (.env 기반)."""
    load_dotenv()

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")

    if not url or not key:
        raise RuntimeError(
            "Supabase URL 또는 KEY가 설정되어 있지 않습니다. "
            ".env 파일에 SUPABASE_URL / SUPABASE_SERVICE_KEY를 설정해 주세요."
        )

    return create_client(url, key)


def upsert_krihs_rows(rows: List[dict], table_name: str = "krihs_mindicons") -> None:
    """rows(list[dict])를 Supabase 테이블에 upsert."""
    if not rows:
        print("[INFO] Supabase upsert: rows가 비어 있어 전송하지 않습니다.")
        return

    try:
        client = get_supabase_client()
    except RuntimeError as e:
        print(f"[WARN] Supabase 초기화 실패: {e}")
        return

    try:
        res = client.table(table_name).upsert(rows).execute()
        inserted = len(res.data) if res.data else 0
        print(f"[Supabase] upsert 완료: {inserted} rows (table={table_name})")
    except Exception as e:
        print(f"[ERROR] Supabase upsert 중 오류 발생: {e}")


# ============================================================
# 1. 수집 대상(지표) 정의
# ============================================================

@dataclass
class Dataset:
    path: str
    default_item: int


DATASETS: Dict[str, Dataset] = {
    "market_consume":   Dataset("SystemIntro",     2),
    "house_consume":    Dataset("Mind_House",      7),
    "house_sale":       Dataset("Mind_House_Cur", 12),
    "house_rent":       Dataset("Mind_Rent_Cur",  14),
    "land_consume":     Dataset("Mind_Land",       6),
}

BASE = "https://kremap.krihs.re.kr/menu2"


def build_url(dataset_key: str, year: int, month: int, item_cd: Optional[int] = None) -> str:
    ds = DATASETS[dataset_key]
    params = {
        "area_cd": "11",
        "Gbn": "MONTH",
        "year": str(year),
        "month": f"{int(month):02d}",
    }
    if ds.path == "SystemIntro":
        params["jin"] = "Jindan"
    params["item_cd"] = str(item_cd or ds.default_item)
    return f"{BASE}/{ds.path}?{urlencode(params)}"


# ============================================================
# 2. 서울 25개 구 매핑
# ============================================================

SEOUL_GU_MAP: Dict[str, Tuple[str, str]] = {
    "종로구":   ("11110", "jongro"),
    "중구":     ("11140", "jung"),
    "용산구":   ("11170", "yongsan"),
    "성동구":   ("11200", "seongdong"),
    "광진구":   ("11215", "gwangjin"),
    "동대문구": ("11230", "dongdaemun"),
    "중랑구":   ("11260", "jungrang"),
    "성북구":   ("11290", "seongbuk"),
    "강북구":   ("11305", "gangbok"),
    "도봉구":   ("11320", "dobong"),
    "노원구":   ("11350", "nowon"),
    "은평구":   ("11380", "eunpyeung"),
    "서대문구": ("11410", "seodaemun"),
    "마포구":   ("11440", "mapo"),
    "양천구":   ("11470", "yangcheon"),
    "강서구":   ("11500", "gangseo"),
    "구로구":   ("11530", "guro"),
    "금천구":   ("11545", "geumcheon"),
    "영등포구": ("11560", "yeongdeungpo"),
    "동작구":   ("11590", "dongjak"),
    "관악구":   ("11620", "gwanak"),
    "서초구":   ("11650", "seocho"),
    "강남구":   ("11680", "gangnam"),
    "송파구":   ("11710", "songpa"),
    "강동구":   ("11740", "gangdong"),
}


# ============================================================
# 3. weather 아이콘 점수화
# ============================================================

def score_weather(alt_text: str) -> Optional[int]:
    import re

    if not alt_text:
        return None

    alt = alt_text.replace(" ", "")

    if alt.startswith("수축"):
        phase = "수축"
    elif alt.startswith("확장"):
        phase = "확장"
    elif alt.startswith("안정"):
        phase = "안정"
    else:
        phase = None

    m = re.search(r"([+-])(\d)단계", alt)
    if m:
        sign = -1 if m.group(1) == "-" else 1
        step = int(m.group(2))
    else:
        sign = 0
        step = 0

    if phase == "수축":
        return -(step + 1)
    if phase == "확장":
        return step + 1
    if phase == "안정":
        return sign * step
    return None


# ============================================================
# 4. 테이블 파싱
# ============================================================

def parse_table_icons(page) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}

    tables = page.locator("table[id*='GridView']")
    if tables.count() == 0:
        return data

    sizes = [tables.nth(i).locator("tr").count() for i in range(tables.count())]
    idx = max(range(len(sizes)), key=lambda i: sizes[i])
    table = tables.nth(idx)

    rows = table.locator("tr")
    for i in range(rows.count()):
        tds = rows.nth(i).locator("td")
        if tds.count() < 2:
            continue

        region = tds.nth(0).inner_text().strip()
        if not region or region == "지역명":
            continue

        imgs = rows.nth(i).locator("img")
        alts: List[str] = []
        for j in range(imgs.count()):
            alt = (imgs.nth(j).get_attribute("alt") or "").strip()
            if alt:
                alts.append(alt)

        weather_alt = alts[0] if len(alts) >= 1 else ""
        mom_alt = alts[-1] if len(alts) >= 2 else ""

        data[region] = {"weather": weather_alt, "mom": mom_alt}

    return data


# ============================================================
# 5. 월 반복 유틸리티
# ============================================================

def month_iter(start_ym: Tuple[int, int],
               end_ym: Optional[Tuple[int, int]] = None) -> Iterable[Tuple[int, int]]:
    y, m = start_ym
    if end_ym is None:
        t = date.today()
        end_ym = (t.year, t.month)
    ey, em = end_ym
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            y += 1
            m = 1


def next_month(year: int, month: int) -> Tuple[int, int]:
    month += 1
    if month > 12:
        year += 1
        month = 1
    return year, month


# ============================================================
# 6. 메인 수집 함수
# ============================================================

def collect(
    dataset_key: str,
    start_ym: Tuple[int, int] = (2015, 1),
    end_ym: Optional[Tuple[int, int]] = None,
    item_cd: Optional[int] = None,
    save_csv: bool = False,
    out_path: str = "output/krihs_mindicons.csv",
    save_supabase: bool = False,
    supabase_table: str = "krihs_mindicons",
) -> List[dict]:

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    rows: List[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900})

        for y, m in month_iter(start_ym, end_ym):
            url = build_url(dataset_key, y, m, item_cd=item_cd)
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1500)

                table = parse_table_icons(page)
                if not table:
                    print(f"[WARN] No data: {dataset_key} {y}-{m:02d}")

                for gu_name, v in table.items():
                    region_code, alias = SEOUL_GU_MAP.get(gu_name, ("", ""))
                    score = score_weather(v.get("weather", ""))

                    rows.append({
                        "dataset": dataset_key,
                        "item_cd": item_cd or DATASETS[dataset_key].default_item,
                        "year": y,
                        "month": m,
                        "region_name": gu_name,
                        "region_code": region_code,
                        "region_alias": alias,
                        "weather": v.get("weather", ""),
                        "weather_score": score,
                        "mom": v.get("mom", ""),
                        "url": url,
                    })

                print(f"[OK] {dataset_key} {y}-{m:02d} rows={len(table)}")
            finally:
                page.close()

        ctx.close()
        browser.close()

    if save_csv and rows:
        import csv
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"CSV saved → {Path(out_path).resolve()} (rows={len(rows)})")

    if save_supabase:
        upsert_krihs_rows(rows, table_name=supabase_table)

    return rows


# ============================================================
# 7. Supabase에서 최신 연월 조회 + Incremental 수집
# ============================================================

def get_latest_ym_from_supabase(
    dataset_key: str,
    table_name: str = "krihs_mindicons",
) -> Optional[Tuple[int, int]]:
    try:
        client = get_supabase_client()
    except RuntimeError as e:
        print(f"[WARN] Supabase 초기화 실패(최신 연월 조회): {e}")
        return None

    try:
        res = (
            client.table(table_name)
            .select("year, month")
            .eq("dataset", dataset_key)
            .order("year", desc=True)
            .order("month", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            return None

        row = res.data[0]
        return int(row["year"]), int(row["month"])
    except Exception as e:
        print(f"[ERROR] Supabase 최근 연월 조회 중 오류: {e}")
        return None


def collect_incremental(
    dataset_key: str,
    default_start_ym: Tuple[int, int] = (2015, 1),
    end_ym: Optional[Tuple[int, int]] = None,
    item_cd: Optional[int] = None,
    save_csv: bool = False,
    out_path: str = "output/krihs_mindicons_incremental.csv",
    save_supabase: bool = True,
    supabase_table: str = "krihs_mindicons",
) -> List[dict]:

    latest = get_latest_ym_from_supabase(dataset_key, table_name=supabase_table)

    if end_ym is None:
        t = date.today()
        end_ym = (t.year, t.month)

    if latest is None:
        start_ym = default_start_ym
        print(f"[INFO] [{dataset_key}] DB에 기존 데이터 없음 → {start_ym[0]}-{start_ym[1]:02d}부터 수집")
    else:
        sy, sm = next_month(*latest)
        start_ym = (sy, sm)
        print(f"[INFO] [{dataset_key}] DB 최신 데이터: {latest[0]}-{latest[1]:02d} "
              f"→ 다음 달 {sy}-{sm:02d}부터 {end_ym[0]}-{end_ym[1]:02d}까지 수집")

    if start_ym > end_ym:
        print(f"[INFO] [{dataset_key}] 이미 최신 상태입니다. 추가 수집 불필요.")
        return []

    rows = collect(
        dataset_key=dataset_key,
        start_ym=start_ym,
        end_ym=end_ym,
        item_cd=item_cd,
        save_csv=save_csv,
        out_path=out_path,
        save_supabase=save_supabase,
        supabase_table=supabase_table,
    )
    return rows


# ============================================================
# 8. 메인 실행부
# ============================================================

if __name__ == "__main__":
    today = date.today()
    end_ym = (today.year, today.month)

    # 예: 5개 지표를 모두 incremental 수집
    targets = list(DATASETS.keys())
    for key in targets:
        csv_path = f"output/{key}_incremental.csv"
        print(f"\n===== [{key}] incremental 수집 시작 =====")
        collect_incremental(
            dataset_key=key,
            default_start_ym=(2015, 1),
            end_ym=end_ym,
            item_cd=None,
            save_csv=True,
            out_path=csv_path,
            save_supabase=True,
            supabase_table="krihs_mindicons",
        )
        print(f"===== [{key}] incremental 수집 종료 =====\n")
