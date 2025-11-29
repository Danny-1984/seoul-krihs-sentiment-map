# pj_krihs_streamlit_map.py
# -*- coding: utf-8 -*-

"""
Supabase에 저장된 KRIHS 부동산 심리지수(krihs_mindicons)를
서울 지도 위에 시각화하는 Streamlit 앱.

- dataset / year / month 선택
- (선택한 dataset 기준) **가장 최근 36개월만** 연/월 선택 가능
- 선택한 (year, month)를 기준으로 최근 3개월 데이터 표시
- 색상: weather_score(-4 ~ +4) 기준 9단계 그라데이션
    - 진한 파랑(–4) ~ 진한 빨강(+4)
- 원 크기: 최근 3개월 구분
    - 큰 원: 기준월
    - 중간 원: 기준월 - 1개월
    - 작은 원: 기준월 - 2개월
- 지도는 **드래그/휠 확대 불가(고정)**, 마커만 인터랙티브
"""

import os
from typing import Dict, Tuple, List

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium

from dotenv import load_dotenv
from supabase import create_client, Client
from branca.element import Element


# ============================================================
# 0. Supabase 클라이언트
# ============================================================

@st.cache_resource
def get_supabase_client() -> Client:
    """SUPABASE_URL / SUPABASE_SERVICE_KEY 기반 Supabase 클라이언트 생성"""
    load_dotenv()

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수가 없습니다. "
            ".env 파일을 확인해 주세요."
        )

    return create_client(url, key)


# ============================================================
# 1. 서울 구별 좌표 (위도, 경도)
# ============================================================

SEOUL_GU_COORDS: Dict[str, Tuple[float, float]] = {
    "강남구": (37.49928211, 127.0589209),
    "강동구": (37.54684465, 127.1475535),
    "강북구": (37.62727787, 127.027914),
    "강서구": (37.5600176, 126.8434151),
    "관악구": (37.47746074, 126.9389221),
    "광진구": (37.53832795, 127.0852243),
    "구로구": (37.49571211, 126.8611501),
    "금천구": (37.45866568, 126.9005425),
    "노원구": (37.64563557, 127.0682143),
    "도봉구": (37.65923081, 127.038526),
    "동대문구": (37.58054743, 127.056142),
    "동작구": (37.49995871, 126.9495588),
    "마포구": (37.55341315, 126.9318522),
    "서대문구": (37.5797409, 126.9386953),
    "서초구": (37.49198703, 127.0124263),
    "성동구": (37.55291715, 127.0331672),
    "성북구": (37.6024169, 127.0266403),
    "송파구": (37.49966216, 127.1256782),
    "양천구": (37.52234835, 126.8588124),
    "영등포구": (37.51686324, 126.9052684),
    "용산구": (37.53093012, 126.9715924),
    "은평구": (37.61378461, 126.9199419),
    "종로구": (37.58051747, 126.9836777),
    "중구":   (37.56097776, 127.001138),
    "중랑구": (37.59890484, 127.0902436),
}


# ============================================================
# 2. 최근 36개월(year, month) 메타 정보 가져오기
# ============================================================

@st.cache_data
def get_recent_ym_list(dataset: str) -> List[Tuple[int, int]]:
    """
    주어진 dataset에 대해 '가장 최근 36개월'의 (year, month) 목록을 반환.
    - Supabase에서 year, month를 year DESC, month DESC로 정렬해서 가져온 후
      중복 제거하면서 상위 36개월만 취함.
    - 반환 시에는 (연도 오름차순, 월 오름차순)으로 정렬해서 돌려줌.
    """
    client = get_supabase_client()
    res = (
        client.table("krihs_mindicons")
        .select("year, month")
        .eq("dataset", dataset)
        .order("year", desc=True)
        .order("month", desc=True)
        .limit(1000)  # 36개월 × 26구 ≈ 936행 → 충분
        .execute()
    )

    if not res.data:
        return []

    seen = set()
    ym_desc: List[Tuple[int, int]] = []

    for row in res.data:
        y = int(row["year"])
        m = int(row["month"])
        key = (y, m)
        if key in seen:
            continue
        seen.add(key)
        ym_desc.append(key)
        if len(ym_desc) >= 36:  # 최근 36개월까지만
            break

    # 현재는 최신 → 과거 순서이므로, UI용으로 과거 → 최신 순서로 정렬
    ym_asc = sorted(ym_desc)
    return ym_asc


@st.cache_data
def get_year_options(dataset: str) -> List[int]:
    """최근 36개월 안에 등장하는 year만 반환."""
    ym_list = get_recent_ym_list(dataset)
    years = sorted({y for (y, _) in ym_list})
    return years


@st.cache_data
def get_month_options(dataset: str, year: int) -> List[int]:
    """해당 year에 대해 최근 36개월 안에 포함된 month만 반환."""
    ym_list = get_recent_ym_list(dataset)
    months = sorted({m for (y, m) in ym_list if y == year})
    return months


# ============================================================
# 3. Supabase에서 특정 연·월 데이터 가져오기
# ============================================================

@st.cache_data
def get_krihs_data(dataset: str, year: int, month: int) -> pd.DataFrame:
    """특정 dataset/year/month에 해당하는 행들을 DataFrame으로 가져오기."""
    client = get_supabase_client()
    res = (
        client.table("krihs_mindicons")
        .select("*")
        .eq("dataset", dataset)
        .eq("year", year)
        .eq("month", f"{int(month):02d}")  # '01' 형식 대응
        .execute()
    )

    df = pd.DataFrame(res.data or [])
    if df.empty:
        return df

    # 좌표 붙이기
    df["lat"] = df["region_name"].map(lambda x: SEOUL_GU_COORDS.get(x, (None, None))[0])
    df["lon"] = df["region_name"].map(lambda x: SEOUL_GU_COORDS.get(x, (None, None))[1])
    return df


# ============================================================
# 4. 보조 함수: 최근 n개월, 색상 매핑 등
# ============================================================

def prev_n_months(year: int, month: int, n: int = 3) -> List[Tuple[int, int]]:
    """
    (year, month)를 포함하여 과거 n개월을 반환.
    예: (2024, 5), n=3 → [(2024,5), (2024,4), (2024,3)]
    """
    result: List[Tuple[int, int]] = []
    y, m = year, month
    for _ in range(n):
        result.append((y, m))
        m -= 1
        if m < 1:
            y -= 1
            m = 12
    return result


def ym_label(y: int, m: int) -> str:
    return f"{y}-{int(m):02d}"


# 9단계 색상: –4(진한 파랑) ~ +4(진한 빨강)
SCORE_COLOR_MAP: Dict[int, str] = {
    -4: "#08306b",  # 매우 강한 수축
    -3: "#2171b5",
    -2: "#6baed6",
    -1: "#c6dbef",
     0: "#f7f7f7",  # 안정
     1: "#fee0d2",
     2: "#fc9272",
     3: "#fb6a4a",
     4: "#cb181d",  # 매우 강한 확장
}
DEFAULT_COLOR = "#d9d9d9"  # 점수 없을 때


def weather_score_to_color(score) -> str:
    """
    weather_score를 –4~+4 범위로 클램핑한 뒤 9단계 색상으로 변환.
    """
    if score is None or score == "":
        return DEFAULT_COLOR
    try:
        s = int(score)
    except (ValueError, TypeError):
        return DEFAULT_COLOR
    s = max(-4, min(4, s))
    return SCORE_COLOR_MAP.get(s, DEFAULT_COLOR)


# ============================================================
# 5. Streamlit UI + folium 지도
# ============================================================

def main():
    st.set_page_config(page_title="서울 부동산 심리지수 지도", layout="wide")
    st.title("서울 부동산 심리지수 지도 (KRIHS)")

    st.sidebar.header("옵션 선택")

    # --- 1) dataset 선택 ---
    datasets = [
        "market_consume",
        "house_consume",
        "house_sale",
        "house_rent",
        "land_consume",
    ]
    dataset = st.sidebar.selectbox("심리지수 종류 (dataset)", datasets, index=0)

    # --- 2) year 선택 (최근 36개월 안의 연도만) ---
    years = get_year_options(dataset)
    if not years:
        st.error(f"{dataset}에 해당하는 최근 36개월 데이터가 없습니다.")
        return

    # 기본값: 가장 최근 연도
    year = st.sidebar.selectbox("연도 (최근 36개월 기준)", years, index=len(years) - 1)

    # --- 3) month 선택 (해당 연도 안의 월만) ---
    months = get_month_options(dataset, year)
    if not months:
        st.error(f"{dataset}, {year}년 (최근 36개월 범위) 내 월 데이터가 없습니다.")
        return

    # 기본값: 해당 연도의 가장 최근 월
    month = st.sidebar.selectbox("월 (최근 36개월 기준)", months, index=len(months) - 1)

    st.sidebar.markdown(
        f"**기준월:** `{dataset}`, {year}년 {month}월  \n"
        f"(선택 가능 범위: 이 지표의 **가장 최근 36개월**)"
    )

    # --- 4) 기준월을 포함한 최근 3개월 데이터 합치기 ---
    ym_list = prev_n_months(year, month, n=3)   # [(기준월), (직전월), (직전2개월)]
    all_rows: List[pd.DataFrame] = []
    for (y, m) in ym_list:
        df_m = get_krihs_data(dataset, y, m)
        if df_m.empty:
            continue
        df_m = df_m.copy()
        df_m["ym"] = ym_label(y, m)
        all_rows.append(df_m)

    if not all_rows:
        st.warning("선택한 기준월 기준 최근 3개월 데이터가 없습니다.")
        return

    df_all = pd.concat(all_rows, ignore_index=True)



    # # --- 5) 데이터 미리보기 ---
    # st.subheader("데이터 미리보기 (기준월 포함 최근 3개월, weather_score 기준)")
    # st.dataframe(
    #     df_all[["dataset", "year", "month", "ym", "region_name",
    #             "weather", "weather_score", "mom"]],
    #     use_container_width=True,
    # )



#     # --- 6) 설명 텍스트 ---
#     st.markdown(
#         """
# **색상 의미 (weather_score 기준)**  

# - 점수 범위: –4 (매우 강한 수축) ~ +4 (매우 강한 확장)  
# - 색상은 **진한 파랑 → 옅은 파랑 → 회색(0) → 옅은 빨강 → 진한 빨강** 순으로 매핑됩니다.  

# **원 크기 의미 (최근 3개월)**  

# - 🔴 **큰 원**: 선택한 기준월  
# - ⚪ **중간 원**: 기준월 - 1개월  
# - ⚪ **작은 원**: 기준월 - 2개월  

# 마커에 마우스를 올리면 **구 이름 + 연월**,  
# 클릭하면 **weather / weather_score / mom(전월대비)**가 함께 표시됩니다.
# """
#     )

    # --- 7) 지도 그리기 (드래그/휠 확대 비활성화) ---
    center_lat, center_lon = 37.6000, 127.0500  # 서울 시청 근처
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles="CartoDB positron",
        dragging=False,         # ✅ 지도 드래그 불가
        scrollWheelZoom=False,  # ✅ 휠 확대/축소 불가
        doubleClickZoom=False,  # ✅ 더블클릭 줌 불가
        zoom_control=True,      # 줌 버튼은 유지 (원하면 False로 꺼도 됨)
    )

    # 원 크기: 오래된 달 → 작게, 기준월 → 크게
    radius_map: Dict[str, int] = {}
    radii = [6, 9, 12]  # 오래된 달, 중간, 기준월
    for idx, (y, mth) in enumerate(reversed(ym_list)):
        label = ym_label(y, mth)
        radius_map[label] = radii[idx]

    for _, row in df_all.iterrows():
        lat, lon = row["lat"], row["lon"]
        if pd.isna(lat) or pd.isna(lon):
            continue

        ym_str = row["ym"]
        color = weather_score_to_color(row.get("weather_score"))
        radius = radius_map.get(ym_str, 8)

        popup_html = (
            f"<b>{row['region_name']}</b><br>"
            f"기간: {ym_str}<br>"
            f"weather_score: {row['weather_score']}<br>"
            f"weather: {row['weather']}<br>"
            f"mom(전월대비): {row['mom']}"
        )

        tooltip_text = f"{row['region_name']} ({ym_str})"

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=popup_html,
            tooltip=tooltip_text,
        ).add_to(m)

    # --- 8) 지도 내 범례(legend) 추가 ---
    legend_html = """
    <div style="
        position: fixed;   
        top: 30px;
        left: 30px;
        z-index: 9999;
        background-color: white;
        padding: 10px 12px;
        border: 2px solid #444444;
        border-radius: 6px;
        font-size: 12px;
        ">
      <b>Weather_score 색상(–4 ~ +4)</b><br>
      <div style="display:flex; flex-direction:column; gap:2px; margin-top:4px;">
        <div><span style="display:inline-block;width:12px;height:12px;background:#08306b;border:1px solid #000;margin-right:4px;"></span> -4 (매우 강한 수축)</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#2171b5;border:1px solid #000;margin-right:4px;"></span> -3</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#6baed6;border:1px solid #000;margin-right:4px;"></span> -2</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#c6dbef;border:1px solid #000;margin-right:4px;"></span> -1</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#f7f7f7;border:1px solid #000;margin-right:4px;"></span>  0 (안정)</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#fee0d2;border:1px solid #000;margin-right:4px;"></span> +1</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#fc9272;border:1px solid #000;margin-right:4px;"></span> +2</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#fb6a4a;border:1px solid #000;margin-right:4px;"></span> +3</div>
        <div><span style="display:inline-block;width:12px;height:12px;background:#cb181d;border:1px solid #000;margin-right:4px;"></span> +4 (매우 강한 확장)</div>
      </div>
      <hr style="margin:6px 0;">
      <b>원 크기 (기준월 포함 최근 3개월)</b><br>
      <div style="margin-top:2px;">
        ● 큰 원: 기준월<br>
        ● 중간 원: 기준월 - 1개월<br>
        ● 작은 원: 기준월 - 2개월
      </div>
    </div>
    """
    m.get_root().html.add_child(Element(legend_html))

    st.subheader("서울 지도 (기준월 포함 최근 3개월, 최근 36개월 범위)")
    st_folium(m, width=900, height=600)


# ============================================================
# 6. 엔트리 포인트
# ============================================================

if __name__ == "__main__":
    main()
