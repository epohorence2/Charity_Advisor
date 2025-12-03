# FILE: backend/app.py
"""
Backend quickstart
------------------
1. Copy backend/.env.example to backend/.env and set EVERY_API_KEY.
2. Install dependencies from backend/requirements.txt.
3. Start the API locally with: uvicorn app:app --reload --host 0.0.0.0 --port 8000
4. Point BACKEND_BASE_URL in docs/assets/survey.js to the deployed API (or http://localhost:8000 for dev).
"""

from __future__ import annotations

import datetime
import os
import random
from typing import Any, Dict, List, Optional, Sequence

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from catalog import CharityRow, fetch_random_pool_for_deciles

EVERY_NONPROFIT_BASE_URL = "https://partners.every.org/v0.2/nonprofit"
MAX_CHARITIES = 15
_EVERY_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}

# Placeholder mapping; keep unchanged so survey codes continue to work.
GENERIC_TO_NTEE: Dict[str, List[str]] = {
    "A0": ["A50", "A51", "A52", "A54", "A56", "A57", "A80"],
    "A1": ["A60", "A61", "A62", "A63", "A65", "A68", "A69", "A6A", "A6B", "A6C", "A6E"],
    "A2": ["A30", "A31", "A32", "A33", "A34"],
    "A3": ["A20", "A23", "A25", "A40", "A70", "A84", "A99"],
    "A4": ["A01", "A02", "A03", "A05", "A11", "A12", "A19", "A26", "A90"],
    "B0": ["B20", "B21", "B24", "B25", "B28"],
    "B1": ["B30", "B40", "B41", "B42", "B43", "B50", "B60"],
    "B2": ["B70", "B80", "B82", "B83", "B84"],
    "B3": ["B90", "B92", "B94", "B99"],
    "B4": ["B01", "B02", "B03", "B05", "B11", "B12", "B19"],
    "X0": ["X20", "X21", "X22", "X30", "X40", "X50", "X70"],
    "X1": ["X80", "X81", "X82", "X83", "X84"],
    "X2": ["X90", "X99"],
    "X3": ["X01", "X02", "X03", "X05", "X11", "X12"],
    "E0": ["E20", "E21", "E22", "E24"],
    "E1": ["E30", "E31", "E32", "E40", "E42"],
    "E2": ["E90", "E91", "E92"],
    "E3": ["E50", "E60", "E61", "E62", "E65", "E70", "E80", "E86", "E99"],
    "E4": ["E01", "E02", "E03", "E05", "E11", "E12", "E19"],
    "F0": ["F20", "F21", "F22", "F50", "F52", "F53", "F54"],
    "F1": ["F30", "F31", "F32", "F33", "F70", "F80", "F99"],
    "F2": ["F40", "F42", "F60"],
    "F3": ["F01", "F02", "F03", "F05", "F11", "F12", "F19"],
    "G0": ["G40", "G41", "G42", "G43", "G44", "G45", "G48", "G50", "G51", "G54", "G60", "G61", "G70"],
    "G1": ["G20", "G25", "G30", "G80", "G81", "G83", "G84"],
    "G2": ["G90", "G92", "G94", "G96", "G98", "G9B", "G99"],
    "G3": ["G11", "G12", "G19"],
    "G4": ["G01", "G02", "G03", "G05"],
    "H0": ["H20", "H25", "H30", "H80", "H81", "H83", "H84"],
    "H1": ["H40", "H41", "H42", "H43", "H44", "H45", "H48", "H50", "H51", "H54", "H60", "H61", "H70"],
    "H2": ["H90", "H92", "H94", "H96", "H98", "H9B", "H99"],
    "H3": ["H01", "H02", "H03", "H05", "H11", "H12", "H19"],
    "C0": ["C20", "C27", "C35"],
    "C1": ["C30", "C32", "C34", "C36"],
    "C2": ["C40", "C41", "C42", "C50", "C60", "C99"],
    "C3": ["C01", "C02", "C03", "C05", "C11", "C12", "C19"],
    "D0": ["D20", "D40", "D60", "D61"],
    "D1": ["D30", "D31", "D32", "D33", "D34"],
    "D2": ["D50", "D99"],
    "D3": ["D01", "D02", "D03", "D05", "D11", "D12", "D19"],
    "I0": ["I20", "I21", "I23", "I70", "I71", "I72", "I73"],
    "I1": ["I30", "I31", "I40", "I43", "I44"],
    "I2": ["I50", "I51", "I60"],
    "I3": ["I80", "I83", "I99"],
    "I4": ["I01", "I02", "I03", "I05", "I11", "I12", "I19"],
    "J0": ["J20", "J21", "J22", "J99"],
    "J1": ["J30", "J32", "J33"],
    "J2": ["J03", "J40"],
    "J3": ["J01", "J02", "J05", "J11", "J12", "J19"],
    "K0": ["K30", "K31", "K34", "K35", "K36"],
    "K1": ["K20", "K25", "K26", "K28"],
    "K2": ["K40", "K50", "K99"],
    "K3": ["K01", "K02", "K03", "K05", "K11", "K12", "K19"],
    "L0": ["L20", "L21", "L22", "L25"],
    "L1": ["L40", "L41"],
    "L2": ["L30", "L50", "L80", "L81", "L82", "L99"],
    "L3": ["L01", "L02", "L03", "L05", "L11", "L12", "L19"],
    "M0": ["M20", "M23", "M24", "M99"],
    "M1": ["M40", "M41", "M42"],
    "M2": ["M01", "M02", "M03", "M05", "M11", "M12", "M19"],
    "N0": ["N20", "N30", "N31", "N32"],
    "N1": ["N50", "N60", "N61", "N62", "N63", "N64", "N65", "N66", "N67", "N68", "N69", "N6A"],
    "N2": ["N70", "N71", "N72"],
    "N3": ["N01", "N02", "N03", "N05", "N11", "N12", "N19"],
    "O0": ["O20", "O21", "O22", "O23"],
    "O1": ["O30", "O31", "O40", "O41", "O42", "O43"],
    "O2": ["O50", "O51", "O52", "O53", "O54", "O55", "O99"],
    "O3": ["O01", "O02", "O03", "O05", "O11", "O12", "O19"],
    "P0": ["P30", "P31", "P32", "P33", "P40", "P42", "P43", "P44", "P45", "P46"],
    "P1": ["P80", "P81", "P82", "P84", "P85", "P86", "P87"],
    "P2": ["P58", "P60", "P61", "P62"],
    "P3": ["P70", "P72", "P73", "P74", "P75"],
    "P4": ["P20", "P21", "P22", "P24", "P26", "P27", "P28", "P29", "P50", "P51", "P52", "P99"],
    "P5": ["P01", "P02", "P03", "P05", "P11", "P12", "P19"],
    "Q0": ["Q30", "Q31", "Q32", "Q33", "Q70", "Q71"],
    "Q1": ["Q20", "Q21", "Q22", "Q23"],
    "Q2": ["Q40", "Q41", "Q42", "Q43", "Q99"],
    "Q3": ["Q01", "Q02", "Q03", "Q05", "Q11", "Q12", "Q19"],
    "R0": ["R20", "R22", "R23", "R24", "R25", "R26", "R30"],
    "R1": ["R40", "R60", "R61", "R62", "R63", "R67", "R99"],
    "R2": ["R01", "R02", "R03", "R05", "R11", "R12", "R19"],
    "S0": ["S20", "S21", "S22"],
    "S1": ["S30", "S31", "S32"],
    "S2": ["S40", "S41", "S43", "S46", "S47", "S50", "S80", "S81", "S82", "S99"],
    "S3": ["S01", "S02", "S03", "S05", "S11", "S12", "S19"],
    "T0": ["T20", "T21", "T22", "T23", "T90"],
    "T1": ["T30", "T31", "T70"],
    "T2": ["T40", "T50", "T99"],
    "T3": ["T01", "T02", "T03", "T05", "T11", "T12", "T19"],
    "U0": ["U30", "U31", "U33", "U34", "U36"],
    "U1": ["U40", "U41", "U42"],
    "U2": ["U20", "U21", "U50", "U99"],
    "U3": ["U01", "U02", "U03", "U05", "U11", "U12", "U19"],
    "V0": ["V20", "V21", "V22", "V23", "V24", "V25", "V26"],
    "V1": ["V30", "V31", "V32", "V33", "V34", "V35", "V36", "V37", "V99"],
    "V2": ["V01", "V02", "V03", "V05", "V11", "V12", "V19"],
    "W0": ["W30", "W40", "W50", "W60", "W99"],
    "W1": ["W70", "W71", "W72", "W73"],
    "W2": ["W01", "W02", "W03", "W05", "W11", "W12", "W19"],
    "Y0": ["Y30", "Y33", "Y34", "Y40"],
    "Y1": ["Y20", "Y22", "Y99"],
    "Y2": ["Y50", "Y60", "Y70"],
    "Y3": ["Y01", "Y02", "Y03", "Y05", "Y11", "Y12", "Y19"],
}

ALLOWED_ORIGINS = [
    "https://map9900.github.io",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
]


class SurveyRequest(BaseModel):
    generic_code: str
    location: str


class CharityResponse(BaseModel):
    generic_code: str
    location: str
    ntee_codes: Sequence[str]
    charities: Sequence[Dict[str, Optional[str]]]


def normalize_state(location: str) -> str:
    """
    Normalize the location string from the survey into a state code or 'any'.
    """
    loc = (location or "").strip()
    if not loc:
        return "any"
    if loc.lower() == "any":
        return "any"
    if len(loc) == 2 and loc.isalpha():
        return loc.upper()
    return "any"


def group_charities_by_decile(rows: List[CharityRow]) -> Dict[str, List[Dict[str, Optional[str]]]]:
    """
    Group catalog rows by NTEE decile and normalize them into the
    shape expected by round_robin_select/CharityResponse.
    """
    grouped: Dict[str, List[Dict[str, Optional[str]]]] = {}
    for row in rows:
        decile = row.get("ntee_code") or row.get("nteeCode")
        if not decile:
            continue

        city = row.get("city") or ""
        state = row.get("state") or ""
        location_str: Optional[str] = None
        if city and state:
            location_str = f"{city}, {state}"
        elif state:
            location_str = state

        normalized = {
            "name": row.get("name") or "Unknown",
            "ein": row.get("ein"),
            "profileUrl": None,
            "websiteUrl": None,
            "nteeCode": decile,
            "location": location_str,
            "description": None,
        }
        grouped.setdefault(decile, []).append(normalized)
    return grouped


def fetch_every_nonprofit_detail(ein: str) -> Optional[Dict[str, Any]]:
    """
    Fetch nonprofit details for a given EIN from Every.org using a simple in-memory cache.
    """
    if not ein:
        return None

    cached = _EVERY_CACHE.get(ein)
    if cached is not None:
        return cached

    api_key = get_api_key()
    url = f"{EVERY_NONPROFIT_BASE_URL}/{ein}"
    params = {"apiKey": api_key}

    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.RequestException:
        _EVERY_CACHE[ein] = None
        return None

    if resp.status_code >= 400:
        _EVERY_CACHE[ein] = None
        return None

    try:
        payload = resp.json()
    except ValueError:
        _EVERY_CACHE[ein] = None
        return None

    nonprofit = payload.get("data", {}).get("nonprofit")
    if not isinstance(nonprofit, dict) or not nonprofit:
        _EVERY_CACHE[ein] = None
        return None

    _EVERY_CACHE[ein] = nonprofit
    return nonprofit


def enrich_charities_with_every(charities: List[Dict[str, Optional[str]]]) -> None:
    """
    Enrich the in-memory charity dicts using Every.org data when available.
    """
    for charity in charities:
        ein = charity.get("ein")
        if not ein:
            continue

        detail = fetch_every_nonprofit_detail(ein)
        if not detail:
            continue

        profile_url = detail.get("profileUrl") or detail.get("profile") or detail.get("url")
        website_url = detail.get("websiteUrl") or detail.get("website")
        description = detail.get("description") or detail.get("mission")
        location = detail.get("location") or detail.get("locationName") or charity.get("location")
        ntee_code = detail.get("nteeCode") or charity.get("nteeCode")

        if profile_url:
            charity["profileUrl"] = profile_url
        if website_url:
            charity["websiteUrl"] = website_url
        if description:
            charity["description"] = description
        if location:
            charity["location"] = location
        if ntee_code:
            charity["nteeCode"] = ntee_code


def get_api_key() -> str:
    api_key = os.getenv("EVERY_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="EVERY_API_KEY is not configured.")
    return api_key


def get_daily_featured_pool(pool_size: int = 200) -> List[Dict[str, Optional[str]]]:
    """
    Fetch a broad pool of normalized charities from the catalog for the featured list.
    """
    sample_generic_codes = ["D0", "B0", "E0", "P0", "C0", "N0"]
    decile_candidates: List[str] = []
    for code in sample_generic_codes:
        decile_candidates.extend(GENERIC_TO_NTEE.get(code, []))
    deciles = list(dict.fromkeys(decile_candidates))
    if not deciles:
        return []

    rows = fetch_random_pool_for_deciles(
        deciles=deciles,
        state=None,
        pool_size=pool_size,
        seed=None,
    )

    normalized: List[Dict[str, Optional[str]]] = []
    for row in rows:
        decile = row.get("ntee_code") or row.get("nteeCode")
        if not decile:
            continue

        city = row.get("city") or ""
        state = row.get("state") or ""
        location_str: Optional[str] = None
        if city and state:
            location_str = f"{city}, {state}"
        elif state:
            location_str = state

        normalized.append(
            {
                "name": row.get("name") or "Unknown",
                "ein": row.get("ein"),
                "profileUrl": None,
                "websiteUrl": None,
                "nteeCode": decile,
                "location": location_str,
                "description": None,
            }
        )

    return normalized




def round_robin_select(
    charities_by_code: Dict[str, List[Dict[str, Optional[str]]]],
    limit: int = MAX_CHARITIES,
) -> List[Dict[str, Optional[str]]]:
    selected: List[Dict[str, Optional[str]]] = []
    seen_ids: set[str] = set()
    codes = [code for code, charities in charities_by_code.items() if charities]
    indices = {code: 0 for code in codes}

    if not codes:
        return selected

    while len(selected) < limit:
        progressed = False
        for code in codes:
            charities = charities_by_code.get(code, [])
            idx = indices.get(code, 0)
            while idx < len(charities):
                charity = charities[idx]
                unique_id = charity.get("ein") or charity.get("profileUrl") or f"{charity.get('name')}-{code}"
                idx += 1
                if not unique_id or unique_id in seen_ids:
                    continue

                seen_ids.add(unique_id)
                selected.append(charity)
                indices[code] = idx
                progressed = True
                break

            if len(selected) >= limit:
                break

        if not progressed:
            break

    return selected


app = FastAPI(title="Charity Recommender API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.post("/recommend", response_model=CharityResponse)
def recommend_charities(payload: SurveyRequest) -> CharityResponse:
    """
    Primary recommendation entrypoint backed by the local SQLite catalog.
    """

    generic_code = (payload.generic_code or "").strip().upper()
    location = (payload.location or "").strip()

    if not generic_code:
        raise HTTPException(status_code=400, detail="generic_code is required.")

    target_deciles = GENERIC_TO_NTEE.get(generic_code)
    if not target_deciles:
        raise HTTPException(status_code=400, detail=f"Unknown generic code: {generic_code}")

    state_code = normalize_state(location)
    pool_size = MAX_CHARITIES * 4
    seed = None

    pool: List[CharityRow] = fetch_random_pool_for_deciles(
        deciles=target_deciles,
        state=None if state_code == "any" else state_code,
        pool_size=pool_size,
        seed=seed,
    )

    if not pool:
        raise HTTPException(
            status_code=404,
            detail="No charities found in the database for these NTEE codes and location.",
        )

    charities_by_decile = group_charities_by_decile(pool)
    selected = round_robin_select(charities_by_decile, limit=MAX_CHARITIES)

    if not selected:
        raise HTTPException(
            status_code=404,
            detail="No charities could be selected after grouping.",
        )

    enrich_charities_with_every(selected)

    return CharityResponse(
        generic_code=generic_code,
        location=location,
        ntee_codes=target_deciles,
        charities=selected,
    )


@app.get("/featured")
def get_featured_charities() -> Dict[str, Any]:
    """
    Return a daily rotating set of featured charities with website links.
    """

    today = datetime.date.today()
    seed = int(today.strftime("%Y%m%d"))

    pool = get_daily_featured_pool(pool_size=200)
    if not pool:
        return {"date": today.isoformat(), "count": 0, "charities": []}

    enrich_charities_with_every(pool)

    with_website = [charity for charity in pool if charity.get("websiteUrl")]

    rng = random.Random(seed)
    rng.shuffle(with_website)

    featured = with_website[:6]

    return {
        "date": today.isoformat(),
        "count": len(featured),
        "charities": featured,
    }
