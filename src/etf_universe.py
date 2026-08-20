"""
ETF UNIVERSUM — UCITS-ETF's, ingedeeld op thema.

Alle tickers zijn geverifieerd tegen Yahoo Finance (Xetra .DE / Euronext .AS /
Parijs .PA / Londen .L). Per thema staat de verwachte marktgroei (CAGR) van de
ONDERLIGGENDE markt — de "verwachting" waartegen het koersrendement wordt
afgezet, in de geest van het De Lange Termijn-dashboard.

TER = lopende kosten per jaar (%). Schattingen op basis van de aanbieder-
documentatie; ze bepalen alleen de kostencomponent van de kwaliteitsscore.
"""
from __future__ import annotations

# ── Thema's: verwachte jaarlijkse marktgroei (CAGR) van de onderliggende markt ─
# Bron: sectoranalyses (Grand View Research / MarketsandMarkets / IEA / WNA),
# afgerond en conservatief genomen. "None" = brede markt, geen themagroei.
THEMES: dict[str, dict] = {
    "Kern & Wereldwijd":      {"cagr": None, "kind": "core",  "icon": "🌍"},
    "Regio":                  {"cagr": None, "kind": "core",  "icon": "🗺️"},
    "Factor & Kwaliteit":     {"cagr": None, "kind": "core",  "icon": "💎"},
    "Dividend":               {"cagr": None, "kind": "core",  "icon": "💶"},
    "Small Caps":             {"cagr": None, "kind": "core",  "icon": "🌱"},
    "Sector Europa":          {"cagr": None, "kind": "sector","icon": "🏛️"},
    "Sector Wereld & VS":     {"cagr": None, "kind": "sector","icon": "🏢"},
    "AI & Robotica":          {"cagr": 0.30, "kind": "theme", "icon": "🤖"},
    "Semiconductors":         {"cagr": 0.13, "kind": "theme", "icon": "🔬"},
    "Cybersecurity":          {"cagr": 0.13, "kind": "theme", "icon": "🔐"},
    "Blockchain & Digitaal":  {"cagr": 0.45, "kind": "theme", "icon": "⛓️"},
    "Defensie":               {"cagr": 0.08, "kind": "theme", "icon": "🛡️"},
    "Space":                  {"cagr": 0.12, "kind": "theme", "icon": "🚀"},
    "Uranium & Kernenergie":  {"cagr": 0.11, "kind": "theme", "icon": "☢️"},
    "Schone energie":         {"cagr": 0.09, "kind": "theme", "icon": "⚡"},
    "Water":                  {"cagr": 0.07, "kind": "theme", "icon": "💧"},
    "Batterij & E-mobiliteit":{"cagr": 0.16, "kind": "theme", "icon": "🔋"},
    "Grondstoffen & Mijnbouw":{"cagr": 0.06, "kind": "theme", "icon": "⛏️"},
    "Edelmetalen":            {"cagr": 0.05, "kind": "theme", "icon": "🥇"},
    "Infrastructuur":         {"cagr": 0.07, "kind": "theme", "icon": "🏗️"},
    "Health & Biotech":       {"cagr": 0.10, "kind": "theme", "icon": "🧬"},
    "Luxe & Consument":       {"cagr": 0.06, "kind": "theme", "icon": "👜"},
    "EM Internet & Groei":    {"cagr": 0.14, "kind": "theme", "icon": "📱"},
    "Obligaties & Defensief": {"cagr": None, "kind": "bond",  "icon": "🧱"},
}

# ── Universum: ticker → (naam, thema, TER%) ───────────────────────────────────
UNIVERSE: dict[str, dict] = {}


def _add(ticker: str, name: str, theme: str, ter: float) -> None:
    UNIVERSE[ticker] = {"name": name, "theme": theme, "ter": ter}


# Kern & Wereldwijd
_add("EUNL.DE", "iShares Core MSCI World", "Kern & Wereldwijd", 0.20)
_add("IWDA.AS", "iShares Core MSCI World (Acc)", "Kern & Wereldwijd", 0.20)
_add("XDWD.DE", "Xtrackers MSCI World", "Kern & Wereldwijd", 0.19)
_add("XDWL.DE", "Xtrackers MSCI World 1D", "Kern & Wereldwijd", 0.19)
_add("SPPW.DE", "SPDR MSCI World", "Kern & Wereldwijd", 0.12)
_add("VWCE.DE", "Vanguard FTSE All-World (Acc)", "Kern & Wereldwijd", 0.22)
_add("VGWL.DE", "Vanguard FTSE All-World (Dist)", "Kern & Wereldwijd", 0.22)
_add("VWRL.AS", "Vanguard FTSE All-World (AMS)", "Kern & Wereldwijd", 0.22)
_add("IUSQ.DE", "iShares MSCI ACWI", "Kern & Wereldwijd", 0.20)
_add("SPYY.DE", "SPDR MSCI ACWI", "Kern & Wereldwijd", 0.12)
_add("SPYI.DE", "SPDR MSCI ACWI IMI", "Kern & Wereldwijd", 0.17)
_add("SXR8.DE", "iShares Core S&P 500 (Acc)", "Kern & Wereldwijd", 0.07)
_add("IUSA.DE", "iShares Core S&P 500 (Dist)", "Kern & Wereldwijd", 0.07)
_add("VUSA.AS", "Vanguard S&P 500", "Kern & Wereldwijd", 0.07)
_add("VUAA.DE", "Vanguard S&P 500 (Acc)", "Kern & Wereldwijd", 0.07)
_add("LYPS.DE", "Amundi Core S&P 500 Swap", "Kern & Wereldwijd", 0.05)
_add("D5BM.DE", "Xtrackers S&P 500 Swap", "Kern & Wereldwijd", 0.07)
_add("ESE.PA", "BNP Easy S&P 500", "Kern & Wereldwijd", 0.12)
_add("XD9U.DE", "Xtrackers MSCI USA", "Kern & Wereldwijd", 0.12)
_add("CW8.PA", "Amundi MSCI World Swap", "Kern & Wereldwijd", 0.38)
_add("SXRV.DE", "iShares Nasdaq 100 (Acc)", "Kern & Wereldwijd", 0.33)
_add("CNDX.AS", "iShares Nasdaq 100", "Kern & Wereldwijd", 0.33)
_add("XNAS.DE", "Xtrackers Nasdaq 100", "Kern & Wereldwijd", 0.20)
_add("LYMS.DE", "Amundi Core Nasdaq 100 Swap", "Kern & Wereldwijd", 0.22)
_add("EXXT.DE", "iShares Nasdaq 100 (DE)", "Kern & Wereldwijd", 0.31)
_add("PANX.PA", "Amundi PEA US Tech", "Kern & Wereldwijd", 0.28)
_add("TSWE.DE", "VanEck World Equal Weight", "Kern & Wereldwijd", 0.20)
_add("XZW0.DE", "Xtrackers MSCI World ESG", "Kern & Wereldwijd", 0.20)
_add("2B7K.DE", "iShares MSCI World SRI", "Kern & Wereldwijd", 0.20)
_add("CBUY.DE", "iShares MSCI ACWI SRI", "Kern & Wereldwijd", 0.20)
_add("UIMM.DE", "UBS MSCI World SRI", "Kern & Wereldwijd", 0.22)
_add("XDWY.DE", "Xtrackers MSCI World Screened", "Kern & Wereldwijd", 0.19)

# Regio
_add("IS3N.DE", "iShares Core MSCI EM IMI", "Regio", 0.18)
_add("XMME.DE", "Xtrackers MSCI EM", "Regio", 0.18)
_add("VFEM.DE", "Vanguard FTSE Emerging Markets", "Regio", 0.22)
_add("AYEM.DE", "iShares MSCI EM IMI Screened", "Regio", 0.18)
_add("XZEM.DE", "Xtrackers MSCI EM ESG", "Regio", 0.25)
_add("PAEEM.PA", "Amundi PEA Émergent", "Regio", 0.30)
_add("AASI.PA", "Amundi MSCI EM Asia", "Regio", 0.20)
_add("VEUR.AS", "Vanguard FTSE Developed Europe", "Regio", 0.10)
_add("VERX.DE", "Vanguard FTSE Dev. Europe ex UK", "Regio", 0.10)
_add("MEUD.PA", "Amundi Stoxx Europe 600", "Regio", 0.07)
_add("XESC.DE", "Xtrackers Euro Stoxx 50", "Regio", 0.09)
_add("VJPN.DE", "Vanguard FTSE Japan", "Regio", 0.15)
_add("XDN0.DE", "Xtrackers MSCI Nordic", "Regio", 0.30)
_add("XCS6.DE", "Xtrackers MSCI China", "Regio", 0.65)
_add("CNAA.DE", "Amundi MSCI China A", "Regio", 0.40)
_add("FLXI.DE", "Franklin FTSE India", "Regio", 0.19)
_add("X014.DE", "Amundi MSCI Pacific ESG", "Regio", 0.30)
_add("RS2K.PA", "Amundi Russell 2000", "Regio", 0.35)

# Factor & Kwaliteit
_add("IS3Q.DE", "iShares MSCI World Quality Factor", "Factor & Kwaliteit", 0.30)
_add("XDEQ.DE", "Xtrackers MSCI World Quality", "Factor & Kwaliteit", 0.25)
_add("IS3R.DE", "iShares MSCI World Momentum", "Factor & Kwaliteit", 0.30)
_add("XDEM.DE", "Xtrackers MSCI World Momentum", "Factor & Kwaliteit", 0.25)
_add("IS3S.DE", "iShares MSCI World Value", "Factor & Kwaliteit", 0.30)
_add("XDEV.DE", "Xtrackers MSCI World Value", "Factor & Kwaliteit", 0.25)
_add("JPGL.DE", "JPM Global Equity Multi-Factor", "Factor & Kwaliteit", 0.19)
_add("IBC0.DE", "iShares STOXX Europe Multifactor", "Factor & Kwaliteit", 0.45)
_add("5HED.DE", "Ossiam Shiller CAPE US Sector", "Factor & Kwaliteit", 0.65)

# Dividend
_add("VHYL.AS", "Vanguard FTSE All-World High Div", "Dividend", 0.29)
_add("TDIV.AS", "VanEck Dev. Markets Dividend Leaders", "Dividend", 0.38)
_add("ISPA.DE", "iShares STOXX Global Select Div 100", "Dividend", 0.46)
_add("SPYD.DE", "SPDR S&P US Dividend Aristocrats", "Dividend", 0.35)
_add("SPYW.DE", "SPDR S&P Euro Dividend Aristocrats", "Dividend", 0.30)
_add("FUSD.DE", "Fidelity US Quality Income", "Dividend", 0.25)
_add("DXSA.DE", "Xtrackers Euro Stoxx Quality Dividend", "Dividend", 0.30)

# Small Caps
_add("IUSN.DE", "iShares MSCI World Small Cap", "Small Caps", 0.35)
_add("ZPRS.DE", "SPDR MSCI World Small Cap", "Small Caps", 0.45)
_add("WSML.AS", "iShares MSCI World Small Cap (AS)", "Small Caps", 0.35)
_add("ZPRV.DE", "SPDR MSCI USA Small Cap Value", "Small Caps", 0.30)
_add("ZPRX.DE", "SPDR MSCI Europe Small Cap Value", "Small Caps", 0.30)
_add("CBUG.DE", "iShares MSCI World Small Cap ESG", "Small Caps", 0.35)

# Sector Europa
_add("EXV1.DE", "STOXX Europe 600 Banks", "Sector Europa", 0.46)
_add("EXV3.DE", "STOXX Europe 600 Technology", "Sector Europa", 0.46)
_add("EXV4.DE", "STOXX Europe 600 Health Care", "Sector Europa", 0.46)
_add("EXV6.DE", "STOXX Europe 600 Basic Resources", "Sector Europa", 0.46)
_add("EXV7.DE", "STOXX Europe 600 Chemicals", "Sector Europa", 0.46)
_add("EXV8.DE", "STOXX Europe 600 Construction", "Sector Europa", 0.46)
_add("EXV9.DE", "STOXX Europe 600 Travel & Leisure", "Sector Europa", 0.46)
_add("EXH1.DE", "STOXX Europe 600 Oil & Gas", "Sector Europa", 0.46)
_add("EXH4.DE", "STOXX Europe 600 Industrial Goods", "Sector Europa", 0.46)
_add("EXH9.DE", "STOXX Europe 600 Utilities", "Sector Europa", 0.46)
_add("EXI5.DE", "STOXX Europe 600 Real Estate", "Sector Europa", 0.46)
_add("ESIT.DE", "iShares MSCI Europe Info Tech", "Sector Europa", 0.18)
_add("ESIH.DE", "iShares MSCI Europe Health Care", "Sector Europa", 0.18)

# Sector Wereld & VS
_add("XDWT.DE", "Xtrackers MSCI World Info Tech", "Sector Wereld & VS", 0.25)
_add("XDWH.DE", "Xtrackers MSCI World Health Care", "Sector Wereld & VS", 0.25)
_add("XDWI.DE", "Xtrackers MSCI World Industrials", "Sector Wereld & VS", 0.25)
_add("XDWS.DE", "Xtrackers MSCI World Cons. Staples", "Sector Wereld & VS", 0.25)
_add("XDWC.DE", "Xtrackers MSCI World Cons. Discr.", "Sector Wereld & VS", 0.25)
_add("QDVE.DE", "iShares S&P 500 Info Tech", "Sector Wereld & VS", 0.15)
_add("QDVG.DE", "iShares S&P 500 Health Care", "Sector Wereld & VS", 0.15)
_add("QDVH.DE", "iShares S&P 500 Financials", "Sector Wereld & VS", 0.15)
_add("IITU.L", "iShares S&P 500 Info Tech (L)", "Sector Wereld & VS", 0.15)
_add("IHCU.L", "iShares S&P 500 Health Care (L)", "Sector Wereld & VS", 0.15)
_add("IUFS.L", "iShares S&P 500 Financials (L)", "Sector Wereld & VS", 0.15)
_add("WELL.DE", "Amundi S&P World Info Tech", "Sector Wereld & VS", 0.18)
_add("ZPDK.DE", "SPDR S&P US Communication Services", "Sector Wereld & VS", 0.15)

# AI & Robotica
_add("XAIX.DE", "Xtrackers AI & Big Data", "AI & Robotica", 0.35)
_add("WTAI.DE", "WisdomTree Artificial Intelligence", "AI & Robotica", 0.40)
_add("XMLD.DE", "L&G Artificial Intelligence", "AI & Robotica", 0.49)
_add("GOAI.DE", "Amundi MSCI Robotics & AI", "AI & Robotica", 0.40)
_add("2B76.DE", "iShares Automation & Robotics", "AI & Robotica", 0.40)

# Semiconductors
_add("SMH.DE", "VanEck Semiconductor (oud)", "Semiconductors", 0.35)
_add("VVSM.DE", "VanEck Semiconductor", "Semiconductors", 0.35)

# Cybersecurity
_add("L0CK.DE", "iShares Digital Security", "Cybersecurity", 0.40)
_add("ISPY.AS", "L&G Cyber Security", "Cybersecurity", 0.69)
_add("USPY.DE", "L&G Cyber Security (DE)", "Cybersecurity", 0.69)
_add("CBRS.DE", "First Trust Nasdaq Cybersecurity", "Cybersecurity", 0.60)

# Blockchain & Digitaal
_add("BNXG.DE", "Invesco CoinShares Global Blockchain", "Blockchain & Digitaal", 0.65)
_add("BLOK.DE", "First Trust Innovative Transaction", "Blockchain & Digitaal", 0.65)

# Defensie
_add("ASWC.DE", "VanEck Future of Defence", "Defensie", 0.55)

# Space
_add("JEDI.DE", "VanEck Space Innovators", "Space", 0.55)

# Uranium & Kernenergie
_add("URNU.DE", "Global X Uranium", "Uranium & Kernenergie", 0.65)
_add("U3O8.DE", "Sprott Uranium Miners", "Uranium & Kernenergie", 0.85)
_add("NUKL.DE", "VanEck Uranium & Nuclear", "Uranium & Kernenergie", 0.55)

# Schone energie
_add("IQQH.DE", "iShares Global Clean Energy", "Schone energie", 0.65)
_add("RENW.DE", "L&G Clean Energy", "Schone energie", 0.49)
_add("NRJ.PA", "Amundi MSCI New Energy", "Schone energie", 0.60)

# Water
_add("IQQQ.DE", "iShares Global Water", "Water", 0.65)
_add("WAT.PA", "Amundi MSCI Water", "Water", 0.60)

# Batterij & E-mobiliteit
_add("BATE.DE", "L&G Battery Value-Chain", "Batterij & E-mobiliteit", 0.49)

# Grondstoffen & Mijnbouw
_add("ASWD.DE", "Sprott Pure Play Copper Miners", "Grondstoffen & Mijnbouw", 0.65)

# Edelmetalen
_add("4GLD.DE", "Xetra-Gold", "Edelmetalen", 0.30)
_add("PPFB.DE", "iShares Physical Metals", "Edelmetalen", 0.20)

# Infrastructuur
_add("INFR.L", "iShares Global Infrastructure", "Infrastructuur", 0.65)
_add("IQQ6.DE", "iShares Dev. Markets Property Yield", "Infrastructuur", 0.59)

# EM Internet & Groei
_add("EMQQ.DE", "EMQQ Emerging Markets Internet", "EM Internet & Groei", 0.86)
_add("CEMG.DE", "iShares MSCI EM Consumer Growth", "EM Internet & Groei", 0.60)

# Luxe & Consument
_add("GLUX.DE", "Amundi Global Luxury", "Luxe & Consument", 0.35)

# Obligaties & Defensief
_add("EUNA.DE", "iShares Core Global Aggregate Bond", "Obligaties & Defensief", 0.10)
_add("VAGF.DE", "Vanguard Global Aggregate Bond", "Obligaties & Defensief", 0.10)
_add("EUNH.DE", "iShares Core € Govt Bond", "Obligaties & Defensief", 0.09)
_add("IBCX.DE", "iShares € Corp Bond Large Cap", "Obligaties & Defensief", 0.20)
_add("IUSV.DE", "iShares $ Treasury 20+yr", "Obligaties & Defensief", 0.07)
_add("IS3V.DE", "iShares Global Inflation Linked", "Obligaties & Defensief", 0.25)
_add("XG7S.DE", "Xtrackers Global Government Bond", "Obligaties & Defensief", 0.20)


def by_theme() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for t, meta in UNIVERSE.items():
        out.setdefault(meta["theme"], []).append(t)
    return out


if __name__ == "__main__":
    print(f"{len(UNIVERSE)} ETF's in {len(by_theme())} thema's")
    for th, ts in sorted(by_theme().items()):
        print(f"  {th:26s} {len(ts):3d}")


# ── Aanbieder → beschikbaarheid bij Trade Republic ────────────────────────────
# Trade Republic publiceert geen open lijst per ISIN. Dit is een inschatting op
# basis van de aanbieders die TR zelf noemt (iShares, Vanguard, Xtrackers,
# Amundi, Invesco, SPDR, VanEck) plus de kleinere huizen die daar los van staan.
#   "zeker"        — grote aanbieder, standaard in het TR-aanbod
#   "waarschijnlijk" — kleiner huis, meestal wel verhandelbaar
#   "onzeker"      — nichefonds, controleer in de app voor je koopt
TR_PROVIDERS = {
    "ishares": "zeker", "stoxx europe": "zeker", "xtrackers": "zeker",
    "vanguard": "zeker", "amundi": "zeker", "spdr": "zeker",
    "invesco": "zeker", "vaneck": "zeker", "wisdomtree": "waarschijnlijk",
    "l&g": "waarschijnlijk", "fidelity": "waarschijnlijk",
    "first trust": "onzeker", "sprott": "onzeker", "franklin": "onzeker",
    "global x": "onzeker", "ubs": "waarschijnlijk", "jpm": "waarschijnlijk",
    "ossiam": "onzeker", "bnp": "waarschijnlijk", "xetra-gold": "waarschijnlijk",
}


def tr_status(name: str) -> str:
    low = name.lower()
    for key, status in TR_PROVIDERS.items():
        if key in low:
            return status
    return "onzeker"


# ── Dubbelen: fondsen die dezelfde index volgen ───────────────────────────────
# Tien S&P 500-trackers naast elkaar is geen keuze maar ruis. Fondsen met
# dezelfde sleutel worden in het dashboard samengevouwen tot de goedkoopste.
INDEX_GROUPS: dict[str, str] = {}
for _key, _tickers in {
    "S&P 500": "SXR8.DE IUSA.DE VUSA.AS VUAA.DE LYPS.DE D5BM.DE ESE.PA",
    "MSCI World": "EUNL.DE IWDA.AS XDWD.DE XDWL.DE SPPW.DE CW8.PA XDWY.DE TSWE.DE",
    "FTSE All-World": "VWCE.DE VGWL.DE VWRL.AS",
    "MSCI ACWI": "IUSQ.DE SPYY.DE SPYI.DE",
    "Nasdaq 100": "SXRV.DE CNDX.AS XNAS.DE LYMS.DE EXXT.DE",
    "MSCI World SRI/ESG": "XZW0.DE 2B7K.DE CBUY.DE UIMM.DE",
    "MSCI EM": "IS3N.DE XMME.DE VFEM.DE AYEM.DE XZEM.DE PAEEM.PA",
    "Europa breed": "VEUR.AS VERX.DE MEUD.PA",
    "MSCI USA": "XD9U.DE",
    "S&P 500 Info Tech": "QDVE.DE IITU.L",
    "S&P 500 Health Care": "QDVG.DE IHCU.L",
    "S&P 500 Financials": "QDVH.DE IUFS.L",
    "World Info Tech": "XDWT.DE WELL.DE",
    "World Small Cap": "IUSN.DE ZPRS.DE WSML.AS CBUG.DE",
    "Semiconductors": "SMH.DE VVSM.DE",
    "Cybersecurity L&G": "ISPY.AS USPY.DE",
    "Global Aggregate Bond": "EUNA.DE VAGF.DE",
}.items():
    for _t in _tickers.split():
        INDEX_GROUPS[_t] = _key

for _t, _meta in UNIVERSE.items():
    _meta["tr"] = tr_status(_meta["name"])
    _meta["groep"] = INDEX_GROUPS.get(_t)
