"""
ETF-WACHT — stuurt een Telegram-bericht zodra een oordeel verandert.

Draait na src/etf_engine.py. De engine schrijft de wijzigingen al weg in
etf_data.json (blok "wijzigingen"); deze module vertaalt die naar een bericht
in gewone taal. Geen wijzigingen betekent geen bericht — stilte is informatie.

Alleen fondsen die je bij Trade Republic kunt kopen halen het bericht, en per
index maar één fonds: tien S&P 500-trackers die tegelijk omslaan is één signaal.
"""
from __future__ import annotations

import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etf_universe import UNIVERSE  # noqa: E402
from notifier import send  # noqa: E402

log = logging.getLogger("etf_watch")

BELANGRIJK = {"KOPEN", "KANS", "VERMIJDEN", "MOMENTUM-VAL", "STERK, MAAR DUUR"}

UITLEG = {
    "KOPEN": "sterke bedrijven én de trend staat mee",
    "KANS": "sterke bedrijven, koers blijft achter",
    "STERK, MAAR DUUR": "sterk, maar de groei is al ingeprijsd",
    "VERMIJDEN": "de bedrijven eronder zakken door de kwaliteitseis",
    "MOMENTUM-VAL": "hard gelopen zonder kwaliteit eronder",
    "HOUDEN": "middenmoot",
    "AFWACHTEN": "niet sterk, niet in trend",
}


def _pct(v) -> str:
    return "—" if v is None else f"{v * 100:+.0f}%"


def bouw_bericht(wijzigingen: list[dict]) -> str | None:
    """Bouwt het Telegram-bericht. Geeft None als er niets te melden valt."""
    relevant = [w for w in wijzigingen
                if w["naar"] in BELANGRIJK or w["van"] in BELANGRIJK]
    relevant = [w for w in relevant
                if UNIVERSE.get(w["ticker"], {}).get("tr") in ("zeker", "waarschijnlijk")]

    # Eén melding per index: dezelfde tracker bij vier aanbieders is één signaal.
    gezien, uniek = set(), []
    for w in relevant:
        sleutel = UNIVERSE.get(w["ticker"], {}).get("groep") or w["ticker"]
        if sleutel in gezien:
            continue
        gezien.add(sleutel)
        uniek.append(w)

    if not uniek:
        return None

    op = [w for w in uniek if w["naar"] in ("KOPEN", "KANS")]
    af = [w for w in uniek if w["naar"] in ("VERMIJDEN", "MOMENTUM-VAL")]
    rest = [w for w in uniek if w not in op and w not in af]

    regels = ["*ETF-signalen*", ""]

    def blok(titel: str, rijen: list[dict]) -> None:
        if not rijen:
            return
        regels.append(f"*{titel}*")
        for w in rijen[:6]:
            delta = ""
            if w.get("delta"):
                richting = "sterker" if w["delta"] > 0 else "zwakker"
                delta = f", kwaliteit {richting} ({w['delta']:+.1f})"
            regels.append(
                f"• {w['naam']} ({w['ticker']}) — {w['van'].lower()} → "
                f"*{w['naar'].lower()}*: {UITLEG.get(w['naar'], '')}{delta}. "
                f"12m {_pct(w.get('r12m'))}.")
        regels.append("")

    blok("Nieuwe kansen", op)
    blok("Weg ermee", af)
    blok("Overig", rest)

    regels.append("_Kwaliteit van de bedrijven weegt 60%, koersbeweging 40%._")
    return "\n".join(regels)


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        with open("etf_data.json", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        log.error("etf_data.json niet leesbaar: %s", exc)
        return

    wijzigingen = data.get("wijzigingen") or []
    log.info("%d oordeelwijzigingen sinds de vorige scan", len(wijzigingen))

    bericht = bouw_bericht(wijzigingen)
    if not bericht:
        log.info("Niets belangrijks veranderd — geen bericht verstuurd.")
        return

    if os.environ.get("DRY_RUN") == "1":
        print(bericht)
        return
    send(bericht)
    log.info("Telegram-bericht verstuurd.")


if __name__ == "__main__":
    main()
