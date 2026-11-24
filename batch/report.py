"""Generate the daily profitability report: HTML (Jinja2 + embedded chart) and CSV."""
from __future__ import annotations

import base64
import io
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # no screen inside a container: render straight to an image
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from jinja2 import Environment, FileSystemLoader, select_autoescape  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

TEMPLATE_DIR = Path(__file__).parent / "templates"

# Colours: a validated blue/red diverging pair (passes colour-blind separation checks)
PROFIT_COLOR = "#2a78d6"
LOSS_COLOR = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"


def profit_chart_png(df: pd.DataFrame) -> str:
    """Horizontal bar chart of profit per vehicle, returned as a base64 PNG string.

    Bars are sorted so losses sit at the top; only loss bars get value labels,
    so the reader's eye goes straight to the problem vehicles.
    """
    data = df.sort_values("profit", ascending=False)  # barh draws bottom-up -> lowest profit on top
    colors = [LOSS_COLOR if p < 0 else PROFIT_COLOR for p in data["profit"]]

    fig, ax = plt.subplots(figsize=(9, 0.26 * len(data) + 1.3), dpi=110)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.barh(data["vehicle_id"], data["profit"], color=colors, height=0.62, edgecolor=SURFACE, linewidth=1.5)
    ax.axvline(0, color=TEXT_SECONDARY, linewidth=1)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=8, length=0)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_xlabel("Daily profit (LKR)", color=TEXT_SECONDARY, fontsize=9)

    for y, profit in enumerate(data["profit"]):
        if profit < 0:
            ax.text(profit, y, f" {profit:,.0f} ", va="center", ha="right", fontsize=7.5, color=TEXT_PRIMARY)
    ax.margins(x=0.12)
    ax.legend(
        handles=[Patch(color=PROFIT_COLOR, label="Profit"), Patch(color=LOSS_COLOR, label="Loss")],
        loc="lower right", frameon=False, fontsize=8, labelcolor=TEXT_SECONDARY,
    )
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", facecolor=SURFACE)
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def summarise(df: pd.DataFrame) -> dict[str, Any]:
    """Fleet-level KPIs shown at the top of the report."""
    earnings, cost = float(df["earnings"].sum()), float(df["total_cost"].sum())
    return {
        "vehicles": len(df),
        "trips": int(df["trips_completed"].sum()),
        "earnings": earnings,
        "total_cost": cost,
        "profit": earnings - cost,
        "margin": (earnings - cost) / earnings if earnings else None,
        "unprofitable": int(df["is_unprofitable"].sum()),
        "trending": int(df["trending_unprofitable"].sum()),
    }


def generate_report(
    df: pd.DataFrame,
    sim_date: date,
    reports_dir: Path,
    quality: dict[str, Any] | None = None,
    margin_threshold: float = 0.10,
) -> tuple[Path, Path]:
    """Write profitability_<date>.html and .csv; returns both paths.

    `quality` holds data-quality facts (cleaning counts, duplicates removed,
    speed-vs-batch earnings) displayed in the report footer section.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    html_path = reports_dir / f"profitability_{sim_date.isoformat()}.html"
    csv_path = reports_dir / f"profitability_{sim_date.isoformat()}.csv"

    df = df.sort_values(["is_unprofitable", "profit"], ascending=[False, True]).reset_index(drop=True)
    df.to_csv(csv_path, index=False)

    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape(["html", "j2"]))
    env.filters["money"] = lambda v: "-" if v is None or pd.isna(v) else f"{v:,.0f}"
    env.filters["pct"] = lambda v: "-" if v is None or pd.isna(v) else f"{v * 100:.1f}%"
    html = env.get_template("profitability_report.html.j2").render(
        sim_date=sim_date.isoformat(),
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        summary=summarise(df),
        rows=df.to_dict("records"),
        chart_png=profit_chart_png(df),
        quality=quality or {},
        margin_threshold=margin_threshold,
    )
    html_path.write_text(html, encoding="utf-8")
    return html_path, csv_path
