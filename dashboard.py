"""
dashboard.py
Interactive Panel dashboard for U.S. Housing Affordability.

Just run:  python dashboard.py
Automatically runs clean_merge.py first if processed data is missing.
Opens on http://localhost:5006
"""

import os
import importlib.util
import pandas as pd
import panel as pn
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

pn.extension("plotly", sizing_mode="stretch_width")

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "processed")


# ── Load data ─────────────────────────────────────────────────────────────────

def _run_clean_merge():
    """Run clean_merge.main() from the same directory as this script."""
    here = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location(
        "clean_merge", os.path.join(here, "clean_merge.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


def load_data():
    """Load processed parquet files, running clean_merge first if missing.

    Returns:
        Tuple of (panel_df, hud_df, fred_df, zhvi_df, zori_df).
    """
    if not os.path.exists(os.path.join(OUT_DIR, "panel.parquet")):
        print("Processed data not found — running clean_merge.py first...")
        _run_clean_merge()
        print("Pipeline complete.\n")

    panel = pd.read_parquet(os.path.join(OUT_DIR, "panel.parquet"))
    hud   = pd.read_parquet(os.path.join(OUT_DIR, "hud.parquet"))
    fred  = pd.read_parquet(os.path.join(OUT_DIR, "fred.parquet"))
    zhvi  = pd.read_parquet(os.path.join(OUT_DIR, "zhvi.parquet"))
    zori  = pd.read_parquet(os.path.join(OUT_DIR, "zori.parquet"))
    return panel, hud, fred, zhvi, zori


panel_df, hud_df, fred_df, zhvi_df, zori_df = load_data()

YEARS         = sorted(panel_df["year"].dropna().unique().tolist())
STATES        = ["All"] + sorted(panel_df["state_name"].dropna().unique().tolist())
ZILLOW_METROS = sorted(zhvi_df["RegionName"].dropna().unique().tolist())


# ── Widgets ───────────────────────────────────────────────────────────────────

year_slider = pn.widgets.IntSlider(
    name="Year", value=max(YEARS), start=min(YEARS), end=max(YEARS)
)
state_select = pn.widgets.Select(
    name="State", options=STATES, value="All", width=200
)
top_n_slider = pn.widgets.IntSlider(
    name="Top N Counties", value=15, start=5, end=40, step=5
)
metro_picker = pn.widgets.MultiChoice(
    name="Metros (Zillow chart)",
    options=ZILLOW_METROS[:50],
    value=ZILLOW_METROS[:6],
    width=300,
)


# ── Chart functions ───────────────────────────────────────────────────────────

def metric_cards(year):
    """Render headline KPI cards for the selected year.

    Args:
        year: Selected year integer.

    Returns:
        Panel Row containing styled metric cards.
    """
    df = panel_df[panel_df["year"] == year]

    def card(label, value, color):
        return pn.pane.HTML(
            '<div style="background:#1e1e2e;border-left:4px solid ' + color + ';'
            'border-radius:6px;padding:12px 16px;min-width:150px">'
            '<div style="color:#aaa;font-size:11px">' + label + '</div>'
            '<div style="color:#fff;font-size:20px;font-weight:700">' + value + '</div>'
            '</div>',
            sizing_mode="stretch_width",
        )

    med_income   = df["median_household_income"].median()
    med_rent     = df["median_gross_rent"].median()
    burden       = df["rent_burden"].median()
    rent_gap     = df["rent_gap"].median()
    pct_burdened = (df["rent_burden"] > 0.30).mean() * 100

    return pn.Row(
        card("Median Income",           f"${med_income:,.0f}"    if pd.notna(med_income) else "—", "#2ecc71"),
        card("Median Rent/mo",          f"${med_rent:,.0f}"      if pd.notna(med_rent)   else "—", "#e67e22"),
        card("Median Rent Burden",      f"{burden:.1%}"           if pd.notna(burden)     else "—", "#e74c3c"),
        card("Avg Rent Gap",            f"${rent_gap:+,.0f}/mo"   if pd.notna(rent_gap)  else "—", "#9b59b6"),
        card("% Counties >30% Burden",  f"{pct_burdened:.1f}%",                                     "#c0392b"),
        sizing_mode="stretch_width",
    )


def animated_scatter():
    """Animated scatter: median rent vs income by county, animated over years.

    Returns:
        Panel Plotly pane with animated figure.
    """
    df = panel_df.dropna(subset=["median_household_income", "median_gross_rent", "year"])
    df = df[df["median_household_income"] > 0].copy()
    df["year_str"] = df["year"].astype(str)

    x_max = df["median_household_income"].quantile(0.99)
    y_max = df["median_gross_rent"].quantile(0.99)

    fig = px.scatter(
        df,
        x="median_household_income",
        y="median_gross_rent",
        animation_frame="year_str",
        color="rent_burden",
        color_continuous_scale="RdYlGn_r",
        size="population",
        size_max=20,
        hover_name="county_name",
        range_x=[0, x_max],
        range_y=[0, y_max],
        labels={
            "median_household_income": "Median Household Income ($)",
            "median_gross_rent":       "Median Gross Rent ($/mo)",
            "rent_burden":             "Rent Burden",
        },
        title="Rent vs Income by County — Animated by Year",
    )
    fig.add_shape(
        type="line", x0=0, y0=0, x1=x_max, y1=x_max / 12 * 0.30,
        line=dict(color="navy", dash="dash", width=2)
    )
    fig.add_annotation(
        x=x_max * 0.75, y=(x_max / 12 * 0.30) * 1.1,
        text="30% threshold", showarrow=False,
        font=dict(color="navy", size=11)
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=50, b=10, l=10, r=10),
    )
    fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 800
    return pn.pane.Plotly(fig, height=480)


def rent_gap_bar(year, state, top_n):
    """Horizontal bar chart of top counties by monthly rent gap.

    Args:
        year:   Selected year.
        state:  State name or 'All'.
        top_n:  Number of counties to show.

    Returns:
        Panel Plotly pane.
    """
    df = panel_df[panel_df["year"] == year].copy()
    if state != "All":
        df = df[df["state_name"] == state]
    df = df.dropna(subset=["rent_gap", "county_name"]).nlargest(top_n, "rent_gap")

    fig = px.bar(
        df.sort_values("rent_gap"),
        x="rent_gap",
        y="county_name",
        orientation="h",
        color="rent_gap",
        color_continuous_scale="RdYlGn_r",
        hover_data={"median_gross_rent": ":$,.0f", "median_household_income": ":$,.0f"},
        labels={"rent_gap": "Monthly Rent Gap ($)", "county_name": "County"},
        title=f"Top {top_n} Counties — Rent Exceeds 30% Threshold ({year})",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False, margin=dict(t=50, b=10, l=10, r=10),
        height=max(350, top_n * 22),
    )
    return pn.pane.Plotly(fig, height=max(350, top_n * 22))


def fred_chart():
    """Dual-axis line chart: 30-yr mortgage rate vs housing starts.

    Returns:
        Panel Plotly pane.
    """
    df = fred_df.dropna(subset=["mortgage_rate_30yr"])
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=df["date"], y=df["mortgage_rate_30yr"],
                   name="30-Yr Mortgage Rate (%)",
                   line=dict(color="#e74c3c", width=2), mode="lines"),
        secondary_y=False,
    )
    if "housing_starts_thousands" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["housing_starts_thousands"],
                       name="Housing Starts (000s)",
                       line=dict(color="#3498db", width=2, dash="dot"), mode="lines"),
            secondary_y=True,
        )

    fig.update_yaxes(title_text="Mortgage Rate (%)", secondary_y=False, color="#e74c3c")
    fig.update_yaxes(title_text="Housing Starts (000s)", secondary_y=True, color="#3498db")
    fig.update_layout(
        title="Mortgage Rate vs Housing Starts (FRED)",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.2),
        margin=dict(t=50, b=10, l=10, r=10),
    )
    return pn.pane.Plotly(fig, height=380)


def hud_chart(top_n=20):
    """Bar chart of metro areas with the highest 2-bedroom Fair Market Rents.

    Args:
        top_n: Number of metro areas to show.

    Returns:
        Panel Plotly pane.
    """
    df = hud_df.nlargest(top_n, "fmr_2bdr").sort_values("fmr_2bdr")

    fig = px.bar(
        df,
        x="fmr_2bdr",
        y="fmr_areaname",
        orientation="h",
        color="fmr_2bdr",
        color_continuous_scale="Reds",
        labels={"fmr_2bdr": "2-BR Fair Market Rent ($/mo)", "fmr_areaname": "Metro Area"},
        title=f"Top {top_n} Metro Areas by HUD 2-BR Fair Market Rent",
        hover_data={"fmr_1bdr": ":$,.0f", "fmr_3bdr": ":$,.0f"},
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False, margin=dict(t=50, b=10, l=10, r=10),
        height=500,
    )
    return pn.pane.Plotly(fig, height=520)


def zillow_chart(metros):
    """Side-by-side ZHVI and ZORI line charts for selected metros.

    Args:
        metros: List of metro region names to display.

    Returns:
        Panel Plotly pane.
    """
    if not metros:
        return pn.pane.HTML("<p style='color:#888'>Select metros in the sidebar.</p>")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Home Value Index (ZHVI)", "Rent Index (ZORI)")
    )
    colors = px.colors.qualitative.Plotly

    for i, metro in enumerate(metros):
        color = colors[i % len(colors)]
        sub_z = zhvi_df[zhvi_df["RegionName"] == metro]
        sub_r = zori_df[zori_df["RegionName"] == metro]

        fig.add_trace(
            go.Scatter(x=sub_z["year"], y=sub_z["zhvi"], name=metro,
                       line=dict(color=color), legendgroup=metro,
                       mode="lines+markers"),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=sub_r["year"], y=sub_r["zori"], name=metro,
                       line=dict(color=color), legendgroup=metro,
                       showlegend=False, mode="lines+markers"),
            row=1, col=2,
        )

    fig.update_layout(
        title="Zillow Home Value & Rent Index by Metro",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.2),
        margin=dict(t=60, b=10, l=10, r=10),
        height=380,
    )
    return pn.pane.Plotly(fig, height=400)


# ── Reactive sections ─────────────────────────────────────────────────────────

@pn.depends(year_slider, state_select, top_n_slider)
def dynamic_section(year, state, top_n):
    """Re-render metric cards and bar chart on widget change.

    Args:
        year:   Year from slider.
        state:  State from dropdown.
        top_n:  Top-N from slider.

    Returns:
        Panel Column with cards and bar chart.
    """
    return pn.Column(
        metric_cards(year),
        pn.layout.Divider(),
        pn.pane.Markdown("### Top Counties by Rent Affordability Gap"),
        rent_gap_bar(year, state, top_n),
    )


@pn.depends(metro_picker)
def dynamic_zillow(metros):
    """Re-render Zillow chart on metro selection change.

    Args:
        metros: List of selected metro names.

    Returns:
        Panel Plotly pane.
    """
    return zillow_chart(metros)


# ── Layout ────────────────────────────────────────────────────────────────────

sidebar = pn.Column(
    pn.pane.Markdown("### Controls"),
    pn.layout.Divider(),
    year_slider,
    state_select,
    top_n_slider,
    pn.layout.Divider(),
    metro_picker,
    pn.layout.Divider(),
    pn.pane.Markdown(
        "_Sources: Census ACS5, FRED, HUD FMR, Zillow_",
        styles={"font-size": "11px", "color": "#888"},
    ),
    width=260,
)

main_content = pn.Column(
    pn.pane.Markdown("## Headline Metrics"),
    dynamic_section,
    pn.layout.Divider(),
    pn.pane.Markdown(
        "## Rent vs Income — All Counties (Animated)\n"
        "_Press **Play** to animate across years. "
        "Dashed line = 30% affordability threshold. "
        "Bubble size = population._"
    ),
    animated_scatter(),
    pn.layout.Divider(),
    pn.Row(
        pn.Column(
            pn.pane.Markdown("### Mortgage Rate & Housing Starts (FRED)"),
            fred_chart(),
        ),
        pn.Column(
            pn.pane.Markdown("### Zillow Home Value & Rent Index"),
            dynamic_zillow,
        ),
    ),
    pn.layout.Divider(),
    pn.pane.Markdown("### HUD Fair Market Rents — Top Metro Areas"),
    hud_chart(top_n=20),
)

template = pn.template.FastListTemplate(
    title="U.S. Housing Affordability Dashboard",
    sidebar=[sidebar],
    main=[main_content],
    accent="#2c3e50",
)

if __name__ == "__main__":
    template.servable()
    pn.serve(template, port=5006, show=True)