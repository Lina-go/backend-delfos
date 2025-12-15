"""Example: build a styled Plotly chart and upload HTML/PNG to Azure Blob."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import plotly.express as px

from src.config.settings import get_settings
from src.infrastructure.storage.blob_client import BlobStorageClient

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
HTML_NAME = "oceania_life_expectancy.html"
PNG_NAME = "oceania_life_expectancy.png"


def build_chart():
    """Create a nicer-looking Oceania life expectancy line chart."""
    df = px.data.gapminder().query("continent == 'Oceania'")
    fig = px.line(
        df,
        x="year",
        y="lifeExp",
        color="country",
        symbol="country",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(line={"width": 3})
    fig.update_layout(
        title="Oceania Life Expectancy Over Time",
        template="plotly_white",
        hovermode="x unified",
        legend_title_text="Country",
        xaxis_title="Year",
        yaxis_title="Life Expectancy",
        font={"family": "Inter, Arial, sans-serif", "size": 14},
        margin={"l": 60, "r": 30, "t": 70, "b": 50},
    )
    fig.update_xaxes(
        showgrid=True, gridcolor="rgba(0,0,0,0.05)", showspikes=True, spikemode="across"
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)", zeroline=False)
    return fig


def save_outputs(fig):
    """Render the chart to local HTML and PNG files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    html_path = OUTPUT_DIR / HTML_NAME
    png_path = OUTPUT_DIR / PNG_NAME

    html_content = fig.to_html(full_html=True, include_plotlyjs="cdn")
    png_bytes = fig.to_image(format="png", width=1100, height=650, scale=2)

    html_path.write_text(html_content, encoding="utf-8")
    png_path.write_bytes(png_bytes)
    return html_path, png_path, html_content.encode("utf-8"), png_bytes


async def upload_to_blob(html_bytes: bytes, png_bytes: bytes):
    """Upload HTML and PNG bytes to Azure Blob Storage."""
    settings = get_settings()
    client = BlobStorageClient(settings)
    container = settings.azure_storage_container_name or "charts"

    html_url = await client.upload_blob(
        container_name=container,
        blob_name=HTML_NAME,
        data=html_bytes,
        content_type="text/html",
    )
    png_url = await client.upload_blob(
        container_name=container,
        blob_name=PNG_NAME,
        data=png_bytes,
        content_type="image/png",
    )
    return html_url, png_url


async def main():
    logging.basicConfig(level=logging.INFO)
    fig = build_chart()
    html_path, png_path, html_bytes, png_bytes = save_outputs(fig)
    logger.info("Saved chart locally: %s , %s", html_path, png_path)

    try:
        html_url, png_url = await upload_to_blob(html_bytes, png_bytes)
        logger.info("Uploaded HTML to: %s", html_url)
        logger.info("Uploaded PNG  to: %s", png_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Upload skipped or failed: %s", exc)


if __name__ == "__main__":
    asyncio.run(main())
