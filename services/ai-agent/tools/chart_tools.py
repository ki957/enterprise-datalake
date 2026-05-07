"""
Chart generation tool for the AI agents.

Creates a Plotly chart from structured data and returns it as an embedded
fenced block. The Streamlit app detects this block and renders the chart
inline in the chat message.

Embedded block format (detected by render_response() in app.py):
    ```plotly
    { ...Plotly figure JSON... }
    ```

Usage contract:
  - Agents call query_clickhouse → get a markdown table
  - Agent reads the table, extracts labels (first column) and values (numeric column)
  - Agent calls create_chart with those arrays + chart type + title
  - The returned block is included in the agent's final response text
  - app.py splits on the block and renders it as a st.plotly_chart()
"""

import json

import plotly.graph_objects as go
from langchain_core.tools import tool


def _parse_list(v):
    """Accept a Python list, a JSON array string, or a comma-separated string."""
    if isinstance(v, list):
        return v
    s = str(v).strip()
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return [x.strip() for x in s.split(",")]


@tool
def create_chart(
    chart_type: str,
    labels: str,
    values: str,
    title: str,
    x_label: str = "",
    y_label: str = "",
) -> str:
    """Create a Plotly chart from query data. Returns an embeddable chart block.

    chart_type — "bar" | "line" | "area" | "pie"
    labels     — JSON array string or comma-separated labels, e.g. '["Jan","Feb"]' or 'Jan,Feb'
    values     — JSON array string or comma-separated numbers, e.g. '[100,200]' or '100,200'
    title      — chart title

    Use after query_clickhouse. Extract the first column as labels and the
    main numeric column as values from the returned markdown table.
    """
    try:
        labs = [str(l) for l in _parse_list(labels)]
        vals = [float(str(v).replace(",", "")) for v in _parse_list(values)]

        fig = go.Figure()

        if chart_type == "bar":
            fig.add_trace(go.Bar(
                x=labs,
                y=vals,
                text=[f"{v:,.0f}" if v >= 100 else f"{v:.2f}" for v in vals],
                textposition="outside",
                marker=dict(
                    color=vals,
                    colorscale="Blues",
                    showscale=False,
                ),
                cliponaxis=False,
            ))
            fig.update_layout(xaxis_tickangle=-35)

        elif chart_type == "line":
            fig.add_trace(go.Scatter(
                x=labs,
                y=vals,
                mode="lines+markers",
                line=dict(color="#2ca02c", width=2.5),
                marker=dict(size=8, color="#2ca02c", symbol="circle"),
            ))

        elif chart_type == "area":
            fig.add_trace(go.Scatter(
                x=labs,
                y=vals,
                fill="tozeroy",
                mode="lines",
                line=dict(color="#4C72B0", width=2),
                fillcolor="rgba(76,114,176,0.18)",
            ))

        elif chart_type == "pie":
            fig.add_trace(go.Pie(
                labels=labs,
                values=vals,
                textinfo="percent+label",
                hole=0.38,
                marker=dict(colors=[
                    "#4C72B0", "#DD8452", "#55A868", "#C44E52",
                    "#8172B3", "#937860", "#DA8BC3",
                ]),
            ))

        else:
            return (
                f"Unknown chart_type '{chart_type}'. "
                "Valid options: bar, line, area, pie."
            )

        fig.update_layout(
            title=dict(text=title, font=dict(size=15, color="#1a1a2e"), x=0),
            xaxis_title=x_label,
            yaxis_title=y_label,
            template="plotly_white",
            height=390,
            margin=dict(l=55, r=30, t=65, b=75),
            font=dict(family="Inter, Arial, sans-serif", size=12),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        )

        return f"```plotly\n{fig.to_json()}\n```"

    except Exception as e:
        return f"Chart generation failed — {e}. Data table shown above instead."
