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


@tool
def create_chart(
    chart_type: str,
    labels: list,
    values: list,
    title: str,
    x_label: str = "",
    y_label: str = "",
) -> str:
    """Create a Plotly chart from query data. Returns an embeddable chart block.

    chart_type — "bar" | "line" | "area" | "pie"
    labels     — list of label strings, e.g. ["Jan", "Feb", "Mar"]
    values     — list of numeric values, e.g. [100.0, 200.0, 150.0]
    title      — chart title

    Use after query_clickhouse. Pass the first column as labels and the main
    numeric column as values directly as Python lists.
    """
    try:
        labs = [str(l) for l in labels]
        vals = [float(str(v).replace(",", "")) for v in values]

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
