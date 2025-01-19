from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd

from tools import get_data_means
from tools_agregator import get_mongo_news, get_predicted_and_real_open, get_mongo_weather

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Data Warehouse Dashboard"),
    html.Div(
        "Visualization of Speed Layer results and statistics.",
        style={"marginBottom": "20px"}
    ),
    html.Hr(style={"border": "1px solid black", "marginBottom": "20px"}),
    dcc.Interval(
        id="interval-component",
        interval=5000,  # Refresh every 5 seconds
        n_intervals=0
    ),
    html.Div([
        dcc.Graph(
            id="news-line-chart",
            style={
                "height": "300px", "width": "48%", "display": "inline-block",
            }
        ),
        html.Div([
            dcc.Graph(
                id="weather-chart",
                style={
                    "height": "300px", "width": "100%"
                }
            ),
            dcc.Graph(
                id="rainfall-chart",
                style={
                    "height": "200px", "width": "100%", "marginTop": "20px"
                }
            )
        ], style={
            "width": "48%",
            "display": "inline-block"
        })
    ], style={
        "display": "flex",
        "justifyContent": "space-between",
        "marginTop": "20px"
    }),
    html.Div([
        html.Div(
            id="stats-output",
            style={
                "width": "30%",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center",
                "alignItems": "center",
                "marginRight": "2%",
                "height": "400px"
            }
        ),
        dcc.Graph(
            id="predicted-vs-real-chart",
            style={
                "height": "400px",
                "width": "68%",
                "display": "inline-block"
            }
        )
    ], style={
        "display": "flex",
        "alignItems": "center",
        "marginTop": "20px"
    })
])


@app.callback(
    Output("rainfall-chart", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_rainfall_chart(n):
    weather_data = get_mongo_weather()
    if weather_data.empty:
        return px.scatter(title="No data for Rainfall")

    fig = px.line(
        weather_data,
        x="record_date",
        y="rain",
        title="Rainfall Over Time",
        labels={
            "record_date": "Date",
            "rain": "Rainfall (mm)"
        },
        line_shape="linear"
    )

    fig.update_layout(
        margin=dict(l=40, r=40, t=40, b=40),
        height=300
    )

    return fig


@app.callback(
    Output("weather-chart", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_weather_chart(n):
    weather_data = get_mongo_weather()
    if weather_data.empty:
        return px.scatter(title="No data for Weather")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    weather_data["sun"] = weather_data["sun"] / 60

    fig.add_trace(
        go.Scatter(
            x=weather_data["record_date"],
            y=weather_data["sun"],
            mode="lines",
            name="Sun",
            line=dict(color="orange")
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=weather_data["record_date"],
            y=weather_data["temperature"],
            mode="lines",
            name="Temperature",
            line=dict(color="red")
        ),
        secondary_y=True
    )

    fig.update_yaxes(
        title_text="Sunlight (minutes)",
        secondary_y=False,
        showgrid=True,
        zeroline=True
    )
    fig.update_yaxes(
        title_text="Temperature (°C)",
        secondary_y=True,
        showgrid=False
    )

    fig.update_xaxes(title_text="Date")

    fig.update_layout(
        title="Weather Data Over Time",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.7
            ),
        margin=dict(l=40, r=40, t=40, b=40)
    )

    return fig


@app.callback(
    Output("news-line-chart", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_news_chart(n):
    news_data = get_mongo_news()
    if news_data.empty:
        return px.scatter(title="No data for 'news' category")

    fig = px.line(
        news_data,
        x="record_date",
        y="count",
        color="source",
        # title="Number of news records over time",
        labels={
            "record_date": "Date",
            "count": "Number of records",
            "source": "Source"
        }
    )
    fig.update_layout(
        title="Number of news records over time",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.7
            ),
        margin=dict(l=40, r=40, t=40, b=40),
        height=300
    )
    return fig


@app.callback(
    Output("predicted-vs-real-chart", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_predicted_vs_real_chart(n):
    data = get_predicted_and_real_open()
    if data.empty:
        return px.scatter(title="No data for Predicted vs Real")

    melted_data = pd.melt(
        data,
        id_vars="record_date",
        value_vars=["predicted_open", "real_open"],
        var_name="Type",
        value_name="Value"
    )

    fig = px.line(
        melted_data,
        x="record_date",
        y="Value",
        color="Type",
        title="Predicted vs Real Open Values Over Time",
        labels={
            "record_date": "Date",
            "Value": "Open Value",
            "Type": "Data Type"
        }
    )
    return fig


@app.callback(
    Output("stats-output", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_stats(n):
    means = get_data_means()
    if not means:
        return "No data to display."

    stats_df = pd.DataFrame(
        list(means.items()),
        columns=["Parameter", "Value"]
    )

    return html.Table([
        html.Thead(html.Tr([html.Th(col) for col in stats_df.columns])),
        html.Tbody([
            html.Tr([
                html.Td(stats_df.iloc[i][col])
                for col in stats_df.columns
            ])
            for i in range(len(stats_df))
        ])
    ])


if __name__ == "__main__":
    app.run_server(debug=False, dev_tools_ui=False, host="0.0.0.0")
