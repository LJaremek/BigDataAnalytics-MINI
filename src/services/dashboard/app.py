from dash.dependencies import Input, Output
from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd

from tools import get_data_means
from tools_agregator import get_mongo_news, get_predicted_and_real_open

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Data Warehouse Dashboard"),
    html.Div(
        "Visualization of Speed Layer results and statistics.",
        style={"marginBottom": "20px"}
    ),
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
                "marginLeft": "4%"
            }
        ),
    ], style={
        "display": "flex", "justifyContent": "space-between",
        "marginTop": "20px"
    }),
    html.Div([
        html.Div(
            id="stats-output",
            style={
                "width": "30%",
                "display": "inline-block",
                "verticalAlign": "top",
                "marginRight": "2%"
            }
        ),
        dcc.Graph(
            id="predicted-vs-real-chart",
            style={
                "height": "400px", "width": "68%",
                "display": "inline-block"
            }
        )
    ], style={
        "display": "flex",
        "marginTop": "20px"
    })
])


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
        title="Number of news records over time",
        labels={
            "record_date": "Date",
            "count": "Number of records",
            "source": "Source"
        }
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
    app.run_server(debug=True, host="0.0.0.0")
