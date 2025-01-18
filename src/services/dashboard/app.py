from dash.dependencies import Input, Output
from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd

from tools import get_data_means


app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dashboard Hurtowni Danych"),
    html.Div(
        "Wizualizacja wyników Speed Layer'a i statystyk.",
        style={"marginBottom": "20px"}
        ),
    dcc.Interval(
        id="interval-component",
        interval=5000,  # Odświeżanie co 5 sekund
        n_intervals=0
    ),
    dcc.Graph(id="real-time-graph"),
    html.Div(id="stats-output", style={"marginTop": "20px"})
])


@app.callback(
    Output("real-time-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph(n):
    means = get_data_means()
    if not means:
        return px.scatter(title="Brak danych w hurtowni")

    df = pd.DataFrame([means])
    df.reset_index(inplace=True)

    fig = px.bar(
        df,
        x="index",
        y=df.columns[1],
        title="Średnie wartości dla ostatnich dni",
        labels={"index": "Parametr", df.columns[1]: "Średnia"}
    )
    return fig


@app.callback(
    Output("stats-output", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_stats(n):
    means = get_data_means()
    if not means:
        return "Brak danych do wyświetlenia."

    stats_df = pd.DataFrame(
        list(means.items()),
        columns=["Parametr", "Wartość"]
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
