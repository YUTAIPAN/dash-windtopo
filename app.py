import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, ctx
import boto3
from io import StringIO
import os

# 用環境變數來存取憑證（Render 上會設定）
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

def load_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))

# 實際使用
BUCKET = "windtopo-visualization"

loc_df = load_csv_from_s3(BUCKET, "ARC.JP_pacific.tbl")
loc_df = loc_df[["HEAD:ID", "LATD", "LOND"]].rename(columns={"HEAD:ID": "ID"})

data_df = load_csv_from_s3(BUCKET, "2024_2025_MSM_WT_ARC_small.csv")

# 計算 >= 25 m/s 的出現頻率
def calc_freq(group, column):
    return (group[column] >= 25).sum() / len(group)

freq_df = data_df.groupby("ID").apply(
    lambda g: pd.Series({
        "obs_freq": calc_freq(g, "ObsGustSpd1h"),
        "wt_freq": calc_freq(g, "wt_operation"),
        "arc_freq": calc_freq(g, "arc_gust_pred")
    })
).reset_index()

plot_df = freq_df.merge(loc_df, on="ID", how="left")
high_freq_df = plot_df[plot_df["obs_freq"] >= 0.004]
low_freq_df = plot_df[plot_df["obs_freq"] < 0.004]

# 案例定義
cases = {
    "Case 1: 2025/01/29 - 2025/02/02": ("2025-01-29", "2025-02-02"),
    "Case 2: 2025/02/13 - 2025/02/15": ("2025-02-13", "2025-02-15"),
    "Case 3: 2025/02/18 - 2025/02/20": ("2025-02-18", "2025-02-20"),
    "Case 4: 2025/03/13 - 2025/03/18": ("2025-03-13", "2025-03-18"),
    "Case 5: 2025/03/25 - 2025/03/28": ("2025-03-25", "2025-03-28"),
}

# 建立 Dash app
app = Dash(__name__)
app.title = "Wind Timeseries Viewer"

# 地圖圖層
def create_map(selected_ids):
    map_fig = go.Figure()
    for df, color, name in [(low_freq_df, 'blue', 'Low Frequency'), (high_freq_df, 'red', 'High Frequency')]:
        opacities = [1.0 if sid in selected_ids else 0.4 for sid in df["ID"]]
        map_fig.add_trace(go.Scattermapbox(
            lat=df["LATD"],
            lon=df["LOND"],
            mode='markers',
            marker=dict(size=8, color=color, opacity=opacities),
            text=df["ID"],
            name=name,
            hoverinfo="text"
        ))

    map_fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=7,
        mapbox_center={"lat": loc_df["LATD"].mean(), "lon": loc_df["LOND"].mean()},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        clickmode='event+select',
        dragmode='pan'
    )
    return map_fig

# Layout
app.layout = html.Div([
    html.H1("Wind Observation Interactive Viewer", style={"textAlign": "center"}),
    html.Div([
        html.Div([
            dcc.Graph(id='station-map', config={
                "scrollZoom": True,
                "displayModeBar": False
            }),
            dcc.Store(id='map-state')  # 儲存地圖視角用
        ], style={'width': '35%', 'display': 'inline-block', 'verticalAlign': 'top'}),
        html.Div([
            dcc.Dropdown(
                id='case-selector',
                options=[{"label": k, "value": k} for k in cases],
                value=list(cases.keys())[0],
                clearable=False,
                style={"margin-bottom": "10px"}
            ),
            html.Button("Reset", id='reset-button', n_clicks=0, style={"margin-bottom": "10px"}),
            dcc.Store(id='selected-station-ids', data=[]),
            html.Div(id='timeseries-container', style={'height': '800px', 'overflowY': 'scroll'})
        ], style={'width': '63%', 'display': 'inline-block', 'verticalAlign': 'top'})
    ])
])

# 更新地圖
@app.callback(
    Output('station-map', 'figure'),
    Input('selected-station-ids', 'data'),
    State('map-state', 'data')
)
def update_map_figure(selected_ids, map_state):
    map_fig = create_map(selected_ids)
    if map_state and 'center' in map_state and 'zoom' in map_state:
        map_fig.update_layout(
            mapbox_center=map_state['center'],
            mapbox_zoom=map_state['zoom']
        )
    return map_fig

@app.callback(
    Output('map-state', 'data'),
    Input('station-map', 'relayoutData'),
    State('map-state', 'data')
)
def save_map_state(relayout_data, current_state):
    if not relayout_data:
        return current_state

    # 如果 current_state 是 None，初始化為空字典
    if current_state is None:
        current_state = {}

    zoom = relayout_data.get('mapbox.zoom', current_state.get('zoom', 7))
    center = relayout_data.get('mapbox.center', current_state.get('center', {
        "lat": loc_df["LATD"].mean(),
        "lon": loc_df["LOND"].mean()
    }))

    return {"zoom": zoom, "center": center}

# 點地圖 → 更新選取站點
@app.callback(
    Output('selected-station-ids', 'data'),
    Input('station-map', 'clickData'),
    Input('reset-button', 'n_clicks'),
    State('selected-station-ids', 'data')
)
def update_station_list(clickData, reset_clicks, selected_ids):
    if ctx.triggered_id == 'reset-button':
        return []
    if not clickData or 'points' not in clickData:
        return selected_ids
    clicked_id = clickData['points'][0]['text']
    if clicked_id not in selected_ids:
        selected_ids.append(clicked_id)
    return selected_ids

# 根據選取站點與案例產生圖
@app.callback(
    Output('timeseries-container', 'children'),
    Input('selected-station-ids', 'data'),
    Input('case-selector', 'value')
)
def update_timeseries(station_ids, selected_case):
    if not station_ids:
        return html.Div("地点を選んでください")

    start_date, end_date = cases[selected_case]
    children = []

    for point_id in station_ids:
        site_df = data_df[data_df["ID"] == point_id]
        site_df = site_df[(site_df["VALIDTIME"] >= start_date) & (site_df["VALIDTIME"] <= end_date)]
        site_df = site_df[site_df["ft"].between(0, 23)]
        site_df = site_df.sort_values("VALIDTIME")

        if site_df.empty:
            fig = go.Figure().update_layout(
                title=f"No data for {point_id} during {selected_case}",
                height=400, width=900
            )
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=site_df["VALIDTIME"], y=site_df["ObsGustSpd1h"],
                                     mode='lines', name='Obs', line=dict(color='black')))
            fig.add_trace(go.Scatter(x=site_df["VALIDTIME"], y=site_df["wt_operation"],
                                     mode='lines', name='WT', line=dict(color='red', dash='dash')))
            fig.add_trace(go.Scatter(x=site_df["VALIDTIME"], y=site_df["arc_gust_pred"],
                                     mode='lines', name='ARC', line=dict(color='blue', dash='dot')))
            # 加上 Y=20, 25 的綠色虛線
            for threshold in [20, 25]:
                fig.add_shape(
                    type="line",
                    x0=site_df["VALIDTIME"].min(),
                    x1=site_df["VALIDTIME"].max(),
                    y0=threshold,
                    y1=threshold,
                    line=dict(color="green", width=1, dash="dash"),
                )
            fig.update_layout(
                title=f"{point_id}",
                xaxis_title="Time", yaxis_title="Wind Speed (m/s)",
                height=400, width=900
            )

        children.append(dcc.Graph(figure=fig, style={'margin-bottom': '20px'}))

    return children

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=True, host="0.0.0.0", port=port)
