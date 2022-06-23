from datetime import date, datetime

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

st.set_page_config(page_icon="📥", page_title="Download App")


def icon(emoji: str):
    """Shows an emoji as a Notion-style page icon."""
    st.write(
        f'<span style="font-size: 78px; line-height: 1">{emoji}</span>',
        unsafe_allow_html=True,
    )


# Share the connector across all users connected to the app
@st.experimental_singleton()
def get_connector():
    """Create a connector using credentials filled in Streamlit secrets"""
    credentials = Credentials.from_service_account_info(st.secrets["bigquery"])
    connector = bigquery.Client(credentials=credentials)
    return connector


@st.experimental_memo(ttl=24 * 60 * 60)
def get_data_frame_from_raw_sql(_connector, query: str) -> pd.DataFrame:
    return _connector.query(query).to_dataframe()


big_query_connector = get_connector()
get_data_frame_from_raw_sql(big_query_connector, "SELECT 'foo'")


def monthly_downloads(start_date):
    df = get_data_frame_from_raw_sql(
        big_query_connector,
        f"""
        SELECT
            date_trunc(date, MONTH) as date,
            project,
            SUM(downloads) as downloads
        FROM streamlit.streamlit.pypi_downloads
        WHERE date >= '{start_date}'
            AND project IN ('pandas', 'keras', 'torch', 'tensorflow', 'numpy', 'sci-kit learn')
        GROUP BY 1,2
        ORDER BY 1,2 ASC
        """,
    )

    # Percentage difference (between 0-1) of downloads of current vs previous month
    df["delta"] = (df.groupby(["project"])["downloads"].pct_change()).fillna(0)
    # BigQuery returns the date column as type dbdate, which is not supported by Altair/Vegalite
    df["date"] = df["date"].astype("datetime64")

    return df


def weekly_downloads(start_date):
    df = get_data_frame_from_raw_sql(
        big_query_connector,
        f"""
        SELECT
            date_trunc(date, WEEK) as date,
            project,
            SUM(downloads) as downloads
        FROM streamlit.streamlit.pypi_downloads
        WHERE date >= '{start_date}'
            AND project IN ('pandas', 'keras', 'torch', 'tensorflow', 'numpy', 'sci-kit learn')
        GROUP BY 1,2
        HAVING date_diff(CURRENT_DATE(), max(date_trunc(date, WEEK)), DAY) >=7
        ORDER BY 1,2 ASC
        """,
    )
    # Percentage difference (between 0-1) of downloads of current vs previous month
    df["delta"] = (df.groupby(["project"])["downloads"].pct_change()).fillna(0)
    # BigQuery returns the date column as type dbdate, which is not supported by Altair/Vegalite
    df["date"] = df["date"].astype("datetime64")

    return df


def plot_all_downloads(
    source, x="date", y="downloads", group="project", axis_scale="linear"
):

    if st.checkbox("View logarithmic scale"):
        axis_scale = "log"

    brush = alt.selection_interval(encodings=["x"], empty="all")

    click = alt.selection_multi(encodings=["color"])

    lines = (
        (
            alt.Chart(source)
            .mark_line(point=True)
            .encode(
                x=x,
                y=alt.Y("downloads", scale=alt.Scale(type=f"{axis_scale}")),
                color=group,
                tooltip=[
                    "date",
                    "project",
                    "downloads",
                    alt.Tooltip("delta", format=".2%"),
                ],
            )
        )
        .add_selection(brush)
        .properties(width=550)
        .transform_filter(click)
    )

    bars = (
        alt.Chart(source)
        .mark_bar()
        .encode(
            y=group,
            color=group,
            x=alt.X("downloads:Q", scale=alt.Scale(type=f"{axis_scale}")),
            tooltip=["date", "downloads", alt.Tooltip("delta", format=".2%")],
        )
        .transform_filter(brush)
        .properties(width=550)
        .add_selection(click)
    )

    return lines & bars


def pandasamlit_downloads(source, x="date", y="downloads"):
    # Create a selection that chooses the nearest point & selects based on x-value
    hover = alt.selection_single(
        fields=[x],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(source)
        .mark_line(point="transparent")
        .encode(x=x, y=y)
        .transform_calculate(color='datum.delta < 0 ? "red" : "green"')
    )

    # Draw points on the line, highlight based on selection, color based on delta
    points = (
        lines.transform_filter(hover)
        .mark_circle(size=65)
        .encode(color=alt.Color("color:N", scale=None))
    )

    # Draw an invisible rule at the location of the selection
    tooltips = (
        alt.Chart(source)
        .mark_rule(opacity=0)
        .encode(
            x=x,
            y=y,
            tooltip=[x, y, alt.Tooltip("delta", format=".2%")],
        )
        .add_selection(hover)
    )

    return (lines + points + tooltips).interactive()


def main():

    # Note that page title/favicon are set in the __main__ clause below,
    # so they can also be set through the mega multipage app (see ../pandas_app.py).

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Select start date",
            date(2020, 1, 1),
            min_value=datetime.strptime("2020-01-01", "%Y-%m-%d"),
            max_value=datetime.now(),
        )

    with col2:
        time_frame = st.selectbox(
            "Select weekly or monthly downloads", ("weekly", "monthly")
        )

    # PREPARING DATA FOR WEEKLY AND MONTHLY

    df_monthly = monthly_downloads(start_date)
    df_weekly = weekly_downloads(start_date)

    pandas_data_monthly = df_monthly[df_monthly["project"] == "pandas"]
    pandas_data_weekly = df_weekly[df_weekly["project"] == "pandas"]

    package_names = df_monthly["project"].unique()

    if time_frame == "weekly":
        selected_data_streamlit = pandas_data_weekly
        selected_data_all = df_weekly
    else:
        selected_data_streamlit = pandas_data_monthly
        selected_data_all = df_monthly

    ## PANDAS DOWNLOADS

    st.header("Pandas downloads")

    st.altair_chart(
        pandasamlit_downloads(selected_data_streamlit), use_container_width=True
    )

    # OTHER DOWNLOADS

    st.header("Compare other package downloads")

    instructions = """
    Click and drag line chart to select and pan date interval\n
    Hover over bar chart to view downloads\n
    Click on a bar to highlight that package
    """
    select_packages = st.multiselect(
        "Select Python packages to compare",
        package_names,
        default=[
            "pandas",
            "keras",
        ],
        help=instructions,
    )

    select_packages_df = pd.DataFrame(select_packages).rename(columns={0: "project"})

    if not select_packages:
        st.stop()

    filtered_df = selected_data_all[
        selected_data_all["project"].isin(select_packages_df["project"])
    ]

    st.altair_chart(plot_all_downloads(filtered_df), use_container_width=True)


st.title("Downloads")
st.write(
    "Metrics on how often Pandas is being downloaded from PyPI (Python's main "
    "package repository, i.e. where `pip install pandas` downloads the package from)."
)
main()
